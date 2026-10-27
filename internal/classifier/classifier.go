// Package classifier turns the result of one SMTP attempt into a decision:
// delivered, transient (retry later) or permanent (bounce). It is pure: no
// I/O, no clock, no dependencies on the rest of mailq.
//
// Rules, applied in order (the first match wins):
//
//  1. No SMTP reply (transport error): timeouts and connection failures are
//     transient; NXDOMAIN and null MX are permanent bad_domain.
//  2. 2xx counts as delivered only as the reply to the end of DATA. A 2xx
//     anywhere else means the session ended early in a way we do not
//     understand, so it is ambiguous.
//  3. A reply code outside 2xx/4xx/5xx is ambiguous.
//  4. If the enhanced code's class (4 or 5) disagrees with the basic code's
//     class, the response is ambiguous.
//  5. X.2.2 (mailbox full) is transient even with a 5xx code. This is a
//     judgment call, not an RFC rule: full mailboxes usually empty again.
//  6. 5xx whose text says "try again later", "temporarily", "rate limit" or
//     "too many" is ambiguous (the code says permanent, the words say
//     temporary). Unverified: this phrase list is a guess at common wording,
//     not taken from any provider's documentation.
//  7. 4xx: pick a sub-reason from the text, then the enhanced code, then the
//     basic code.
//  8. 5xx: same, for permanent sub-reasons. A clean 5xx with no recognized
//     detail is still permanent: the server was unambiguous about the class.
//
// Ambiguous always means transient. A wrong retry costs a delay and a few
// extra attempts, bounded by the max message age; a wrong bounce silently
// loses mail the sender cannot recover. The cost of this choice is that
// some real hard bounces are retried for up to the max age, which wastes
// attempts and, sent to a dead address repeatedly, can hurt sender
// reputation.
package classifier

import "strings"

type Outcome string

const (
	Delivered Outcome = "delivered"
	Transient Outcome = "transient"
	Permanent Outcome = "permanent"
)

type Reason string

const (
	ReasonNone Reason = ""

	// Transient.
	MailboxFull       Reason = "mailbox_full"
	MailboxBusy       Reason = "mailbox_busy"
	Throttled         Reason = "throttled"
	Greylisted        Reason = "greylisted"
	ConnectionFailure Reason = "connection_failure"
	Timeout           Reason = "timeout"
	Interrupted       Reason = "interrupted" // we canceled the attempt (shutdown)
	TemporaryOther    Reason = "temporary_other"
	Ambiguous         Reason = "ambiguous"

	// Permanent.
	NoSuchUser      Reason = "no_such_user"
	PolicyBlocked   Reason = "policy_blocked"
	BadDomain       Reason = "bad_domain"
	ContentRejected Reason = "content_rejected"
	PermanentOther  Reason = "permanent_other"
)

// Transport errors, matching the smtp package's ErrKind values. Declared
// here as plain strings so the classifier does not import the client.
const (
	TransportTimeout      = "timeout"
	TransportConnection   = "connection"
	TransportCanceled     = "canceled"
	TransportNoSuchDomain = "no_such_domain"
	TransportNullMX       = "null_mx"
	TransportDNSTemporary = "dns_temporary"
)

// Input is everything known about how one attempt ended.
type Input struct {
	Code      int    // basic SMTP reply code, 0 if no reply was received
	Enhanced  string // RFC 3463 code, e.g. "5.1.1", or ""
	Text      string // reply text
	Stage     string // SMTP stage the attempt ended in; "data_end" is the final reply
	Transport string // non-empty when the attempt ended without a reply
}

type Result struct {
	Outcome Outcome
	Reason  Reason
	Rule    string // which rule decided, for logs
}

const stageDataEnd = "data_end"

func Classify(in Input) Result {
	if in.Code == 0 {
		return transport(in.Transport)
	}
	class := in.Code / 100
	text := strings.ToLower(in.Text)
	enh := parseEnhanced(in.Enhanced)

	switch {
	case class == 2 && in.Stage == stageDataEnd:
		return Result{Delivered, ReasonNone, "2xx after DATA"}
	case class == 2:
		return Result{Transient, Ambiguous, "2xx before end of DATA"}
	case class != 4 && class != 5:
		return Result{Transient, Ambiguous, "unexpected reply class"}
	case enh.ok && enh.class != class:
		return Result{Transient, Ambiguous, "basic and enhanced class disagree"}
	case enh.ok && enh.subject == 2 && enh.detail == 2:
		return Result{Transient, MailboxFull, "X.2.2 mailbox full"}
	case class == 5 && containsAny(text, temporaryWords):
		return Result{Transient, Ambiguous, "5xx with temporary wording"}
	case class == 4:
		return Result{Transient, transientReason(in.Code, enh, text), "4xx"}
	default:
		return Result{Permanent, permanentReason(enh, text), "5xx"}
	}
}

func transport(kind string) Result {
	switch kind {
	case TransportTimeout:
		return Result{Transient, Timeout, "transport timeout"}
	case TransportCanceled:
		return Result{Transient, Interrupted, "attempt canceled"}
	case TransportNoSuchDomain, TransportNullMX:
		// Unverified: we trust a single NXDOMAIN answer. A DNS outage that
		// returns NXDOMAIN by mistake would bounce mail that a retry would
		// have delivered.
		return Result{Permanent, BadDomain, "domain does not accept mail"}
	case TransportConnection, TransportDNSTemporary:
		return Result{Transient, ConnectionFailure, "transport connection failure"}
	default:
		return Result{Transient, Ambiguous, "no reply and unknown transport error"}
	}
}

var (
	temporaryWords = []string{"try again later", "temporarily", "rate limit", "rate-limit", "too many"}
	greylistWords  = []string{"greylist", "graylist", "grey-list", "gray-list"}
	throttleWords  = []string{"rate limit", "rate-limit", "too many", "throttl", "slow down"}
	noUserWords    = []string{"no such user", "user unknown", "unknown user", "does not exist", "mailbox not found", "recipient not found", "invalid recipient"}
	contentWords   = []string{"spam", "virus", "malware", "content", "message too large", "message size"}
)

func transientReason(code int, e enhanced, text string) Reason {
	switch {
	case containsAny(text, greylistWords):
		return Greylisted
	case code == 421, containsAny(text, throttleWords):
		return Throttled
	case e.ok && e.subject == 7:
		// Unverified: 4.7.x is a security/policy deferral. Providers use it
		// for rate limits and reputation deferrals, but also for greylisting
		// without saying so. We call it throttled, which slows the domain
		// down; the cost of being wrong is sending slower than needed.
		return Throttled
	case e.ok && e.subject == 2:
		return MailboxBusy // X.2.x mailbox status (X.2.2 is caught earlier)
	case code == 452:
		return MailboxFull // "insufficient system storage" without detail
	case code == 450:
		return MailboxBusy
	default:
		return TemporaryOther
	}
}

func permanentReason(e enhanced, text string) Reason {
	switch {
	case e.ok && e.subject == 1 && (e.detail == 2 || e.detail == 10):
		return BadDomain // 5.1.2 bad destination system, 5.1.10 null MX
	case e.ok && e.subject == 4 && e.detail == 4:
		return BadDomain // 5.4.4 unable to route
	case e.ok && e.subject == 1, e.ok && e.subject == 2 && e.detail == 1, containsAny(text, noUserWords):
		return NoSuchUser // 5.1.x bad mailbox, 5.2.1 mailbox disabled
	case e.ok && (e.subject == 6 || (e.subject == 3 && e.detail == 4)), containsAny(text, contentWords):
		return ContentRejected // 5.6.x media, 5.3.4 too big, or content wording
	case e.ok && e.subject == 7, strings.Contains(text, "policy"), strings.Contains(text, "blocked"):
		return PolicyBlocked
	default:
		return PermanentOther
	}
}

type enhanced struct {
	ok                     bool
	class, subject, detail int
}

func parseEnhanced(s string) enhanced {
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return enhanced{}
	}
	var n [3]int
	for i, p := range parts {
		if p == "" || len(p) > 3 {
			return enhanced{}
		}
		for _, c := range p {
			if c < '0' || c > '9' {
				return enhanced{}
			}
			n[i] = n[i]*10 + int(c-'0')
		}
	}
	if n[0] != 2 && n[0] != 4 && n[0] != 5 {
		return enhanced{}
	}
	return enhanced{true, n[0], n[1], n[2]}
}

func containsAny(s string, words []string) bool {
	for _, w := range words {
		if strings.Contains(s, w) {
			return true
		}
	}
	return false
}
