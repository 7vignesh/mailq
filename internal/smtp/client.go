// Package smtp is a minimal outbound SMTP client: MX resolution, one
// transaction per connection, opportunistic STARTTLS, and a deadline on
// every stage. It reports what happened; deciding what that means (deliver,
// retry, bounce) is the classifier's job.
package smtp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/textproto"
	"strings"
	"time"
)

// Stage is the step of the SMTP session where an attempt ended.
type Stage string

const (
	StageDNS       Stage = "dns"
	StageDial      Stage = "dial"
	StageGreeting  Stage = "greeting"
	StageEHLO      Stage = "ehlo"
	StageStartTLS  Stage = "starttls"
	StageMail      Stage = "mail"
	StageRcpt      Stage = "rcpt"
	StageData      Stage = "data"       // the DATA command itself (expects 354)
	StageDataWrite Stage = "data_write" // sending the message body
	StageDataEnd   Stage = "data_end"   // waiting for the reply to the final "."
)

// ErrKind says why an attempt ended without an SMTP reply.
type ErrKind string

const (
	ErrNone         ErrKind = ""
	ErrTimeout      ErrKind = "timeout"
	ErrConnection   ErrKind = "connection"     // refused, reset, closed, TLS failure
	ErrCanceled     ErrKind = "canceled"       // our own context was canceled (shutdown)
	ErrNoSuchDomain ErrKind = "no_such_domain" // NXDOMAIN, no MX and no address records
	ErrNullMX       ErrKind = "null_mx"        // RFC 7505: domain says it accepts no mail
	ErrDNSTemporary ErrKind = "dns_temporary"
)

// Timeouts bound every stage. RFC 5321 section 4.5.3.2 suggests 5 minutes
// for the greeting and commands, 3 minutes per DATA block and 10 minutes
// for the final reply. We shorten the command timeouts, but keep DataEnd
// long: giving up while the server is still processing the message is the
// most likely way to cause a duplicate, since the server may then accept it.
type Timeouts struct {
	Dial      time.Duration
	Greeting  time.Duration
	Command   time.Duration // EHLO, STARTTLS handshake, MAIL, RCPT, DATA
	DataWrite time.Duration // writing the whole body
	DataEnd   time.Duration // reply to the final "."
}

func DefaultTimeouts() Timeouts {
	return Timeouts{Dial: 30 * time.Second, Greeting: 2 * time.Minute, Command: 2 * time.Minute,
		DataWrite: 3 * time.Minute, DataEnd: 10 * time.Minute}
}

type Resolver interface {
	LookupMX(ctx context.Context, name string) ([]*net.MX, error)
	LookupHost(ctx context.Context, host string) ([]string, error)
}

type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type Client struct {
	HeloName   string // our hostname for EHLO; should match our PTR record in production
	Resolver   Resolver
	Dialer     Dialer
	Port       string // "25"
	Timeouts   Timeouts
	StartTLS   bool // opportunistic STARTTLS when the server offers it
	MaxMXHosts int  // hosts tried per attempt, in preference order
}

// Envelope is one message to one recipient. Data is the complete RFC 5322
// message with CRLF line endings, before dot-stuffing.
type Envelope struct {
	From string
	To   string
	Data []byte
}

// Result describes how an attempt ended. If Err is nil, Code/Enhanced/Text
// hold the SMTP reply that decided the outcome.
type Result struct {
	MXHost     string
	HostsTried []string
	Stage      Stage
	Code       int
	Enhanced   string // RFC 3463 code from the reply text, e.g. "5.1.1"
	Text       string // full reply text; multiline replies are joined with \n
	TLS        bool
	Err        error
	ErrKind    ErrKind
}

// Send resolves the recipient domain and tries its MX hosts in preference
// order. It moves to the next host only when a host fails before the mail
// transaction starts (dial, greeting, EHLO, STARTTLS). Once a server answers
// MAIL/RCPT/DATA, that answer is about the message, not the host, and it is
// returned as is.
func (c *Client) Send(ctx context.Context, env Envelope) Result {
	_, domain, ok := strings.Cut(env.To, "@")
	if !ok || domain == "" {
		return Result{Stage: StageDNS, ErrKind: ErrNoSuchDomain, Err: fmt.Errorf("smtp: bad recipient %q", env.To)}
	}
	hosts, res := c.resolve(ctx, strings.ToLower(domain))
	if res != nil {
		return *res
	}
	var tried []string
	var r Result
	for _, h := range hosts {
		tried = append(tried, h)
		r = c.sendHost(ctx, h, env)
		r.HostsTried = tried
		if ctx.Err() != nil || !hostLevelFailure(r) {
			return r
		}
	}
	return r
}

func hostLevelFailure(r Result) bool {
	switch r.Stage {
	case StageDial, StageGreeting, StageEHLO, StageStartTLS:
		return true
	}
	return false
}

func (c *Client) resolve(ctx context.Context, domain string) ([]string, *Result) {
	fail := func(kind ErrKind, err error) ([]string, *Result) {
		return nil, &Result{Stage: StageDNS, ErrKind: kind, Err: err}
	}
	mxs, err := c.Resolver.LookupMX(ctx, domain)
	if err != nil && !isNotFound(err) {
		return fail(ErrDNSTemporary, err)
	}
	if len(mxs) == 0 {
		// RFC 5321 section 5.1: with no MX records, the domain's own address
		// records act as an implicit MX with preference 0.
		if _, herr := c.Resolver.LookupHost(ctx, domain); herr != nil {
			if isNotFound(herr) {
				return fail(ErrNoSuchDomain, herr)
			}
			return fail(ErrDNSTemporary, herr)
		}
		return []string{domain}, nil
	}
	if len(mxs) == 1 && (mxs[0].Host == "." || mxs[0].Host == "") {
		return fail(ErrNullMX, fmt.Errorf("smtp: %s publishes a null MX", domain))
	}
	// net.LookupMX returns records sorted by preference (randomized within
	// equal preference), so order here is the delivery order.
	var hosts []string
	for _, mx := range mxs {
		hosts = append(hosts, strings.TrimSuffix(mx.Host, "."))
	}
	if c.MaxMXHosts > 0 && len(hosts) > c.MaxMXHosts {
		hosts = hosts[:c.MaxMXHosts]
	}
	return hosts, nil
}

func isNotFound(err error) bool {
	var de *net.DNSError
	return errors.As(err, &de) && de.IsNotFound
}

func (c *Client) sendHost(ctx context.Context, host string, env Envelope) (r Result) {
	r.MXHost = host
	t := c.Timeouts

	dctx, cancel := context.WithTimeout(ctx, t.Dial)
	raw, err := c.Dialer.DialContext(dctx, "tcp", net.JoinHostPort(host, c.Port))
	cancel()
	if err != nil {
		return r.fail(ctx, StageDial, err)
	}
	// Deadlines bound each stage; this makes ctx cancellation (shutdown, or
	// the per-attempt deadline) interrupt a blocked read or write too.
	stop := context.AfterFunc(ctx, func() { raw.Close() })
	defer stop()
	defer raw.Close()

	s := &session{conn: raw, text: textproto.NewConn(raw)}

	code, msg, err := s.cmd(t.Greeting, "")
	if err != nil {
		return r.fail(ctx, StageGreeting, err)
	}
	if code != 220 {
		return r.reply(StageGreeting, code, msg)
	}

	ext, code, msg, err := s.hello(t.Command, c.HeloName)
	if err != nil {
		return r.fail(ctx, StageEHLO, err)
	}
	if code != 250 {
		return r.reply(StageEHLO, code, msg)
	}

	if c.StartTLS && ext["STARTTLS"] {
		code, msg, err = s.cmd(t.Command, "STARTTLS")
		if err != nil {
			return r.fail(ctx, StageStartTLS, err)
		}
		// A refusal leaves the session usable, so continue in plaintext.
		if code == 220 {
			if err := s.startTLS(ctx, t.Command, host); err != nil {
				// After a failed handshake the connection is unusable. We fail
				// this host and let Send try the next MX. Unverified: Postfix
				// instead retries the same host in plaintext.
				return r.fail(ctx, StageStartTLS, err)
			}
			r.TLS = true
			if _, code, msg, err = s.hello(t.Command, c.HeloName); err != nil {
				return r.fail(ctx, StageEHLO, err)
			}
			if code != 250 {
				return r.reply(StageEHLO, code, msg)
			}
		}
	}

	if code, msg, err = s.cmd(t.Command, "MAIL FROM:<%s>", env.From); err != nil {
		return r.fail(ctx, StageMail, err)
	}
	if code != 250 {
		return r.reply(StageMail, code, msg)
	}
	if code, msg, err = s.cmd(t.Command, "RCPT TO:<%s>", env.To); err != nil {
		return r.fail(ctx, StageRcpt, err)
	}
	if code != 250 && code != 251 {
		return r.reply(StageRcpt, code, msg)
	}
	if code, msg, err = s.cmd(t.Command, "DATA"); err != nil {
		return r.fail(ctx, StageData, err)
	}
	if code != 354 {
		return r.reply(StageData, code, msg)
	}

	if err := s.writeBody(t.DataWrite, env.Data); err != nil {
		return r.fail(ctx, StageDataWrite, err)
	}
	// From here until we read the reply, the server may have accepted the
	// message without us knowing. Any failure in this window can cause a
	// duplicate on retry.
	code, msg, err = s.cmd(t.DataEnd, "")
	if err != nil {
		return r.fail(ctx, StageDataEnd, err)
	}
	r = r.reply(StageDataEnd, code, msg)

	s.cmd(5*time.Second, "QUIT") // best effort; the outcome is already decided
	return r
}

func (r Result) fail(ctx context.Context, stage Stage, err error) Result {
	r.Stage, r.Err = stage, err
	var ne net.Error
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		r.ErrKind = ErrCanceled
	case ctx.Err() != nil, errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		r.ErrKind = ErrTimeout
	default:
		r.ErrKind = ErrConnection
	}
	return r
}

func (r Result) reply(stage Stage, code int, msg string) Result {
	r.Stage, r.Code, r.Text, r.Enhanced = stage, code, msg, ParseEnhanced(msg)
	return r
}

type session struct {
	conn net.Conn
	text *textproto.Conn
}

// cmd sends one command (or none, to just read) and reads one reply, all
// within timeout. textproto handles multiline replies ("250-...") and
// replies that arrive split across several TCP reads.
func (s *session) cmd(timeout time.Duration, format string, args ...any) (int, string, error) {
	s.conn.SetDeadline(time.Now().Add(timeout))
	if format != "" {
		if err := s.text.PrintfLine(format, args...); err != nil {
			return 0, "", err
		}
	}
	return s.text.ReadResponse(0)
}

// hello sends EHLO, falling back to HELO for servers that reject it, and
// returns the advertised extensions (upper-cased keywords).
func (s *session) hello(timeout time.Duration, name string) (map[string]bool, int, string, error) {
	code, msg, err := s.cmd(timeout, "EHLO %s", name)
	if err != nil {
		return nil, 0, "", err
	}
	if code == 500 || code == 502 {
		code, msg, err = s.cmd(timeout, "HELO %s", name)
		return map[string]bool{}, code, msg, err
	}
	ext := map[string]bool{}
	if code == 250 {
		lines := strings.Split(msg, "\n")
		for _, l := range lines[1:] { // first line is the server's greeting text
			kw, _, _ := strings.Cut(l, " ")
			ext[strings.ToUpper(kw)] = true
		}
	}
	return ext, code, msg, nil
}

// startTLS upgrades the connection. Opportunistic TLS (RFC 7435) encrypts
// without authenticating: certificate checks are skipped because most MX
// certificates do not match the MX hostname, and refusing to deliver over an
// unverified channel would just mean delivering in plaintext instead.
// Enforcing verification is what MTA-STS and DANE are for (out of scope).
func (s *session) startTLS(ctx context.Context, timeout time.Duration, host string) error {
	s.conn.SetDeadline(time.Now().Add(timeout))
	tc := tls.Client(s.conn, &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: true, //nolint:gosec // opportunistic TLS, see above
		MinVersion:         tls.VersionTLS12,
	})
	if err := tc.HandshakeContext(ctx); err != nil {
		return err
	}
	s.conn, s.text = tc, textproto.NewConn(tc)
	return nil
}

// writeBody sends the message with dot-stuffing: DotWriter doubles a
// leading "." on every line, turns bare "\n" into "\r\n", and terminates
// with "\r\n.\r\n" even if the data lacks a final newline.
func (s *session) writeBody(timeout time.Duration, data []byte) error {
	s.conn.SetDeadline(time.Now().Add(timeout))
	w := s.text.DotWriter()
	if _, err := w.Write(data); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// ParseEnhanced extracts an RFC 3463 status code ("class.subject.detail")
// from the start of a reply's text, or returns "" if there is none.
func ParseEnhanced(text string) string {
	first, _, _ := strings.Cut(text, "\n")
	tok, _, _ := strings.Cut(first, " ")
	parts := strings.Split(tok, ".")
	if len(parts) != 3 || len(parts[0]) != 1 || !strings.ContainsAny(parts[0], "245") {
		return ""
	}
	for _, p := range parts[1:] {
		if len(p) == 0 || len(p) > 3 || strings.Trim(p, "0123456789") != "" {
			return ""
		}
	}
	return tok
}
