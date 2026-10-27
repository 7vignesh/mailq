package classifier

import "testing"

func TestClassify(t *testing.T) {
	const end, rcpt, mail, greet = "data_end", "rcpt", "mail", "greeting"
	tests := []struct {
		name    string
		in      Input
		outcome Outcome
		reason  Reason
	}{
		// Delivered.
		{"250 after data", Input{250, "2.0.0", "2.0.0 OK queued", end, ""}, Delivered, ReasonNone},
		{"250 no enhanced", Input{250, "", "Ok: queued as ABC", end, ""}, Delivered, ReasonNone},
		{"251 forwarded", Input{251, "2.1.5", "user not local, will forward", end, ""}, Delivered, ReasonNone},
		{"2xx with conflicting enhanced still delivered", Input{250, "5.0.0", "5.0.0 weird", end, ""}, Delivered, ReasonNone},

		// The fake server's scripted replies.
		{"421 throttle", Input{421, "4.7.0", "4.7.0 Too many messages from your IP, rate limited", rcpt, ""}, Transient, Throttled},
		{"450 busy", Input{450, "4.2.1", "4.2.1 Mailbox busy, try again later", rcpt, ""}, Transient, MailboxBusy},
		{"451 greylist", Input{451, "4.7.1", "4.7.1 Greylisted, please try again later", rcpt, ""}, Transient, Greylisted},
		{"452 storage", Input{452, "4.2.2", "4.2.2 Insufficient storage: mailbox full", rcpt, ""}, Transient, MailboxFull},
		{"550 no such user", Input{550, "5.1.1", "5.1.1 The email account that you tried to reach does not exist.\n5.1.1 Please check the recipient address and try again.", rcpt, ""}, Permanent, NoSuchUser},
		{"554 policy", Input{554, "5.7.1", "5.7.1 Message rejected due to local policy", end, ""}, Permanent, PolicyBlocked},

		// Transient sub-reasons.
		{"421 without enhanced", Input{421, "", "Service not available, closing channel", greet, ""}, Transient, Throttled},
		{"421 greylist wording wins", Input{421, "4.7.0", "4.7.0 greylisted, come back later", rcpt, ""}, Transient, Greylisted},
		{"450 graylist spelling", Input{450, "4.2.0", "4.2.0 Recipient address rejected: Graylisted", rcpt, ""}, Transient, Greylisted},
		{"4.7.x is a throttle signal", Input{451, "4.7.650", "4.7.650 The mail server has been temporarily rate limited", rcpt, ""}, Transient, Throttled},
		{"4.7.0 bare", Input{450, "4.7.0", "4.7.0 deferred", rcpt, ""}, Transient, Throttled},
		{"throttle words without codes", Input{451, "", "Throttling failure: slow down", rcpt, ""}, Transient, Throttled},
		{"452 without enhanced", Input{452, "", "Insufficient system storage", rcpt, ""}, Transient, MailboxFull},
		{"450 without enhanced", Input{450, "", "Requested mail action not taken", rcpt, ""}, Transient, MailboxBusy},
		{"451 local error", Input{451, "4.3.0", "4.3.0 Local error in processing", end, ""}, Transient, TemporaryOther},
		{"4xx at MAIL", Input{451, "4.4.1", "4.4.1 no answer from host", mail, ""}, Transient, TemporaryOther},

		// Permanent sub-reasons.
		{"550 user unknown no enhanced", Input{550, "", "User unknown in virtual mailbox table", rcpt, ""}, Permanent, NoSuchUser},
		{"550 5.1.10 null MX", Input{550, "5.1.10", "5.1.10 Recipient domain has null MX", rcpt, ""}, Permanent, BadDomain},
		{"553 5.1.2 bad system", Input{553, "5.1.2", "5.1.2 bad destination system", rcpt, ""}, Permanent, BadDomain},
		{"554 5.4.4 unroutable", Input{554, "5.4.4", "5.4.4 Unable to route", rcpt, ""}, Permanent, BadDomain},
		{"5.2.1 disabled", Input{550, "5.2.1", "5.2.1 mailbox disabled", rcpt, ""}, Permanent, NoSuchUser},
		{"5.7.26 dmarc", Input{550, "5.7.26", "5.7.26 Unauthenticated email is not accepted", end, ""}, Permanent, PolicyBlocked},
		{"554 blocked at greeting", Input{554, "", "Your IP is blocked by RBL", greet, ""}, Permanent, PolicyBlocked},
		{"552 5.3.4 too big", Input{552, "5.3.4", "5.3.4 Message size exceeds fixed limit", end, ""}, Permanent, ContentRejected},
		{"554 5.6.0 media", Input{554, "5.6.0", "5.6.0 Message content rejected", end, ""}, Permanent, ContentRejected},
		{"spam wording beats 5.7.1", Input{550, "5.7.1", "5.7.1 Message rejected as spam", end, ""}, Permanent, ContentRejected},
		{"clean 5xx unknown detail", Input{550, "5.0.0", "5.0.0 Rejected", rcpt, ""}, Permanent, PermanentOther},
		{"clean 5xx no enhanced", Input{500, "", "Syntax error", mail, ""}, Permanent, PermanentOther},

		// Ambiguous: always transient.
		{"5.2.2 mailbox full under 552", Input{552, "5.2.2", "5.2.2 Mailbox full", rcpt, ""}, Transient, MailboxFull},
		{"basic 5 enhanced 4", Input{550, "4.2.2", "4.2.2 over quota", rcpt, ""}, Transient, Ambiguous},
		{"basic 4 enhanced 5", Input{450, "5.1.1", "5.1.1 no such user", rcpt, ""}, Transient, Ambiguous},
		{"5xx try again later", Input{550, "5.7.1", "5.7.1 Service unavailable, try again later", end, ""}, Transient, Ambiguous},
		{"5xx temporarily", Input{554, "", "Temporarily rejected", rcpt, ""}, Transient, Ambiguous},
		{"5xx rate limit", Input{550, "5.7.1", "5.7.1 rate limit exceeded", rcpt, ""}, Transient, Ambiguous},
		{"2xx before data end", Input{221, "2.0.0", "2.0.0 Bye", rcpt, ""}, Transient, Ambiguous},
		{"3xx final", Input{354, "", "go ahead", end, ""}, Transient, Ambiguous},
		{"garbage code", Input{999, "", "???", rcpt, ""}, Transient, Ambiguous},
		{"malformed enhanced ignored", Input{550, "5.1", "5.1 no", rcpt, ""}, Permanent, PermanentOther},

		// Transport errors (no reply).
		{"timeout", Input{Stage: rcpt, Transport: TransportTimeout}, Transient, Timeout},
		{"connection refused", Input{Stage: "dial", Transport: TransportConnection}, Transient, ConnectionFailure},
		{"drop mid data", Input{Stage: "data_write", Transport: TransportConnection}, Transient, ConnectionFailure},
		{"drop after data", Input{Stage: end, Transport: TransportConnection}, Transient, ConnectionFailure},
		{"dns servfail", Input{Stage: "dns", Transport: TransportDNSTemporary}, Transient, ConnectionFailure},
		{"canceled", Input{Stage: rcpt, Transport: TransportCanceled}, Transient, Interrupted},
		{"nxdomain", Input{Stage: "dns", Transport: TransportNoSuchDomain}, Permanent, BadDomain},
		{"null mx", Input{Stage: "dns", Transport: TransportNullMX}, Permanent, BadDomain},
		{"no reply no transport", Input{}, Transient, Ambiguous},
		{"unknown transport", Input{Transport: "cosmic_ray"}, Transient, Ambiguous},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.in)
			if got.Outcome != tt.outcome || got.Reason != tt.reason {
				t.Fatalf("Classify(%+v) = %s/%s (rule %q), want %s/%s",
					tt.in, got.Outcome, got.Reason, got.Rule, tt.outcome, tt.reason)
			}
			if got.Rule == "" {
				t.Fatal("every result must name the rule that decided it")
			}
		})
	}
}

// Every 4xx must be transient and every clean 5xx permanent or transient,
// never delivered, whatever the text says.
func TestClassifyNeverDeliversNon2xx(t *testing.T) {
	texts := []string{"", "OK", "2.0.0 OK", "250 OK", "delivered"}
	for code := 100; code < 700; code++ {
		if code/100 == 2 {
			continue
		}
		for _, text := range texts {
			r := Classify(Input{Code: code, Text: text, Stage: "data_end"})
			if r.Outcome == Delivered {
				t.Fatalf("code %d text %q classified as delivered", code, text)
			}
			if code/100 == 4 && r.Outcome != Transient {
				t.Fatalf("4xx code %d classified as %s", code, r.Outcome)
			}
		}
	}
}

func TestParseEnhanced(t *testing.T) {
	tests := []struct {
		in   string
		want enhanced
	}{
		{"5.1.1", enhanced{true, 5, 1, 1}},
		{"4.7.650", enhanced{true, 4, 7, 650}},
		{"2.0.0", enhanced{true, 2, 0, 0}},
		{"3.0.0", enhanced{}},
		{"5.1", enhanced{}},
		{"5.1.1.1", enhanced{}},
		{"5..1", enhanced{}},
		{"5.x.1", enhanced{}},
		{"5.1.1234", enhanced{}},
		{"", enhanced{}},
	}
	for _, tt := range tests {
		if got := parseEnhanced(tt.in); got != tt.want {
			t.Errorf("parseEnhanced(%q) = %+v, want %+v", tt.in, got, tt.want)
		}
	}
}
