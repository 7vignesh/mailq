package smtp_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/quotedprintable"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/7vignesh/mailq/internal/smtp"
	"github.com/7vignesh/mailq/test/fakesmtp"
)

// fakeDNS maps domains to MX hosts, and hosts to the fake servers' addresses.
type fakeDNS struct {
	mx    map[string][]*net.MX
	hosts map[string]bool
	err   map[string]error
}

func (d fakeDNS) LookupMX(_ context.Context, name string) ([]*net.MX, error) {
	if err := d.err[name]; err != nil {
		return nil, err
	}
	if mx, ok := d.mx[name]; ok {
		return mx, nil
	}
	return nil, &net.DNSError{Err: "no such host", Name: name, IsNotFound: true}
}

func (d fakeDNS) LookupHost(_ context.Context, host string) ([]string, error) {
	if d.hosts[host] {
		return []string{"127.0.0.1"}, nil
	}
	return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
}

// routeDialer sends "host:25" to whatever address the test mapped host to.
type routeDialer map[string]string

func (r routeDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, _, _ := net.SplitHostPort(addr)
	target, ok := r[host]
	if !ok {
		return nil, errors.New("routeDialer: no route to " + host)
	}
	var d net.Dialer
	return d.DialContext(ctx, network, target)
}

func fastTimeouts() smtp.Timeouts {
	return smtp.Timeouts{Dial: time.Second, Greeting: time.Second, Command: 300 * time.Millisecond,
		DataWrite: time.Second, DataEnd: time.Second}
}

func startFake(t *testing.T, opts fakesmtp.Options) *fakesmtp.Server {
	t.Helper()
	s, err := fakesmtp.Start(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// clientFor returns a client whose only MX for example.test is the fake.
func clientFor(s *fakesmtp.Server) *smtp.Client {
	return &smtp.Client{
		HeloName: "mailq.test",
		Resolver: fakeDNS{mx: map[string][]*net.MX{"example.test": {{Host: "mx.example.test.", Pref: 10}}}},
		Dialer:   routeDialer{"mx.example.test": s.Addr()},
		Port:     "25",
		Timeouts: fastTimeouts(),
		StartTLS: true,
	}
}

func envelope(to, body string) smtp.Envelope {
	return smtp.Envelope{From: "sender@mailq.test", To: to,
		Data: smtp.BuildMessage("id-1", "sender@mailq.test", to, "hello", body, "mailq.test", time.Unix(0, 0))}
}

func TestSendAccepted(t *testing.T) {
	s := startFake(t, fakesmtp.Options{})
	r := clientFor(s).Send(context.Background(), envelope("accept@example.test", "hi there"))
	if r.Err != nil || r.Code != 250 || r.Enhanced != "2.0.0" || r.Stage != smtp.StageDataEnd {
		t.Fatalf("result %+v", r)
	}
	if r.MXHost != "mx.example.test" {
		t.Fatalf("mx host %q", r.MXHost)
	}
	if got := s.Received(); len(got) != 1 || got[0].To != "accept@example.test" {
		t.Fatalf("received %+v", got)
	}
}

func TestSendSurfacesReplies(t *testing.T) {
	s := startFake(t, fakesmtp.Options{})
	tests := []struct {
		rcpt     string
		code     int
		enhanced string
		stage    smtp.Stage
	}{
		{"throttle@example.test", 421, "4.7.0", smtp.StageRcpt},
		{"busy@example.test", 450, "4.2.1", smtp.StageRcpt},
		{"greylist@example.test", 451, "4.7.1", smtp.StageRcpt},
		{"full@example.test", 452, "4.2.2", smtp.StageRcpt},
		{"nouser@example.test", 550, "5.1.1", smtp.StageRcpt},
		{"policy@example.test", 554, "5.7.1", smtp.StageDataEnd},
	}
	c := clientFor(s)
	for _, tt := range tests {
		t.Run(tt.rcpt, func(t *testing.T) {
			r := c.Send(context.Background(), envelope(tt.rcpt, "x"))
			if r.Err != nil || r.Code != tt.code || r.Enhanced != tt.enhanced || r.Stage != tt.stage {
				t.Fatalf("got code=%d enhanced=%q stage=%s err=%v, want %d %s %s",
					r.Code, r.Enhanced, r.Stage, r.Err, tt.code, tt.enhanced, tt.stage)
			}
		})
	}
}

func TestSendTimeoutOnHang(t *testing.T) {
	s := startFake(t, fakesmtp.Options{})
	start := time.Now()
	r := clientFor(s).Send(context.Background(), envelope("hang@example.test", "x"))
	if r.ErrKind != smtp.ErrTimeout || r.Stage != smtp.StageRcpt {
		t.Fatalf("got kind=%q stage=%s err=%v", r.ErrKind, r.Stage, r.Err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("command timeout not enforced, took %v", d)
	}
}

func TestSendConnectionDrops(t *testing.T) {
	s := startFake(t, fakesmtp.Options{})
	c := clientFor(s)

	r := c.Send(context.Background(), envelope("drop@example.test", strings.Repeat("line\n", 2000)))
	if r.ErrKind != smtp.ErrConnection || (r.Stage != smtp.StageDataWrite && r.Stage != smtp.StageDataEnd) {
		t.Fatalf("mid-DATA drop: kind=%q stage=%s err=%v", r.ErrKind, r.Stage, r.Err)
	}

	// The dangerous case: the server has the whole message but we never
	// heard back. The client can only report "connection lost at data_end".
	r = c.Send(context.Background(), envelope("dropafter@example.test", "x"))
	if r.ErrKind != smtp.ErrConnection || r.Stage != smtp.StageDataEnd {
		t.Fatalf("drop after DATA: kind=%q stage=%s err=%v", r.ErrKind, r.Stage, r.Err)
	}
	if got := s.Received(); len(got) != 1 || got[0].Replied {
		t.Fatalf("server should hold one unacknowledged message, got %+v", got)
	}
}

func TestSendCancelInterruptsBlockedRead(t *testing.T) {
	s := startFake(t, fakesmtp.Options{})
	c := clientFor(s)
	c.Timeouts.Command = time.Minute // only cancellation can end this quickly
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	start := time.Now()
	r := c.Send(ctx, envelope("hang@example.test", "x"))
	if r.ErrKind != smtp.ErrCanceled {
		t.Fatalf("kind=%q err=%v", r.ErrKind, r.Err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("cancel did not interrupt the blocked read")
	}
}

func TestStartTLSUsedWhenOffered(t *testing.T) {
	cfg, err := fakesmtp.SelfSignedTLS()
	if err != nil {
		t.Fatal(err)
	}
	s := startFake(t, fakesmtp.Options{TLS: cfg})
	r := clientFor(s).Send(context.Background(), envelope("accept@example.test", "secret"))
	if r.Err != nil || r.Code != 250 || !r.TLS {
		t.Fatalf("result %+v", r)
	}
	if got := s.Received(); len(got) != 1 || !got[0].TLS {
		t.Fatal("message was not received over TLS")
	}
}

func TestMXFallbackOnHostFailure(t *testing.T) {
	s := startFake(t, fakesmtp.Options{})
	dead, _ := net.Listen("tcp", "127.0.0.1:0")
	deadAddr := dead.Addr().String()
	dead.Close() // nothing listens here: connection refused

	c := clientFor(s)
	c.Resolver = fakeDNS{mx: map[string][]*net.MX{"example.test": {
		{Host: "mx1.example.test.", Pref: 10}, {Host: "mx2.example.test.", Pref: 20}}}}
	c.Dialer = routeDialer{"mx1.example.test": deadAddr, "mx2.example.test": s.Addr()}

	r := c.Send(context.Background(), envelope("accept@example.test", "x"))
	if r.Code != 250 || r.MXHost != "mx2.example.test" || len(r.HostsTried) != 2 {
		t.Fatalf("result %+v", r)
	}
}

func TestNoFallbackAfterRecipientReply(t *testing.T) {
	a := startFake(t, fakesmtp.Options{})
	b := startFake(t, fakesmtp.Options{})
	c := clientFor(a)
	c.Resolver = fakeDNS{mx: map[string][]*net.MX{"example.test": {
		{Host: "mx1.example.test.", Pref: 10}, {Host: "mx2.example.test.", Pref: 20}}}}
	c.Dialer = routeDialer{"mx1.example.test": a.Addr(), "mx2.example.test": b.Addr()}

	r := c.Send(context.Background(), envelope("nouser@example.test", "x"))
	if r.Code != 550 || b.Connections() != 0 {
		t.Fatalf("a 550 at RCPT is about the recipient; must not try the next MX (code=%d, mx2 conns=%d)",
			r.Code, b.Connections())
	}
}

func TestDNSOutcomes(t *testing.T) {
	s := startFake(t, fakesmtp.Options{})
	c := clientFor(s)
	c.Resolver = fakeDNS{
		mx: map[string][]*net.MX{
			"nullmx.test": {{Host: ".", Pref: 0}},
		},
		hosts: map[string]bool{"implicit.test": true},
		err:   map[string]error{"servfail.test": &net.DNSError{Err: "server misbehaving", IsTemporary: true}},
	}
	c.Dialer = routeDialer{"implicit.test": s.Addr()}

	tests := []struct {
		to   string
		kind smtp.ErrKind
		code int
	}{
		{"a@nxdomain.test", smtp.ErrNoSuchDomain, 0},
		{"a@nullmx.test", smtp.ErrNullMX, 0},
		{"a@servfail.test", smtp.ErrDNSTemporary, 0},
		{"accept@implicit.test", smtp.ErrNone, 250}, // no MX: fall back to the A record
	}
	for _, tt := range tests {
		r := c.Send(context.Background(), envelope(tt.to, "x"))
		if r.ErrKind != tt.kind || r.Code != tt.code {
			t.Errorf("%s: kind=%q code=%d err=%v, want %q %d", tt.to, r.ErrKind, r.Code, r.Err, tt.kind, tt.code)
		}
	}
}

func TestSplitRepliesAreReassembled(t *testing.T) {
	s := startFake(t, fakesmtp.Options{SplitReplies: true})
	c := clientFor(s)
	c.Timeouts.Command = 5 * time.Second
	if r := c.Send(context.Background(), envelope("accept@example.test", "x")); r.Code != 250 {
		t.Fatalf("accept over split replies: %+v", r)
	}
	r := c.Send(context.Background(), envelope("nouser@example.test", "x"))
	if r.Code != 550 || r.Enhanced != "5.1.1" || strings.Count(r.Text, "\n") != 1 {
		t.Fatalf("multiline 550 over split replies: code=%d enhanced=%q text=%q", r.Code, r.Enhanced, r.Text)
	}
}

// decodeBody returns the QP-decoded body of a received message.
func decodeBody(t *testing.T, data []byte) string {
	t.Helper()
	_, body, ok := bytes.Cut(data, []byte("\r\n\r\n"))
	if !ok {
		t.Fatalf("no header/body separator in %q", data)
	}
	out, err := io.ReadAll(quotedprintable.NewReader(bytes.NewReader(body)))
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestDotStuffing(t *testing.T) {
	s := startFake(t, fakesmtp.Options{})
	// A line that is just "." would end DATA early if not stuffed, silently
	// truncating the message. The text after it proves it was not.
	body := "first\n.\nafter a lone dot\n..two dots\n.hidden\nlast"
	r := clientFor(s).Send(context.Background(), envelope("accept@example.test", body))
	if r.Code != 250 {
		t.Fatalf("result %+v", r)
	}
	got := s.Received()[0]
	for _, want := range []string{"\r\n..\r\n", "\r\n...two dots\r\n", "\r\n..hidden\r\n"} {
		if !bytes.Contains(got.Raw, []byte(want)) {
			t.Errorf("wire data missing stuffed line %q:\n%s", want, got.Raw)
		}
	}
	// BuildMessage always ends the body with CRLF, so ignore that one.
	if dec := strings.TrimSuffix(decodeBody(t, got.Data), "\r\n"); strings.ReplaceAll(dec, "\r\n", "\n") != body {
		t.Fatalf("body round trip:\n got %q\nwant %q", dec, body)
	}
	if v := s.Violations(); len(v) != 0 {
		t.Fatalf("protocol violations: %v", v)
	}
}

func TestLineEndingsAreAlwaysCRLF(t *testing.T) {
	s := startFake(t, fakesmtp.Options{})
	bodies := []string{
		"unix\nline endings\n",
		"windows\r\nline endings\r\n",
		"mixed\nand\r\nmatched\n",
		"bare\rcarriage return",
		"no trailing newline",
		strings.Repeat("x", 5000), // one very long line
		"ünïcödé ✓\n",
	}
	c := clientFor(s)
	for _, body := range bodies {
		if r := c.Send(context.Background(), envelope("accept@example.test", body)); r.Code != 250 {
			t.Fatalf("body %q: %+v", body, r)
		}
	}
	if v := s.Violations(); len(v) != 0 {
		t.Fatalf("protocol violations: %v", v)
	}
	for i, got := range s.Received() {
		for _, line := range bytes.SplitAfter(got.Raw, []byte("\r\n")) {
			if len(line) > 1000 {
				t.Errorf("body %d: line of %d bytes exceeds SMTP limit", i, len(line))
			}
		}
	}
}

func TestHeaderInjectionIsStripped(t *testing.T) {
	msg := smtp.BuildMessage("id", "a@b.test", "c@d.test", "hi\r\nBcc: victim@evil.test", "x", "b.test", time.Unix(0, 0))
	if bytes.Contains(msg, []byte("\r\nBcc:")) {
		t.Fatalf("subject injected a header:\n%s", msg)
	}
}

func TestParseEnhanced(t *testing.T) {
	tests := map[string]string{
		"5.1.1 No such user":          "5.1.1",
		"4.7.0 Try later\nmore":       "4.7.0",
		"2.0.0":                       "2.0.0",
		"5.7.26 Unauthenticated mail": "5.7.26",
		"4.2.123 three digit detail":  "4.2.123",
		"No such user":                "",
		"3.1.1 bad class":             "",
		"5.1 too short":               "",
		"5.1.1234 too long":           "",
		"5.a.1 not numeric":           "",
		"":                            "",
	}
	for in, want := range tests {
		if got := smtp.ParseEnhanced(in); got != want {
			t.Errorf("ParseEnhanced(%q) = %q, want %q", in, got, want)
		}
	}
}
