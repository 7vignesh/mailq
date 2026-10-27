package fakesmtp

import (
	"crypto/tls"
	"errors"
	"net"
	"net/textproto"
	"strings"
	"testing"
	"time"
)

func start(t *testing.T, opts Options) *Server {
	t.Helper()
	s, err := Start(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// rawSession speaks just enough SMTP by hand to drive the fake.
type rawSession struct {
	t    *testing.T
	conn net.Conn
	tp   *textproto.Conn
}

func dial(t *testing.T, s *Server) *rawSession {
	t.Helper()
	c, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(5 * time.Second))
	r := &rawSession{t: t, conn: c, tp: textproto.NewConn(c)}
	r.expect(220)
	return r
}

func (r *rawSession) cmd(line string) (int, string, error) {
	r.t.Helper()
	if err := r.tp.PrintfLine("%s", line); err != nil {
		return 0, "", err
	}
	return r.tp.ReadResponse(0)
}

func (r *rawSession) expect(code int) string {
	r.t.Helper()
	got, msg, err := r.tp.ReadResponse(0)
	if err != nil || got != code {
		r.t.Fatalf("got %d %q (err %v), want %d", got, msg, err, code)
	}
	return msg
}

func (r *rawSession) toRcpt(rcpt string) (int, string, error) {
	r.t.Helper()
	if c, m, _ := r.cmd("EHLO client.test"); c != 250 {
		r.t.Fatalf("EHLO: %d %s", c, m)
	}
	if c, m, _ := r.cmd("MAIL FROM:<a@client.test>"); c != 250 {
		r.t.Fatalf("MAIL: %d %s", c, m)
	}
	return r.cmd("RCPT TO:<" + rcpt + ">")
}

func TestRcptBehaviors(t *testing.T) {
	s := start(t, Options{})
	tests := []struct {
		rcpt     string
		code     int
		enhanced string
	}{
		{"accept@x.test", 250, "2.1.5"},
		{"throttle@x.test", 421, "4.7.0"},
		{"busy@x.test", 450, "4.2.1"},
		{"greylist@x.test", 451, "4.7.1"},
		{"full@x.test", 452, "4.2.2"},
		{"nouser@x.test", 550, "5.1.1"},
	}
	for _, tt := range tests {
		t.Run(tt.rcpt, func(t *testing.T) {
			r := dial(t, s)
			code, msg, err := r.toRcpt(tt.rcpt)
			if err != nil {
				t.Fatal(err)
			}
			if code != tt.code || !strings.HasPrefix(msg, tt.enhanced) {
				t.Fatalf("got %d %q, want %d %s", code, msg, tt.code, tt.enhanced)
			}
		})
	}
}

func TestNoSuchUserIsMultiline(t *testing.T) {
	s := start(t, Options{})
	_, msg, _ := dial(t, s).toRcpt("nouser@x.test")
	if lines := strings.Split(msg, "\n"); len(lines) != 2 {
		t.Fatalf("want 2-line reply, got %q", msg)
	}
}

func TestScriptSequenceThenRepeatLast(t *testing.T) {
	s := start(t, Options{})
	s.Script("bob@x.test", Greylist, Accept)
	for i, want := range []int{451, 250, 250} {
		code, _, _ := dial(t, s).toRcpt("bob@x.test")
		if code != want {
			t.Fatalf("rcpt #%d: got %d, want %d", i, code, want)
		}
	}
}

func TestDataDotUnstuffingAndPolicyReject(t *testing.T) {
	s := start(t, Options{})
	r := dial(t, s)
	r.toRcpt("accept@x.test")
	r.cmd("DATA")
	r.conn.Write([]byte("Subject: x\r\n\r\n..leading dot\r\n.\r\n"))
	r.expect(250)
	got := s.Received()
	if len(got) != 1 || string(got[0].Data) != "Subject: x\r\n\r\n.leading dot\r\n" {
		t.Fatalf("received %+v", got)
	}

	r = dial(t, s)
	r.toRcpt("policy@x.test")
	r.cmd("DATA")
	r.conn.Write([]byte("hi\r\n.\r\n"))
	if msg := r.expect(554); !strings.HasPrefix(msg, "5.7.1") {
		t.Fatalf("policy reply %q", msg)
	}
	if len(s.Received()) != 1 {
		t.Fatal("rejected message must not be recorded as received")
	}
}

func TestBareLFIsAViolation(t *testing.T) {
	s := start(t, Options{})
	r := dial(t, s)
	r.toRcpt("accept@x.test")
	r.cmd("DATA")
	r.conn.Write([]byte("line one\nline two\r\n.\r\n"))
	r.expect(250)
	if len(s.Violations()) == 0 {
		t.Fatal("bare LF in DATA was not flagged")
	}
}

func TestHangTimesOutClient(t *testing.T) {
	s := start(t, Options{})
	r := dial(t, s)
	r.conn.SetDeadline(time.Now().Add(5 * time.Second))
	r.cmd("EHLO c")
	r.cmd("MAIL FROM:<a@c>")
	r.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err := r.cmd("RCPT TO:<hang@x.test>")
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("want timeout, got %v", err)
	}
}

func TestDropMidDataClosesConnection(t *testing.T) {
	s := start(t, Options{})
	r := dial(t, s)
	r.toRcpt("drop@x.test")
	r.cmd("DATA")
	r.conn.Write([]byte("first line\r\nsecond\r\n.\r\n"))
	if _, _, err := r.tp.ReadResponse(0); err == nil {
		t.Fatal("expected connection error after mid-DATA drop")
	}
	if len(s.Received()) != 0 {
		t.Fatal("dropped message must not be recorded")
	}
}

func TestDropAfterDataRecordsUnrepliedMessage(t *testing.T) {
	s := start(t, Options{})
	r := dial(t, s)
	r.toRcpt("dropafter@x.test")
	r.cmd("DATA")
	r.conn.Write([]byte("hi\r\n.\r\n"))
	if _, _, err := r.tp.ReadResponse(0); err == nil {
		t.Fatal("expected connection error")
	}
	got := s.Received()
	if len(got) != 1 || got[0].Replied {
		t.Fatalf("want one unreplied message, got %+v", got)
	}
}

func TestStartTLS(t *testing.T) {
	cfg, err := SelfSignedTLS()
	if err != nil {
		t.Fatal(err)
	}
	s := start(t, Options{TLS: cfg})
	r := dial(t, s)
	if _, msg, _ := r.cmd("EHLO c"); !strings.Contains(msg, "STARTTLS") {
		t.Fatalf("STARTTLS not advertised: %q", msg)
	}
	if code, _, _ := r.cmd("STARTTLS"); code != 220 {
		t.Fatalf("STARTTLS code %d", code)
	}
	tc := tls.Client(r.conn, &tls.Config{InsecureSkipVerify: true})
	if err := tc.Handshake(); err != nil {
		t.Fatal(err)
	}
	r.conn, r.tp = tc, textproto.NewConn(tc)
	if _, msg, _ := r.cmd("EHLO c"); strings.Contains(msg, "STARTTLS") {
		t.Fatal("STARTTLS advertised again after TLS started")
	}
}

func TestSplitRepliesStillParse(t *testing.T) {
	s := start(t, Options{SplitReplies: true})
	code, msg, err := dial(t, s).toRcpt("nouser@x.test")
	if err != nil || code != 550 || strings.Count(msg, "\n") != 1 {
		t.Fatalf("got %d %q %v", code, msg, err)
	}
}
