// Package fakesmtp is a scriptable SMTP server for tests and local demos.
//
// Each recipient gets a Behavior that decides how the server answers. A
// behavior is chosen, in order, from: an explicit Script for that address,
// the recipient's local part if it names a behavior (nouser@x -> NoSuchUser),
// or Accept.
//
// The server is deliberately strict about line endings: bare LF or bare CR
// in commands or DATA is recorded as a violation, so client bugs fail tests
// instead of being silently tolerated as many real servers do.
package fakesmtp

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type Behavior string

const (
	Accept              Behavior = "accept"    // 250 2.0.0 after DATA
	Throttle            Behavior = "throttle"  // 421 4.7.0 at RCPT, then closes the connection
	MailboxBusy         Behavior = "busy"      // 450 4.2.1 at RCPT
	Greylist            Behavior = "greylist"  // 451 4.7.1 at RCPT
	InsufficientStorage Behavior = "full"      // 452 4.2.2 at RCPT
	NoSuchUser          Behavior = "nouser"    // multiline 550 5.1.1 at RCPT
	PolicyReject        Behavior = "policy"    // 554 5.7.1 after DATA
	Hang                Behavior = "hang"      // never answers RCPT
	DropMidData         Behavior = "drop"      // closes the connection after the first DATA line
	DropAfterData       Behavior = "dropafter" // reads the whole message, closes without a reply
	SlowAccept          Behavior = "slow"      // waits Options.SlowDelay, then 250
)

var byName = map[string]Behavior{}

func init() {
	for _, b := range []Behavior{Accept, Throttle, MailboxBusy, Greylist, InsufficientStorage,
		NoSuchUser, PolicyReject, Hang, DropMidData, DropAfterData, SlowAccept} {
		byName[string(b)] = b
	}
}

type Options struct {
	Addr         string         // listen address, default 127.0.0.1:0
	Hostname     string         // used in the greeting, default fake.mailq.test
	TLS          *tls.Config    // if set, STARTTLS is advertised
	SplitReplies bool           // write replies a few bytes at a time to exercise client buffering
	SlowDelay    time.Duration  // for SlowAccept, default 2s
	OnReceive    func(Received) // called when a message is read in full, before any reply
}

// Received is a message the server read in full.
type Received struct {
	From, To string
	Raw      []byte // DATA exactly as sent on the wire, dot-stuffed, without the final ".\r\n"
	Data     []byte // Raw with dot-stuffing removed
	TLS      bool
	Replied  bool // false for DropAfterData: the client never learned the outcome
}

type Server struct {
	opts Options
	ln   net.Listener

	mu         sync.Mutex
	scripts    map[string][]Behavior
	received   []Received
	violations []string
	conns      map[net.Conn]struct{}
	nConns     int
	closed     bool
	wg         sync.WaitGroup
}

// Start listens and serves in the background until Close.
func Start(opts Options) (*Server, error) {
	if opts.Addr == "" {
		opts.Addr = "127.0.0.1:0"
	}
	if opts.Hostname == "" {
		opts.Hostname = "fake.mailq.test"
	}
	if opts.SlowDelay == 0 {
		opts.SlowDelay = 2 * time.Second
	}
	ln, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return nil, err
	}
	s := &Server{opts: opts, ln: ln, scripts: map[string][]Behavior{}, conns: map[net.Conn]struct{}{}}
	s.wg.Add(1)
	go s.serve()
	return s, nil
}

func (s *Server) Addr() string { return s.ln.Addr().String() }

// Script sets the behaviors for successive RCPT commands to rcpt. The last
// behavior repeats, so Script(a, Greylist, Accept) defers once then accepts.
func (s *Server) Script(rcpt string, behaviors ...Behavior) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scripts[strings.ToLower(rcpt)] = behaviors
}

func (s *Server) Received() []Received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Received(nil), s.received...)
}

func (s *Server) Violations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.violations...)
}

func (s *Server) Connections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nConns
}

// Close stops listening and drops all open connections, including hung ones.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	err := s.ln.Close()
	s.wg.Wait()
	return err
}

func (s *Server) serve() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			c.Close()
			return
		}
		s.conns[c] = struct{}{}
		s.nConns++
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			sess := &session{srv: s}
			sess.setConn(c)
			sess.run()
			s.mu.Lock()
			delete(s.conns, sess.conn)
			s.mu.Unlock()
			sess.conn.Close()
		}()
	}
}

func (s *Server) behaviorFor(rcpt string) Behavior {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.ToLower(rcpt)
	if seq := s.scripts[key]; len(seq) > 0 {
		b := seq[0]
		if len(seq) > 1 {
			s.scripts[key] = seq[1:]
		}
		return b
	}
	local, _, _ := strings.Cut(key, "@")
	if b, ok := byName[local]; ok {
		return b
	}
	return Accept
}

func (s *Server) violation(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.violations = append(s.violations, fmt.Sprintf(format, args...))
}

type session struct {
	srv      *Server
	conn     net.Conn
	r        *bufio.Reader
	tls      bool
	from, to string
	behavior Behavior
}

func (ss *session) setConn(c net.Conn) {
	ss.conn = c
	ss.r = bufio.NewReader(c)
}

// reply writes a possibly multiline reply: "550-a\r\n550 b\r\n".
func (ss *session) reply(code int, lines ...string) {
	var b strings.Builder
	for i, l := range lines {
		sep := "-"
		if i == len(lines)-1 {
			sep = " "
		}
		fmt.Fprintf(&b, "%d%s%s\r\n", code, sep, l)
	}
	out := []byte(b.String())
	if !ss.srv.opts.SplitReplies {
		ss.conn.Write(out)
		return
	}
	for len(out) > 0 {
		n := min(3, len(out))
		ss.conn.Write(out[:n])
		out = out[n:]
		time.Sleep(time.Millisecond)
	}
}

// readLine returns one CRLF-terminated line without the CRLF.
func (ss *session) readLine() (string, error) {
	line, err := ss.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		ss.srv.violation("bare LF in command: %q", line)
		return strings.TrimRight(line, "\r\n"), nil
	}
	return line[:len(line)-2], nil
}

func (ss *session) run() {
	ss.reply(220, ss.srv.opts.Hostname+" ESMTP fakesmtp")
	for {
		line, err := ss.readLine()
		if err != nil {
			return
		}
		verb, arg, _ := strings.Cut(line, " ")
		switch strings.ToUpper(verb) {
		case "EHLO":
			lines := []string{ss.srv.opts.Hostname + " greets " + arg, "8BITMIME", "ENHANCEDSTATUSCODES", "SIZE 10485760"}
			if ss.srv.opts.TLS != nil && !ss.tls {
				lines = append(lines, "STARTTLS")
			}
			ss.reply(250, append(lines, "HELP")...)
		case "HELO":
			ss.reply(250, ss.srv.opts.Hostname)
		case "STARTTLS":
			if ss.srv.opts.TLS == nil || ss.tls {
				ss.reply(502, "5.5.1 STARTTLS not available")
				continue
			}
			ss.reply(220, "2.0.0 Ready to start TLS")
			tc := tls.Server(ss.conn, ss.srv.opts.TLS)
			if err := tc.Handshake(); err != nil {
				return
			}
			ss.srv.mu.Lock()
			delete(ss.srv.conns, ss.conn)
			ss.srv.conns[tc] = struct{}{}
			ss.srv.mu.Unlock()
			ss.setConn(tc)
			ss.tls = true
			ss.from, ss.to = "", "" // RFC 3207: forget all state after TLS starts
		case "MAIL":
			ss.from = extractPath(arg)
			ss.reply(250, "2.1.0 OK")
		case "RCPT":
			if !ss.handleRcpt(extractPath(arg)) {
				return
			}
		case "DATA":
			if !ss.handleData() {
				return
			}
		case "RSET":
			ss.from, ss.to = "", ""
			ss.reply(250, "2.0.0 OK")
		case "NOOP":
			ss.reply(250, "2.0.0 OK")
		case "QUIT":
			ss.reply(221, "2.0.0 Bye")
			return
		default:
			ss.reply(500, "5.5.2 Command not recognized")
		}
	}
}

// handleRcpt returns false if the connection should be closed.
func (ss *session) handleRcpt(rcpt string) bool {
	ss.behavior = ss.srv.behaviorFor(rcpt)
	switch ss.behavior {
	case Throttle:
		ss.reply(421, "4.7.0 Too many messages from your IP, rate limited, try again later")
		return false
	case MailboxBusy:
		ss.reply(450, "4.2.1 Mailbox busy, try again later")
	case Greylist:
		ss.reply(451, "4.7.1 Greylisted, please try again later")
	case InsufficientStorage:
		ss.reply(452, "4.2.2 Insufficient storage: mailbox full")
	case NoSuchUser:
		ss.reply(550, "5.1.1 The email account that you tried to reach does not exist.",
			"5.1.1 Please check the recipient address and try again.")
	case Hang:
		io.Copy(io.Discard, ss.r) // blocks until the client gives up or Close
		return false
	default:
		ss.to = rcpt
		ss.reply(250, "2.1.5 OK")
	}
	return true
}

// handleData returns false if the connection should be closed.
func (ss *session) handleData() bool {
	if ss.to == "" {
		ss.reply(503, "5.5.1 No valid recipients")
		return true
	}
	ss.reply(354, "Go ahead, end with <CRLF>.<CRLF>")

	var raw, data []byte
	first := true
	for {
		line, err := ss.r.ReadString('\n')
		if err != nil {
			return false
		}
		if line == ".\r\n" {
			break
		}
		if !strings.HasSuffix(line, "\r\n") || strings.Count(line, "\r") != 1 {
			ss.srv.violation("bare CR or LF in DATA: %q", line)
		}
		if first && ss.behavior == DropMidData {
			return false
		}
		first = false
		raw = append(raw, line...)
		data = append(data, strings.TrimPrefix(line, ".")...)
	}

	rec := Received{From: ss.from, To: ss.to, Raw: raw, Data: data, TLS: ss.tls, Replied: true}
	switch ss.behavior {
	case PolicyReject:
		ss.reply(554, "5.7.1 Message rejected due to local policy")
		return true
	case DropAfterData:
		rec.Replied = false
		ss.record(rec)
		return false
	case SlowAccept:
		ss.record(rec)
		time.Sleep(ss.srv.opts.SlowDelay)
	default:
		ss.record(rec)
	}
	ss.reply(250, "2.0.0 OK queued")
	ss.from, ss.to = "", ""
	return true
}

func (ss *session) record(r Received) {
	ss.srv.mu.Lock()
	ss.srv.received = append(ss.srv.received, r)
	ss.srv.mu.Unlock()
	if f := ss.srv.opts.OnReceive; f != nil {
		f(r)
	}
}

// extractPath turns "FROM:<a@b> SIZE=10" into "a@b".
func extractPath(arg string) string {
	_, rest, ok := strings.Cut(arg, "<")
	if !ok {
		return ""
	}
	path, _, _ := strings.Cut(rest, ">")
	return path
}
