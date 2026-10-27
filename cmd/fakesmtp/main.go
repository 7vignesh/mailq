// Command fakesmtp runs the test SMTP server for local demos. Dev only.
//
// The recipient's local part picks the behavior: accept@, throttle@, busy@,
// greylist@, full@, nouser@, policy@, hang@, drop@, dropafter@, slow@.
// Anything else is accepted.
package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/7vignesh/mailq/test/fakesmtp"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:2525", "listen address")
	useTLS := flag.Bool("tls", true, "advertise STARTTLS with a throwaway self-signed certificate")
	slow := flag.Duration("slow", 30*time.Second, "how long slow@ waits before accepting")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "fakesmtp")

	opts := fakesmtp.Options{Addr: *addr, SlowDelay: *slow}
	if *useTLS {
		cfg, err := fakesmtp.SelfSignedTLS()
		if err != nil {
			log.Error("tls", "err", err)
			os.Exit(1)
		}
		opts.TLS = cfg
	}
	s, err := fakesmtp.Start(opts)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	log.Info("listening", "addr", s.Addr(), "tls", *useTLS)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	for _, r := range s.Received() {
		log.Info("received", "from", r.From, "to", r.To, "bytes", len(r.Data), "tls", r.TLS, "replied", r.Replied)
	}
	s.Close()
}
