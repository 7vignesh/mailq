// Command worker claims due messages and delivers them over SMTP.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/7vignesh/mailq/internal/backoff"
	"github.com/7vignesh/mailq/internal/clock"
	"github.com/7vignesh/mailq/internal/env"
	"github.com/7vignesh/mailq/internal/smtp"
	"github.com/7vignesh/mailq/internal/store"
	"github.com/7vignesh/mailq/internal/worker"
)

const devDatabaseURL = "postgres://mailq:mailq@localhost:55432/mailq?sslmode=disable"

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "worker")
	if err := run(log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, client, bo, err := load()
	if err != nil {
		return err
	}
	st, err := store.Open(ctx, env.String("MAILQ_DATABASE_URL", devDatabaseURL))
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	w, err := worker.New(cfg, st, client, bo, clock.Real{}, log)
	if err != nil {
		return err
	}
	log.Info("starting", "worker_id", cfg.ID, "concurrency", cfg.Concurrency)
	w.Run(ctx)
	log.Info("stopped")
	return nil
}

func load() (worker.Config, *smtp.Client, backoff.Policy, error) {
	var errs []error
	intv := func(k string, d int) int { v, err := env.Int(k, d); errs = append(errs, err); return v }
	dur := func(k string, d time.Duration) time.Duration {
		v, err := env.Duration(k, d)
		errs = append(errs, err)
		return v
	}
	boolv := func(k string, d bool) bool { v, err := env.Bool(k, d); errs = append(errs, err); return v }

	host, _ := os.Hostname()
	helo := env.String("MAILQ_HELO_NAME", "localhost")
	cfg := worker.Config{
		ID:             fmt.Sprintf("%s-%d-%s", host, os.Getpid(), randHex(3)),
		Concurrency:    intv("MAILQ_CONCURRENCY", 16),
		PollInterval:   dur("MAILQ_POLL_INTERVAL", 500*time.Millisecond),
		AttemptTimeout: dur("MAILQ_ATTEMPT_TIMEOUT", 15*time.Minute),
		Lease:          dur("MAILQ_LEASE", 20*time.Minute),
		ReapInterval:   dur("MAILQ_REAP_INTERVAL", 30*time.Second),
		HeloName:       helo,
	}

	t := smtp.DefaultTimeouts()
	t.Dial = dur("MAILQ_DIAL_TIMEOUT", t.Dial)
	t.Command = dur("MAILQ_COMMAND_TIMEOUT", t.Command)
	t.DataEnd = dur("MAILQ_DATA_END_TIMEOUT", t.DataEnd)
	client := &smtp.Client{
		HeloName:   helo,
		Resolver:   net.DefaultResolver,
		Dialer:     &net.Dialer{},
		Port:       env.String("MAILQ_SMTP_PORT", "25"),
		Timeouts:   t,
		StartTLS:   boolv("MAILQ_STARTTLS", true),
		MaxMXHosts: intv("MAILQ_MAX_MX_HOSTS", 3),
	}
	// Dev only: send every message to one server (e.g. cmd/fakesmtp)
	// instead of resolving MX records.
	if addr := env.String("MAILQ_STATIC_ROUTE", ""); addr != "" {
		client.Resolver, client.Dialer = smtp.StaticRoute(addr)
	}

	bo := backoff.Default()
	bo.Min = dur("MAILQ_RETRY_MIN", bo.Min)
	bo.Base = dur("MAILQ_RETRY_BASE", bo.Base)
	bo.Max = dur("MAILQ_RETRY_MAX", bo.Max)
	bo.MaxAge = dur("MAILQ_MAX_AGE", bo.MaxAge)

	for _, err := range errs {
		if err != nil {
			return cfg, nil, bo, err
		}
	}
	return cfg, client, bo, nil
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}
