// Command api serves POST /messages, GET /messages/{id} and /metrics.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/7vignesh/mailq/internal/api"
	"github.com/7vignesh/mailq/internal/clock"
	"github.com/7vignesh/mailq/internal/env"
	"github.com/7vignesh/mailq/internal/store"
)

// Matches docker-compose.yml. Dev-only credentials; set MAILQ_DATABASE_URL elsewhere.
const devDatabaseURL = "postgres://mailq:mailq@localhost:55432/mailq?sslmode=disable"

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "api")
	if err := run(log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, env.String("MAILQ_DATABASE_URL", devDatabaseURL))
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              env.String("MAILQ_API_ADDR", "127.0.0.1:8080"),
		Handler:           api.New(st, clock.Real{}, log).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("listening", "addr", srv.Addr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
