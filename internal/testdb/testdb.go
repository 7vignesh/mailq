// Package testdb gives each test its own throwaway Postgres schema.
// Tests skip when MAILQ_TEST_DATABASE_URL is unset, so `go test ./...` works
// without a database and only exercises the pure packages.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/7vignesh/mailq/internal/store"
)

const EnvURL = "MAILQ_TEST_DATABASE_URL"

// New returns a migrated store isolated in a fresh schema, dropped on cleanup.
func New(t testing.TB) *store.Store {
	t.Helper()
	base := os.Getenv(EnvURL)
	if base == "" {
		t.Skipf("%s not set; skipping Postgres-backed test", EnvURL)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("testdb: connect: %v", err)
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	schema := "test_" + hex.EncodeToString(b)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("testdb: create schema: %v", err)
	}

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("testdb: parse url: %v", err)
	}
	q := u.Query()
	q.Set("search_path", schema) // pgx forwards unknown params as runtime params
	u.RawQuery = q.Encode()

	s, err := store.Open(ctx, u.String())
	if err != nil {
		t.Fatalf("testdb: open: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("testdb: migrate: %v", err)
	}
	t.Cleanup(func() {
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(ctx)
	})
	return s
}
