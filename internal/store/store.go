// Package store owns every SQL statement in mailq: the messages queue and the
// append-only attempts log.
package store

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// State is a message's position in the delivery state machine.
type State string

const (
	StateQueued    State = "queued"    // accepted, never attempted
	StateSending   State = "sending"   // claimed by a worker (leased)
	StateDeferred  State = "deferred"  // transient failure, waiting for next_attempt_at
	StateDelivered State = "delivered" // terminal: remote server accepted with 2xx
	StateBounced   State = "bounced"   // terminal: permanent failure (hard bounce)
	StateFailed    State = "failed"    // terminal: gave up after max age
)

var (
	ErrNotFound            = errors.New("store: message not found")
	ErrIdempotencyConflict = errors.New("store: idempotency key reused with a different payload")
	// ErrLeaseLost means the row is no longer leased to this worker: the
	// reaper decided the worker was dead and handed the message to someone
	// else. The caller's result must not overwrite the newer state.
	ErrLeaseLost = errors.New("store: lease lost")
)

type Message struct {
	ID             string     `json:"id"`
	IdempotencyKey string     `json:"idempotency_key"`
	From           string     `json:"from"`
	To             string     `json:"to"`
	Domain         string     `json:"domain"`
	Subject        string     `json:"subject"`
	Body           string     `json:"body"`
	State          State      `json:"state"`
	AttemptCount   int        `json:"attempt_count"`
	NextAttemptAt  time.Time  `json:"next_attempt_at"`
	LockedBy       *string    `json:"locked_by,omitempty"`
	LockedAt       *time.Time `json:"locked_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`

	payloadHash string
}

type Attempt struct {
	ID            int64     `json:"id"`
	AttemptNumber int       `json:"attempt_number"`
	WorkerID      string    `json:"worker_id"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	MXHost        string    `json:"mx_host"`
	SMTPCode      int       `json:"smtp_code"`
	EnhancedCode  string    `json:"enhanced_code"`
	ResponseText  string    `json:"response_text"`
	Stage         string    `json:"stage"`
	Error         string    `json:"error"`
	Outcome       string    `json:"outcome"`
	Reason        string    `json:"reason"`
}

type Store struct {
	pool *pgxpool.Pool
}

// Open connects to Postgres. Pool settings can be tuned through the URL
// (e.g. pool_max_conns=20).
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Migrate applies the embedded schema. Statements are idempotent, so this is
// safe to run from every process on startup. A real system with schema
// evolution would use versioned migrations instead.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	return nil
}

type NewMessage struct {
	IdempotencyKey string
	From           string
	To             string
	Domain         string
	Subject        string
	Body           string
}

const messageCols = `id::text, idempotency_key, payload_hash, from_addr, to_addr, domain, subject, body,
	state, attempt_count, next_attempt_at, locked_by, locked_at, created_at, updated_at`

// Enqueue inserts a message unless its idempotency key already exists.
// created reports whether this call inserted the row. Concurrent calls with
// the same key are serialized by the unique index: exactly one inserts, the
// others observe its row. Reusing a key with a different payload returns the
// original message together with ErrIdempotencyConflict.
func (s *Store) Enqueue(ctx context.Context, m NewMessage, now time.Time) (msg Message, created bool, err error) {
	hash := payloadHash(m)
	row := s.pool.QueryRow(ctx, `
		INSERT INTO messages (idempotency_key, payload_hash, from_addr, to_addr, domain, subject, body,
		                      state, next_attempt_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'queued', $8, $8, $8)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+messageCols,
		m.IdempotencyKey, hash, m.From, m.To, m.Domain, m.Subject, m.Body, now)
	msg, err = scanMessage(row)
	if err == nil {
		return msg, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Message{}, false, fmt.Errorf("store: enqueue: %w", err)
	}

	// Conflict: the key exists (possibly committed by a concurrent request a
	// moment ago). READ COMMITTED gives this new statement a fresh snapshot.
	row = s.pool.QueryRow(ctx, `SELECT `+messageCols+` FROM messages WHERE idempotency_key = $1`, m.IdempotencyKey)
	msg, err = scanMessage(row)
	if err != nil {
		return Message{}, false, fmt.Errorf("store: enqueue lookup: %w", err)
	}
	if msg.payloadHash != hash {
		return msg, false, ErrIdempotencyConflict
	}
	return msg, false, nil
}

// Get returns a message and its full attempt history, oldest first.
func (s *Store) Get(ctx context.Context, id string) (Message, []Attempt, error) {
	msg, err := scanMessage(s.pool.QueryRow(ctx, `SELECT `+messageCols+` FROM messages WHERE id = $1::uuid`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, nil, ErrNotFound
	}
	if err != nil {
		return Message{}, nil, fmt.Errorf("store: get: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, attempt_number, worker_id, started_at, finished_at, mx_host, smtp_code,
		       enhanced_code, response_text, stage, error, outcome, reason
		FROM attempts WHERE message_id = $1::uuid ORDER BY id`, id)
	if err != nil {
		return Message{}, nil, fmt.Errorf("store: get attempts: %w", err)
	}
	attempts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Attempt, error) {
		var a Attempt
		err := r.Scan(&a.ID, &a.AttemptNumber, &a.WorkerID, &a.StartedAt, &a.FinishedAt, &a.MXHost,
			&a.SMTPCode, &a.EnhancedCode, &a.ResponseText, &a.Stage, &a.Error, &a.Outcome, &a.Reason)
		return a, err
	})
	if err != nil {
		return Message{}, nil, fmt.Errorf("store: scan attempts: %w", err)
	}
	return msg, attempts, nil
}

// CountByState returns the number of messages in each state, for the queue
// depth gauge.
func (s *Store) CountByState(ctx context.Context) (map[State]int, error) {
	rows, err := s.pool.Query(ctx, `SELECT state, count(*) FROM messages GROUP BY state`)
	if err != nil {
		return nil, fmt.Errorf("store: count: %w", err)
	}
	defer rows.Close()
	counts := map[State]int{}
	for rows.Next() {
		var st State
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		counts[st] = n
	}
	return counts, rows.Err()
}

// CountByStateString is CountByState keyed by plain strings, for the
// metrics package.
func (s *Store) CountByStateString(ctx context.Context) (map[string]int, error) {
	c, err := s.CountByState(ctx)
	out := make(map[string]int, len(c))
	for k, v := range c {
		out[string(k)] = v
	}
	return out, err
}

func scanMessage(row pgx.Row) (Message, error) {
	var m Message
	err := row.Scan(&m.ID, &m.IdempotencyKey, &m.payloadHash, &m.From, &m.To, &m.Domain, &m.Subject, &m.Body,
		&m.State, &m.AttemptCount, &m.NextAttemptAt, &m.LockedBy, &m.LockedAt, &m.CreatedAt, &m.UpdatedAt)
	return m, err
}

// payloadHash length-prefixes each field so ("ab","c") and ("a","bc") differ.
func payloadHash(m NewMessage) string {
	h := sha256.New()
	for _, f := range []string{m.From, m.To, m.Subject, m.Body} {
		fmt.Fprintf(h, "%d:%s", len(f), f)
	}
	return hex.EncodeToString(h.Sum(nil))
}
