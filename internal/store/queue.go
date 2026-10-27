package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Claim leases the oldest due message to workerID and moves it to 'sending'.
// It returns (nil, nil) when nothing is due.
//
// FOR UPDATE SKIP LOCKED lets many workers claim concurrently: each skips rows
// another transaction has locked instead of waiting on them, so two workers
// never get the same row and never block each other. The claim commits
// immediately; we do NOT hold a transaction open across the SMTP session.
// Ownership during the send is a lease (locked_by + locked_at) instead, which
// the reaper can break if the worker dies.
func (s *Store) Claim(ctx context.Context, workerID string, now time.Time) (*Message, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE messages
		SET state = 'sending', locked_by = $1, locked_at = $2, updated_at = $2
		WHERE id = (
			SELECT id FROM messages
			WHERE state IN ('queued', 'deferred') AND next_attempt_at <= $2
			ORDER BY next_attempt_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING `+messageCols, workerID, now)
	m, err := scanMessage(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: claim: %w", err)
	}
	return &m, nil
}

// Release gives a claimed message back without counting an attempt, e.g.
// when the per-domain throttle says "not yet". It becomes claimable at 'at'.
// attemptCount is the value the claim returned; see Complete for fencing.
func (s *Store) Release(ctx context.Context, workerID, id string, attemptCount int, at, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE messages
		SET state = CASE WHEN attempt_count = 0 THEN 'queued' ELSE 'deferred' END,
		    next_attempt_at = $4, locked_by = NULL, locked_at = NULL, updated_at = $5
		WHERE id = $1::uuid AND state = 'sending' AND locked_by = $2 AND attempt_count = $3`,
		id, workerID, attemptCount, at, now)
	if err != nil {
		return fmt.Errorf("store: release: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Completion is the result of one delivery attempt and the state it leads to.
type Completion struct {
	WorkerID      string
	MessageID     string
	AttemptNumber int
	StartedAt     time.Time
	FinishedAt    time.Time
	MXHost        string
	SMTPCode      int
	EnhancedCode  string
	ResponseText  string
	Stage         string
	Error         string
	Outcome       string
	Reason        string

	NewState      State     // deferred, delivered, bounced or failed
	NextAttemptAt time.Time // only meaningful for deferred
}

// Complete records the attempt and moves the message out of 'sending' in one
// transaction.
//
// The state update is fenced on (locked_by, attempt_count). The reaper
// increments attempt_count, so if it already broke this lease the update
// matches no row, even if the same worker re-claimed the message since,
// and ErrLeaseLost is returned. The attempt row is still committed, because
// the attempt really happened and the history should show it.
func (s *Store) Complete(ctx context.Context, c Completion) error {
	switch c.NewState {
	case StateDeferred, StateDelivered, StateBounced, StateFailed:
	default:
		return fmt.Errorf("store: complete: invalid target state %q", c.NewState)
	}
	next := c.NextAttemptAt
	if c.NewState != StateDeferred {
		next = c.FinishedAt
	}

	lost := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO attempts (message_id, attempt_number, worker_id, started_at, finished_at, mx_host,
			                      smtp_code, enhanced_code, response_text, stage, error, outcome, reason)
			VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
			c.MessageID, c.AttemptNumber, c.WorkerID, c.StartedAt, c.FinishedAt, c.MXHost,
			c.SMTPCode, c.EnhancedCode, c.ResponseText, c.Stage, c.Error, c.Outcome, c.Reason)
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
			UPDATE messages
			SET state = $3, attempt_count = attempt_count + 1, next_attempt_at = $4,
			    locked_by = NULL, locked_at = NULL, updated_at = $5
			WHERE id = $1::uuid AND state = 'sending' AND locked_by = $2 AND attempt_count = $6`,
			c.MessageID, c.WorkerID, c.NewState, next, c.FinishedAt, c.AttemptNumber-1)
		if err != nil {
			return err
		}
		lost = tag.RowsAffected() == 0
		return nil
	})
	if err != nil {
		return fmt.Errorf("store: complete: %w", err)
	}
	if lost {
		return ErrLeaseLost
	}
	return nil
}

// ReapReason is recorded on the synthetic attempt written for a reaped lease.
const ReapReason = "lease_expired"

// Reap returns messages whose lease started before staleBefore to 'deferred',
// due immediately, and logs a synthetic attempt so the history shows that an
// attempt was lost. A lease only goes stale if its worker crashed or stalled
// past the lease duration, which must be longer than the worst-case attempt.
//
// This is where at-least-once comes from: the dead worker may have completed
// the SMTP transaction before dying, so the next attempt can send a duplicate.
func (s *Store) Reap(ctx context.Context, staleBefore, now time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		WITH stale AS (
			SELECT id, locked_by, locked_at FROM messages
			WHERE state = 'sending' AND locked_at < $1
			FOR UPDATE SKIP LOCKED
		), reaped AS (
			UPDATE messages m
			SET state = 'deferred', attempt_count = m.attempt_count + 1, next_attempt_at = $2,
			    locked_by = NULL, locked_at = NULL, updated_at = $2
			FROM stale WHERE m.id = stale.id
			RETURNING m.id, m.attempt_count, stale.locked_by, stale.locked_at
		)
		INSERT INTO attempts (message_id, attempt_number, worker_id, started_at, finished_at, error, outcome, reason)
		SELECT id, attempt_count, locked_by, locked_at, $2,
		       'lease expired: worker died or stalled mid-attempt', 'transient', $3
		FROM reaped`,
		staleBefore, now, ReapReason)
	if err != nil {
		return 0, fmt.Errorf("store: reap: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
