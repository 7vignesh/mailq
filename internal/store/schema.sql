-- mailq schema. Applied on startup; every statement is idempotent.
-- All queue timestamps (next_attempt_at, locked_at, ...) are written by the
-- application clock, not now(), so tests can drive time with a fake clock and
-- so claim/reap comparisons use a single time source.

CREATE TABLE IF NOT EXISTS messages (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key  TEXT NOT NULL UNIQUE,
    payload_hash     TEXT NOT NULL,          -- sha256 of from/to/subject/body, detects key reuse with a different payload
    from_addr        TEXT NOT NULL,
    to_addr          TEXT NOT NULL,
    domain           TEXT NOT NULL,          -- recipient domain, lowercased
    subject          TEXT NOT NULL,
    body             TEXT NOT NULL,
    state            TEXT NOT NULL CHECK (state IN ('queued','sending','deferred','delivered','bounced','failed')),
    attempt_count    INT  NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL,
    locked_by        TEXT,                   -- worker id holding the lease while state = 'sending'
    locked_at        TIMESTAMPTZ,            -- lease start; a stale lease means the worker probably died
    created_at       TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL
);

-- Claim scans only due, claimable rows. Partial index keeps it small: terminal
-- rows (the vast majority over time) are not in it.
CREATE INDEX IF NOT EXISTS messages_due_idx
    ON messages (next_attempt_at)
    WHERE state IN ('queued', 'deferred');

-- Reaper scans only in-flight rows.
CREATE INDEX IF NOT EXISTS messages_sending_idx
    ON messages (locked_at)
    WHERE state = 'sending';

-- Append-only log of every delivery attempt, including reaped (crashed) ones.
CREATE TABLE IF NOT EXISTS attempts (
    id              BIGSERIAL PRIMARY KEY,
    message_id      UUID NOT NULL REFERENCES messages(id),
    attempt_number  INT  NOT NULL,
    worker_id       TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ NOT NULL,
    mx_host         TEXT NOT NULL DEFAULT '',
    smtp_code       INT  NOT NULL DEFAULT 0,  -- 0 when no SMTP reply was received (transport error)
    enhanced_code   TEXT NOT NULL DEFAULT '', -- RFC 3463, e.g. 5.1.1
    response_text   TEXT NOT NULL DEFAULT '',
    stage           TEXT NOT NULL DEFAULT '', -- SMTP stage the attempt ended in: dial, rcpt, data_end, ...
    error           TEXT NOT NULL DEFAULT '', -- transport error text, if any
    outcome         TEXT NOT NULL,            -- delivered | transient | permanent
    reason          TEXT NOT NULL DEFAULT ''  -- classifier sub-reason
);

CREATE INDEX IF NOT EXISTS attempts_message_idx ON attempts (message_id, id);
