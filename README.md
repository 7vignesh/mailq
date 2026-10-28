# mailq

A small outbound email delivery engine (MTA) in Go. An HTTP API accepts
messages into a Postgres-backed queue, and workers deliver them over SMTP
to the recipient's MX hosts. Workers retry with jittered backoff, throttle
each recipient domain, and record every attempt.

It is a learning project. The scope is deliberately small. The goal is a
system whose decisions can be explained and whose failure modes are tested,
not a feature-complete MTA. Things it does not do are listed under
[Out of scope](#out-of-scope).

- [Quick start](#quick-start)
- [Architecture](#architecture)
- [State machine](#state-machine)
- [Queue and claiming](#queue-and-claiming)
- [SMTP delivery](#smtp-delivery)
- [Classifier and ambiguous responses](#classifier-and-ambiguous-responses)
- [Retry design](#retry-design)
- [Throttling design](#throttling-design)
- [Delivery guarantees](#delivery-guarantees)
- [Observability](#observability)
- [Running the tests](#running-the-tests)
- [Crash demo](#crash-demo)
- [Sending to a real mailbox](#sending-to-a-real-mailbox)
- [Unverified assumptions](#unverified-assumptions)
- [What broke while I built this](#what-broke-while-i-built-this)
- [Out of scope](#out-of-scope)
- [Configuration](#configuration)

## Quick start

Needs Go 1.26+ (per `go.mod`) and Docker. Postgres listens on host port 55432 so it does
not collide with other local instances.

```sh
docker compose up -d --wait          # Postgres
go build -o bin/ ./cmd/...           # bin/api, bin/worker, bin/fakesmtp

bin/fakesmtp -addr 127.0.0.1:2525 &  # dev-only fake SMTP server
bin/api &                            # :8080, migrates the schema on start
MAILQ_STATIC_ROUTE=127.0.0.1:2525 bin/worker &   # skip DNS, send everything to the fake

curl -s localhost:8080/messages -d '{"from":"me@mailq.test","to":"nouser@example.org",
  "subject":"hi","body":"hello","idempotency_key":"k1"}'
curl -s localhost:8080/messages/<id>  # message plus full attempt history
curl -s localhost:8080/metrics        # API: ingest counts, queue depth
curl -s localhost:9091/metrics        # worker: attempts, latency, throttling
```

On Windows PowerShell, set `$env:MAILQ_STATIC_ROUTE="127.0.0.1:2525"`
before starting `bin\worker.exe`.

The fake server picks a behavior from the recipient's local part: `accept@`,
`throttle@` (421), `busy@` (450), `greylist@` (451), `full@` (452),
`nouser@` (550 5.1.1), `policy@` (554 5.7.1), `hang@`, `drop@` (connection
drop mid-DATA), `dropafter@` (drop after the full body, no reply), `slow@`.

## Architecture

```
            POST /messages                         SMTP :25
 client ───────────────────► cmd/api ──┐     ┌──► recipient MX
            GET /messages/{id}         │     │
                                       ▼     │
                              ┌────────────────┐
                              │    Postgres    │
                              │ messages       │
                              │ attempts       │
                              └────────────────┘
                                       ▲     │
                     claim / complete  │     │
                                    cmd/worker ── N goroutines + reaper
```

Two binaries over shared `internal/` packages. Postgres is the only shared
state, so the API and workers start, stop and scale independently.

| Package | Job |
|---|---|
| `cmd/api`, `cmd/worker` | Thin mains: read env config, wire dependencies, handle signals. |
| `cmd/fakesmtp` | Dev-only. Runs the test SMTP server for demos. |
| `internal/api` | HTTP handlers and input validation. |
| `internal/store` | Every SQL statement: enqueue, claim, release, complete, reap. |
| `internal/smtp` | Hand-rolled SMTP client: MX lookup, STARTTLS, per-stage deadlines. |
| `internal/classifier` | Pure function: SMTP result to delivered / transient / permanent. |
| `internal/backoff` | Pure retry schedule. |
| `internal/throttle` | Per-domain concurrency cap and AIMD token bucket (in memory). |
| `internal/worker` | The loop: claim, throttle, send, classify, record, reschedule. |
| `internal/metrics` | Prometheus metric definitions. |
| `test/fakesmtp` | Scriptable fake SMTP server. |
| `test/` | Integration tests for the whole pipeline. |

## State machine

```mermaid
stateDiagram-v2
    [*] --> queued: POST /messages
    queued --> sending: claimed
    deferred --> sending: claimed once next_attempt_at has passed
    sending --> delivered: 2xx after DATA
    sending --> bounced: permanent failure
    sending --> deferred: transient failure, younger than max age
    sending --> failed: transient failure, max age reached
    sending --> deferred: lease expired (worker died), reaper
    sending --> queued: released by throttle (no attempt counted)
    delivered --> [*]
    bounced --> [*]
    failed --> [*]
```

`delivered`, `bounced` and `failed` are terminal. `sending` always has an
owner (`locked_by`) and a lease start (`locked_at`). A released message goes
back to `queued` if it was never attempted, or to `deferred` otherwise.

## Queue and claiming

A worker claims one message per query:

```sql
UPDATE messages SET state = 'sending', locked_by = $1, locked_at = $2
WHERE id = (SELECT id FROM messages
            WHERE state IN ('queued','deferred') AND next_attempt_at <= $2
              AND domain <> ALL($3)          -- domains the throttle has no room for
            ORDER BY next_attempt_at LIMIT 1
            FOR UPDATE SKIP LOCKED)
RETURNING ...
```

**Why SKIP LOCKED.** Without it, concurrent claimers all pick the same
oldest row, and all but one block on its lock. With it, each claimer skips
rows locked by others and takes the next one. Claims never overlap and never
wait on each other. A test runs 8 concurrent claimers over 200 rows and
checks that no row is claimed twice.

**Lease, not a held transaction.** The claim commits immediately. During
the SMTP session, ownership comes from `locked_by` and `locked_at`, not from
an open transaction. Holding a row lock for the whole session would make
crash recovery automatic, because Postgres releases locks when a connection
dies. The cost is that every in-flight message would pin a pooled
connection and an open transaction for up to the 10-minute end-of-DATA
timeout. That caps concurrency at the pool size and holds back vacuum.

**Reaper.** Each worker runs a reaper. It finds `sending` rows whose lease
is older than `MAILQ_LEASE` and moves them to `deferred`, due immediately.
It also writes a synthetic attempt (`reason = lease_expired`), so the
history shows that an attempt was lost. The worker refuses to start if the
lease is not longer than the attempt timeout. Otherwise the reaper could
hand a live attempt to a second worker.

**Fencing.** `Complete` and `Release` only apply if `locked_by` still
matches *and* `attempt_count` is unchanged. The reaper increments
`attempt_count`, so a worker that was presumed dead and then reports late
cannot overwrite the newer state. This holds even when the same worker has
since re-claimed the message. The late attempt is still logged in
`attempts`, because it really happened.

**Idempotency.** `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`, then
a read of the existing row. The unique index serializes concurrent inserts,
so exactly one wins. A test sends 25 concurrent identical POSTs and checks
that exactly one message is created and exactly one copy reaches the SMTP
server. A reused key with a *different* payload (detected by a SHA-256 of
from/to/subject/body) returns 409 and the original message id, rather than
silently returning a message the caller did not send.

## SMTP delivery

Hand-rolled on `net.Conn`, `net/textproto` (reply parsing, dot-stuffing)
and `crypto/tls`. I rejected `net/smtp` because it is frozen, has no
per-command deadlines, and makes it hard to tell "the server replied 4xx"
from "the connection died after DATA". The classifier and the duplicate
analysis depend on exactly that distinction.

- **MX resolution.** `LookupMX`, tried in preference order (up to
  `MAILQ_MAX_MX_HOSTS`). No MX records means the domain's A/AAAA records
  act as an implicit MX (RFC 5321 §5.1). A null MX (RFC 7505) and NXDOMAIN
  are reported as their own error kinds.
- **When to try the next MX.** Only on host-level failures before the mail
  transaction: dial, greeting, EHLO, STARTTLS. A reply to MAIL, RCPT or
  DATA is about the message, not the host. A 550 at RCPT is the answer, and
  asking the backup MX would just be asking twice.
- **Opportunistic STARTTLS.** Used when advertised. The certificate is not
  verified (RFC 7435 style): most MX certificates don't match the MX
  hostname, and refusing unverified TLS would only mean falling back to
  plaintext. Enforcement is MTA-STS/DANE territory (out of scope). EHLO is
  re-sent after the handshake, as RFC 3207 requires.
- **Deadlines on every stage.** Each read and write gets its own deadline,
  and the whole attempt has a context deadline. `context.AfterFunc` closes
  the socket on cancellation, so shutdown can interrupt a blocked read.

| Stage | Default | Notes |
|---|---|---|
| Dial | 30s | |
| Greeting | 2m | RFC 5321 §4.5.3.2 suggests 5m |
| Each command (EHLO, STARTTLS, MAIL, RCPT, DATA) | 2m | RFC suggests 5m for MAIL/RCPT |
| Writing the body | 3m | |
| Reply to the final `.` | 10m | Kept at the RFC value on purpose, see below |
| Whole attempt | 15m | Lease is 20m |

The end-of-DATA timeout is not shortened because giving up while the
server is still processing is the most likely way to cause a duplicate. The
server may finish and accept a message we have already decided to retry.

**Message format.** Plain text, quoted-printable body. The message is 7-bit
clean and no line exceeds 76 characters, far below SMTP's 998 limit. CR and
LF are stripped from header values. The API rejects them too, so a subject
cannot inject a `Bcc:` header. The Message-ID is derived from the queue id,
so retries of the same message carry the same Message-ID.

Tests cover the places hand-rolled clients usually break. The fake server
flags any bare CR or LF it receives. Cases include lines that are a single
`.`, lines starting with dots, mixed `\n` and `\r\n`, bare `\r`, a missing
final newline, a 5000-character line, multiline replies, and replies written
3 bytes at a time across TCP reads.

## Classifier and ambiguous responses

`internal/classifier` is a pure function. It takes the reply code, enhanced
status code (RFC 3463), reply text, SMTP stage and transport error, and
returns an outcome, a sub-reason, and the name of the rule that decided.
The rule name goes into the logs, so "why did this bounce?" has an answer.

| Outcome | Sub-reasons |
|---|---|
| delivered | |
| transient | `mailbox_full`, `mailbox_busy`, `throttled`, `greylisted`, `connection_failure`, `timeout`, `interrupted`, `temporary_other`, `ambiguous` |
| permanent | `no_such_user`, `policy_blocked`, `bad_domain`, `content_rejected`, `permanent_other` |

Rules, first match wins:

1. **No reply.** Timeouts and connection failures are transient. NXDOMAIN
   and null MX are permanent `bad_domain`.
2. **2xx is delivered only as the reply to the end of DATA.** A 2xx
   anywhere else means the session ended in a way we don't understand.
3. **A reply class other than 2, 4 or 5** is ambiguous.
4. **The basic code and the enhanced code disagree on the class** (e.g.
   `550 4.2.2`): ambiguous. RFC 5321 clients act on the basic code, but a
   server that sends contradictory codes is not a safe basis for a bounce.
5. **X.2.2 (mailbox full) is transient even under a 5xx.** This is a
   judgment call, not an RFC rule. Full mailboxes usually get emptied.
6. **A 5xx whose text says "try again later", "temporarily", "rate limit"
   or "too many"** is ambiguous: the code says permanent, the words say
   temporary.
7. **4xx:** the sub-reason comes from the text (greylist wording), then the
   code (421 means throttled), then the enhanced code (X.7.x means
   throttled, X.2.x means mailbox), then the basic code.
8. **5xx:** the sub-reason comes from the enhanced code (5.1.1 means no
   such user, 5.1.2/5.1.10/5.4.4 mean bad domain, 5.7.x means policy,
   5.6.x/5.3.4 mean content), then the text. A clean 5xx with nothing
   recognizable is still permanent, because the server was unambiguous
   about the class.

**Ambiguous always means transient.** A wrong retry costs a delay and a few
extra attempts, and the max age bounds it. A wrong bounce loses mail the
sender may never recover. The cost of this choice is that some real hard
bounces get retried for up to 72 hours. That wastes attempts, and repeated
sends to dead addresses can hurt sender reputation. A real system would
track how often the `ambiguous` rules fire, per provider, and tune them.

## Retry design

```
ceiling(n) = min(4h, 5m * 2^(n-1))          n = attempts so far
delay(n)   = 1m + rand[0,1) * (ceiling(n) - 1m)
give up    when the message is 72h old
```

- **Why exponential.** If a domain is down or throttling us, retrying at a
  fixed interval keeps hammering it. Doubling backs off quickly at first,
  then settles at the cap.
- **Why full jitter.** Messages that failed together (an outage, a burst of
  421s) would otherwise all come back together and recreate the burst.
  Drawing uniformly from the whole window spreads them out.
- **Why a floor.** Plain full jitter can draw a delay of a few seconds.
  Retrying seconds after a 451 greylist or a 421 almost certainly fails
  again and adds load on a server that just asked us to slow down.
- **Why a cap.** Without one, attempt 10 would wait about 42 hours. 4 hours
  keeps late retries frequent enough to deliver soon after a long outage
  ends.
- **Why max age instead of max attempts.** Senders care about how long mail
  can be late, not how many tries it took. If a jittered retry would land
  past the deadline, it is moved onto the deadline exactly, so every message
  gets one final attempt at 72h.

All of this is configurable (`MAILQ_RETRY_*`, `MAILQ_MAX_AGE`). The tests
drive a message's whole lifetime with a fake clock. With every draw at the
ceiling, a message gets 24 attempts before failing at exactly 72h.

An interrupted attempt (our own shutdown) is retried immediately, because
the remote server did nothing wrong.

## Throttling design

Each recipient domain gets a cap on concurrent deliveries (default 5) and
a token bucket in messages per minute (default start 60, range 1 to 600).
The bucket never holds more than 10 seconds' worth of the current rate, so
a domain that was slowed down cannot get a burst after it goes idle.

**AIMD.** A throttle signal halves the domain's rate. Each minute of clean
deliveries adds 10/min. This is the TCP congestion-control pattern: back
off fast when the receiver pushes back, and probe upward slowly.

- **Why 421 and 4.7.x slow a domain down.** 421 means the server is
  closing the channel, and providers use it for "too many connections or
  messages from you". The 4.7.x enhanced codes are security and policy
  deferrals, which providers use for rate limits and reputation deferrals.
  Both mean "you, specifically, are sending too much", and the only fix is
  to send less. Retrying each message on its own schedule doesn't fix that,
  because the next message in the queue hits the same limit.
- **Greylisting is not throttling.** A greylist deferral (often a 451
  4.7.1) is about an unknown sender/recipient pair, not load. The
  classifier checks greylist wording before it looks at the enhanced code,
  and greylisting does not change the rate. Bounces and timeouts are also
  neutral.
- **One decrease per cooldown (1 minute).** Five connections that hit the
  same 421 at once are one signal, not five. They should halve the rate
  once, not divide it by 32.
- **Additive in time, not per message.** Adding a step per successful
  message would grow the rate in proportion to itself, which is exponential.

**Never block on a domain.** Before each claim, the worker asks the
throttle which domains have no budget and excludes them in SQL. A throttled
domain's backlog then never ties up goroutines that could deliver to other
domains. If two goroutines race for a domain's last slot, the loser
releases the message without counting an attempt. The rejected alternative
was blocking in `Acquire` until a token frees up. One hot, throttled domain
would then park every worker goroutine (head-of-line blocking). A test
checks that a domain limited to 6/min does not delay another domain.

**State is in memory, per worker process.**
- *On restart*, every domain starts again at the initial rate, and the
  system forgets that a domain was throttling it a minute ago. That is why
  the initial rate is conservative (60/min) rather than the maximum (600).
  After a restart we may still hit a throttle once and then back off again.
- *With several worker processes*, each has its own budget, so a domain
  sees up to N times the configured rate. The default is one worker process
  with many goroutines. A multi-process deployment would need shared
  throttle state, for example a Postgres table updated atomically or Redis
  with a token-bucket script.
- Domains idle for an hour are forgotten, so the map doesn't grow forever.
  A forgotten domain that had been slowed down starts again at the initial
  rate, exactly as after a restart.

The default numbers are starting points, not measured provider limits (see
[Unverified assumptions](#unverified-assumptions)).

## Delivery guarantees

**At-least-once.** Once POST returns 202 or 200, the message is committed
and will reach a terminal state: delivered, bounced, or failed after 72h.
It will not be silently dropped. It may be delivered more than once.

Duplicates can occur in these windows. Each one exists because the worker
cannot know whether the remote server accepted the message:

1. **Worker crash after the body was sent** (kill -9, OOM, power loss).
   The lease expires, the reaper requeues the message, and it is sent again.
   [Crash demo](#crash-demo) shows this: the fake server ends up with two
   copies.
2. **Connection lost or timed out after the final `.`**, before the reply.
   The server may have accepted. We classify it as transient and retry.
   (`dropafter@` in the tests.)
3. **Shutdown grace expires during end-of-DATA.** The attempt is cut and
   retried. `TestShutdownGraceExpiryRequeuesAndCanDuplicate` pins this
   down.
4. **The database write fails after a 250.** The message stays `sending`
   until the reaper retries it.
5. **A worker stalls longer than the lease** (VM freeze, very long GC
   pause). The reaper hands the message to another worker while the
   original is still running. Fencing stops the late result from
   overwriting state, but both may have sent.

Duplicates cannot come from:

- duplicate POSTs (unique idempotency key);
- two workers claiming one row (SKIP LOCKED);
- a late result clobbering newer state (fencing on `attempt_count`);
- a clean shutdown, as long as the grace period covers in-flight attempts.

Exactly-once would need the receiving server to deduplicate. Retries keep
the same Message-ID, but I have not verified whether any provider uses it
to collapse duplicates, and mailq doesn't rely on it.

**Graceful shutdown.** On SIGINT/SIGTERM the worker stops claiming
immediately and gives in-flight attempts `MAILQ_SHUTDOWN_GRACE` (20s) to
finish. Then it cancels them. Each canceled attempt is recorded as
`interrupted` with the message due immediately. The process exits only
after every attempt has been recorded. Keep the grace below the
supervisor's kill timeout (Docker 10s, Kubernetes 30s by default).
Otherwise SIGKILL arrives first and the leases are left for the reaper.

## Observability

**Logs.** JSON via `log/slog`. Every attempt logs `message_id`, `domain`,
`mx_host`, `attempt`, `outcome`, `reason`, `smtp_code`, `enhanced_code`,
`stage`, `rule`, `state`, `duration_ms`, and `next_attempt_at` for
deferrals. Rate cuts log the domain's new rate.

**Debug endpoint.** `GET /messages/{id}` returns the message and every
attempt: worker, timestamps, MX host, reply code, enhanced code, full reply
text, stage, transport error, outcome and reason.

**Metrics.** API on `:8080/metrics`, each worker on `:9091/metrics`.

| Metric | Labels | Question it answers |
|---|---|---|
| `mailq_attempts_total` | domain, outcome, reason | What is happening to deliveries? |
| `mailq_messages_finished_total` | domain, state | How much mail ends delivered / bounced / failed? |
| `mailq_retries_scheduled_total` | domain, reason | Why are we retrying? |
| `mailq_attempt_duration_seconds` | outcome | Is a provider slow right now? (one SMTP session) |
| `mailq_time_to_final_seconds` | state | How long do senders wait? (enqueue to terminal) |
| `mailq_attempts_per_message` | state | How many tries does mail need? |
| `mailq_throttle_decreases_total` | domain | Who is pushing back on us? |
| `mailq_throttle_releases_total` | domain | How often do claims race the throttle? |
| `mailq_leases_reaped_total` | | Are workers dying mid-send? |
| `mailq_lease_lost_total` | | Did a "dead" worker turn out to be alive? |
| `mailq_attempts_in_flight` | | Current SMTP sessions in this process |
| `mailq_queue_depth` | state | Backlog, counted in Postgres at scrape time (API only) |
| `mailq_ingest_total` | result | created / duplicate / conflict / invalid / error |

**Two latency histograms**, because they answer different questions and
span very different ranges. Per-attempt buckets run from 10ms (a local 250)
to 15m (the end-of-DATA timeout plus margin). End-to-end buckets run from
1s (first-try delivery) to 4 days (past the 72h max age). The Prometheus
default buckets (5ms to 10s) would put almost every deferred message in
`+Inf`.

**Label cardinality.** Recipient domains are unbounded, and each label
value is a new time series held in memory by mailq and Prometheus. Only
domains on an allowlist get their own label value. The default list is
about 20 large consumer providers (`MAILQ_METRIC_DOMAINS` overrides it).
Everything else becomes `other`, which bounds each metric at about
(allowlist + 1) × outcomes × reasons series. The long tail is debugged from
logs and the attempts table, which have the exact domain. I rejected
dynamic "top N domains" because rankings change: a domain would hop
between its own series and `other`, which corrupts rates over time, and
old series stay in memory until restart anyway. Queue depth lives in the
API so it is reported once, not once per worker.

## Running the tests

```sh
go test -race ./...                 # unit tests; DB-backed tests skip themselves

docker compose up -d --wait         # full suite, including Postgres and integration tests
MAILQ_TEST_DATABASE_URL='postgres://mailq:mailq@localhost:55432/mailq?sslmode=disable' \
  go test -race -count=1 ./...
```

PowerShell: `$env:MAILQ_TEST_DATABASE_URL="postgres://mailq:mailq@localhost:55432/mailq?sslmode=disable"; go test -race -count=1 ./...`.
`-race` needs cgo, which on Windows means a C compiler such as MinGW gcc.

Each DB test runs in its own throwaway Postgres schema. Integration tests
run the real API, store, worker and SMTP client against the fake server on
loopback, with a fake clock. Retries and lease expiry happen when a test
advances the clock, not by sleeping.

| Where | What |
|---|---|
| `internal/classifier` | Table of about 50 replies (each scripted reply, provider-style texts, conflicts, transport errors); a sweep of codes 100 to 699 checks that no non-2xx is ever delivered |
| `internal/backoff` | Ceiling growth and cap, jitter bounds and spread, clamping at max age, whole schedules on a fake clock |
| `internal/throttle` | Bucket sizes and waits, concurrency cap, AIMD steps and cooldowns, sweep, 50 goroutines under `-race` |
| `internal/smtp` | Each reply surfaced with code, enhanced code and stage; hang timeout; drops; cancel; STARTTLS; MX fallback; DNS cases; CRLF, dot-stuffing, split replies |
| `internal/store` | Concurrent idempotent enqueue, SKIP LOCKED with 8 claimers, due times, reap, fencing, domain exclusion |
| `test/` | Every scripted behavior through the full pipeline, retry then deliver, give up at max age, concurrent duplicate POSTs sent once, two workers with no duplicates, crash reaping, throttling, metrics, both shutdown paths |

## Crash demo

`scripts/crash-demo.ps1` (Windows PowerShell; needs Postgres and
`go build -o bin/ ./cmd/...`):

1. starts fakesmtp, the API and a worker with a 45s lease;
2. sends a message to `slow@`. The fake reads the whole body, then waits 30s
   before replying;
3. kills the worker with `Stop-Process -Force` (TerminateProcess, the
   Windows equivalent of `kill -9`) while it waits for the reply;
4. starts a second worker. When the lease expires, the reaper requeues the
   message and the second worker delivers it.

Output from a run on my machine:

```
02:48:25 message 584359c6-... state=sending locked_by=vignesh-21228-a36605
02:48:25 worker1 killed (no graceful shutdown)
02:48:25 worker2 started; waiting for the lease to expire
...
02:49:11 state=deferred attempts=1
02:49:46 state=delivered attempts=2

attempt_number worker_id            stage    smtp_code outcome   reason
             1 vignesh-21228-a36605                  0 transient lease_expired
             2 vignesh-26120-8f63f0 data_end       250 delivered

fake SMTP server received 2 copies of the message
```

The two copies are the at-least-once guarantee working as described. The
dead worker had already handed over the body.

On Linux the same steps are: start the three binaries with the env vars
from the script, `kill -9 <worker pid>` while the message is `sending`,
and start another worker.

## Sending to a real mailbox

Many home and cloud networks block outbound port 25, so real delivery may
need a VPS whose provider allows it. On 2026-10-28, my home network did
**not** block it: a TCP connection to `gmail-smtp-in.l.google.com:25` got a
`220 mx.google.com ESMTP` banner, and the real send below reached Gmail's
MX and got a reply from it.

To try one real send to your own address, run the worker *without*
`MAILQ_STATIC_ROUTE`, set `MAILQ_HELO_NAME` to a name you control, and
POST a message to your Gmail or Outlook address. A residential IP with no
matching PTR record, SPF or DKIM is likely to be rejected or spam-foldered.
That is still a useful result: the reply codes and texts show how the
classifier handles a real provider.

Result (2026-10-28, sending to my own Gmail from a home IPv6 address, HELO
name = my machine's hostname, no PTR/SPF/DKIM):

- MX host: `gmail-smtp-in.l.google.com` (resolved via a real `LookupMX`)
- Reply: `550 5.7.1` at end of DATA
- Text: "The IP you're using to send mail is not authorized to send email
  directly to our servers. Please use the SMTP relay at your service
  provider instead." (`NotAuthorizedError`)
- Classifier: **permanent / policy_blocked**, rule "5xx"

Gmail accepted the TCP connection, the EHLO and the whole message, then
rejected it at the final `.` because an unauthenticated residential IP is
not allowed to deliver directly. So the SMTP pipeline worked end to end
against real MX on port 25; the bounce is about sender authorization, which
is exactly what DKIM, SPF and a proper relay (all out of scope here) exist
to satisfy. A nice bonus: a stale `example.net` message from an earlier
demo hit a real null MX (RFC 7505) and bounced as `bad_domain`, which
exercised that DNS path against live records too.

## Unverified assumptions

These are places where the code relies on beliefs about real-world SMTP or
provider behavior that I have not checked against live traffic or provider
documentation:

- **4.7.x means throttling.** Providers use 4.7.x for rate limits and
  reputation deferrals, but it can also be greylisting without greylist
  wording. Treating it as throttling may slow a domain down more than
  needed.
- **The 5xx "temporary wording" phrase list** ("try again later",
  "temporarily", "rate limit", "too many") is a guess at common phrasing.
- **X.2.2 mailbox full is temporary, even under a 5xx.** This is a policy
  choice, not something the RFC says.
- **A single NXDOMAIN answer is trusted as a permanent bounce.** A DNS
  outage that wrongly returns NXDOMAIN would bounce mail a retry would have
  delivered.
- **Throttle defaults** (5 connections, 60 to 600/min, halve on throttle,
  +10/min per minute) are not based on any provider's published limits.
- **A 1-minute retry floor is enough to avoid an immediate repeat
  greylist.** The common belief is that greylisting windows are a few
  minutes (Postgrey defaults to 5). I have not measured this.
- **Timeouts are neutral for throttling.** An overloaded server may well
  show up as timeouts, but treating them as throttle signals would also
  slow us down on ordinary network trouble.
- **A failed STARTTLS handshake moves to the next MX.** I believe some MTAs
  retry the same host in plaintext instead. Not verified.
- **Reusing the Message-ID on retries** may let some receivers collapse
  duplicates. Not relied on, not verified.
- **Provider-style reply texts in the classifier tests** (e.g. 4.7.650,
  5.7.26) are paraphrased from memory, not copied from live replies.
- **The metric domain allowlist** is based on general knowledge of large
  providers, not traffic data.

## What broke while I built this

<!-- Written by the author, not generated. -->

## Out of scope

Each of these is something a production MTA needs and mailq does not do.

- **DKIM signing.** Large receivers increasingly expect mail to be
  authenticated, and DMARC needs an aligned DKIM signature or an aligned SPF
  pass. A real MTA signs each message with a per-domain key, manages key
  rotation, and publishes selectors in DNS.
- **SPF/DMARC alignment.** The sending IPs must be listed in the sender
  domain's SPF record, and the From domain must align with the SPF or DKIM
  domain, or DMARC-enforcing receivers reject or quarantine the mail. This
  involves DNS and customer onboarding as much as code.
- **IP warmup.** A new IP with no history is treated with suspicion. Real
  senders ramp volume per IP and per provider over weeks. That is a
  schedule layered on top of throttling, which mailq doesn't have.
- **IP pools.** Separating transactional from bulk traffic, or customers
  with different reputations, onto different IPs keeps one sender's
  behavior from hurting another's. It needs per-pool source binding and
  per-pool, per-domain throttles.
- **Reputation scoring.** Real systems track bounce, complaint and deferral
  rates per sender and per destination, and act on them by slowing down,
  pausing, or blocking a customer. mailq only reacts to throttling, per
  domain, in memory.
- **Feedback loops.** Providers send spam complaints (ARF reports) back to
  registered senders. These must be processed into suppression lists, or
  you keep mailing people who marked you as spam.
- **Real mailbox-provider tuning.** Gmail, Microsoft and Yahoo each have
  their own limits, reply codes and expectations. Production MTAs keep
  per-provider rules (connection limits, messages per connection, specific
  codes to back off on), learned from experience. The classifier here is
  generic.
- **TLS policy enforcement (MTA-STS/DANE).** Opportunistic TLS stops
  passive eavesdropping but not an active attacker who strips STARTTLS.
  MTA-STS (a policy fetched over HTTPS) and DANE (TLSA records with DNSSEC)
  let a domain require verified TLS. A real MTA fetches, caches and enforces
  those policies, and reports with TLS-RPT.
- **Connection pooling.** mailq opens one connection per message. Real MTAs
  reuse a connection for several messages (RSET between them) and keep
  connections warm per MX. That saves the TCP and TLS handshake on every
  message, and providers expect it.

Also not built: bounce notifications (DSNs or webhooks) to the sender,
multiple recipients per message, API authentication, and retention or
cleanup of old rows and idempotency keys (keys are kept forever).

## Configuration

All configuration is through environment variables. Durations use Go
syntax (`30s`, `5m`, `72h`).

| Variable | Default | Used by |
|---|---|---|
| `MAILQ_DATABASE_URL` | local compose DB | both |
| `MAILQ_API_ADDR` | `127.0.0.1:8080` | api |
| `MAILQ_METRICS_ADDR` | `127.0.0.1:9091` | worker |
| `MAILQ_CONCURRENCY` | `16` | worker: delivery goroutines |
| `MAILQ_HELO_NAME` | `localhost` | worker: EHLO name, should match the PTR record |
| `MAILQ_SMTP_PORT` | `25` | worker |
| `MAILQ_STARTTLS` | `true` | worker |
| `MAILQ_MAX_MX_HOSTS` | `3` | worker |
| `MAILQ_STATIC_ROUTE` | | worker: dev only, send all mail to this host:port |
| `MAILQ_DIAL_TIMEOUT`, `MAILQ_COMMAND_TIMEOUT`, `MAILQ_DATA_END_TIMEOUT` | `30s`, `2m`, `10m` | worker |
| `MAILQ_ATTEMPT_TIMEOUT` | `15m` | worker: whole attempt, all MX hosts |
| `MAILQ_LEASE` | `20m` | worker: must exceed the attempt timeout |
| `MAILQ_REAP_INTERVAL` | `30s` | worker |
| `MAILQ_SHUTDOWN_GRACE` | `20s` | worker |
| `MAILQ_POLL_INTERVAL` | `500ms` | worker: sleep when nothing is due |
| `MAILQ_RETRY_MIN`, `MAILQ_RETRY_BASE`, `MAILQ_RETRY_MAX`, `MAILQ_MAX_AGE` | `1m`, `5m`, `4h`, `72h` | worker |
| `MAILQ_DOMAIN_MAX_CONNS` | `5` | worker |
| `MAILQ_DOMAIN_INITIAL_RATE`, `MAILQ_DOMAIN_MIN_RATE`, `MAILQ_DOMAIN_MAX_RATE` | `60`, `1`, `600` | worker: messages/minute |
| `MAILQ_METRIC_DOMAINS` | ~20 large providers | worker: comma-separated label allowlist |
