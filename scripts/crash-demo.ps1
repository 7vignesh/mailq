# Crash-safety demo: hard-kill a worker mid-send and watch the message get
# reclaimed. Stop-Process -Force is TerminateProcess on Windows, the
# equivalent of kill -9: no signal handler runs, nothing is cleaned up.
#
# Needs Postgres running (docker compose up -d --wait) and `make build` or
# `go build -o bin/ ./cmd/...`. Takes about 90 seconds.
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$bin = Join-Path $root 'bin'
$logs = Join-Path $env:TEMP 'mailq-demo'
New-Item -ItemType Directory -Force $logs | Out-Null
function Now { (Get-Date).ToString('HH\:mm\:ss') }
function Get-Msg($id) { Invoke-RestMethod "http://127.0.0.1:8080/messages/$id" }

# Short lease so the demo is quick. The lease must exceed the attempt timeout.
$env:MAILQ_STATIC_ROUTE = '127.0.0.1:2525'
$env:MAILQ_ATTEMPT_TIMEOUT = '40s'
$env:MAILQ_DATA_END_TIMEOUT = '35s'
$env:MAILQ_LEASE = '45s'
$env:MAILQ_REAP_INTERVAL = '5s'

$procs = @()
function Start-Bin($name, $log, $argList = @()) {
    $opts = @{ FilePath = (Join-Path $bin "$name.exe"); PassThru = $true; WindowStyle = 'Hidden'
               RedirectStandardOutput = (Join-Path $logs "$log.log"); RedirectStandardError = (Join-Path $logs "$log.err") }
    if ($argList.Count -gt 0) { $opts.ArgumentList = $argList } # PowerShell 5 rejects an empty list
    $p = Start-Process @opts
    $script:procs += $p
    $p
}

try {
    Start-Bin fakesmtp fake @('-addr', '127.0.0.1:2525', '-slow', '30s') | Out-Null
    Start-Bin api api | Out-Null
    $w1 = Start-Bin worker worker1
    Start-Sleep 2

    # slow@ makes the fake server read the whole message, then wait 30s
    # before answering 250: the worker is stuck at end-of-DATA.
    $body = @{ from = 'me@mailq.test'; to = 'slow@gmail.com'; subject = 'crash demo'; body = 'hi'
               idempotency_key = "crash-$([guid]::NewGuid())" } | ConvertTo-Json
    $id = (Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/messages -ContentType application/json -Body $body).id
    Start-Sleep 3
    $m = (Get-Msg $id).message
    "$(Now) message $id state=$($m.state) locked_by=$($m.locked_by)"

    Stop-Process -Id $w1.Id -Force
    "$(Now) worker1 killed (no graceful shutdown)"
    Start-Bin worker worker2 | Out-Null
    "$(Now) worker2 started; waiting for the lease to expire"

    do {
        Start-Sleep 5
        $v = Get-Msg $id
        "$(Now) state=$($v.message.state) attempts=$($v.attempts.Count)"
    } until ($v.message.state -eq 'delivered')

    $v.attempts | Format-Table attempt_number, worker_id, stage, smtp_code, outcome, reason -AutoSize
    $copies = @(Get-Content (Join-Path $logs 'fake.log') | Select-String '"msg":"received"').Count
    "fake SMTP server received $copies copies of the message"
} finally {
    $procs | Stop-Process -Force -ErrorAction SilentlyContinue
}
