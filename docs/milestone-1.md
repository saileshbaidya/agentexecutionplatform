# Milestone 1: durable execution core

## Implemented scope

- PostgreSQL migration with checksum verification and an advisory migration lock.
- Idempotent submissions, persisted plans, tasks, dependencies, attempts, artifacts,
  and ordered run events.
- Fixed, free planner and deterministic research/review/report handlers.
- Atomic task claims, independent lease heartbeats, increasing fencing tokens,
  bounded retry backoff, expired-attempt recovery, and transactional completion.
- Basic all-success dependency admission to demonstrate the existing sample DAG.
- Run/task deadlines and fail-fast draining when a task exhausts its attempts.
- Authenticated submission, status, event pagination, and artifact retrieval APIs.
- Local content-addressed artifacts published before metadata commits, verified on read.

The worker performs scheduler maintenance before each claim. A separate scheduler
service is unnecessary at this scale. A stopped API does not interrupt workers;
stopped workers leave state in PostgreSQL for replacements to recover.

## Start with Docker Compose

Docker Desktop must be running with Linux containers. From the repository root:

```powershell
Copy-Item .env.example .env
docker compose up --build -d
docker compose ps
docker compose logs -f worker
```

The API binds to localhost port 8000. PostgreSQL binds to localhost port 5432.
The example credentials are for local development. The deployment is single-owner,
not a multi-tenant authentication system. Choose a different `AEP_API_KEY` in `.env`
if needed; use the same key in requests. `/docs` exposes API endpoint documentation.

Submit and retrieve a report (the supplied request is recorded; the planner always
runs the synthetic fixture workflow):

```powershell
$headers = @{ 'X-API-Key' = 'local-development-only' }
$body = @{
    schema_version = '1'
    idempotency_key = 'demo-001'
    request = 'Compare the synthetic evidence and write a report'
    budget_micro_usd = 0
    timeout_seconds = 600
} | ConvertTo-Json
$submitted = Invoke-RestMethod http://localhost:8000/runs -Method Post -Headers $headers -ContentType 'application/json' -Body $body
$runUrl = 'http://localhost:8000/runs/' + $submitted.run_id
do {
    Start-Sleep -Seconds 1
    $run = Invoke-RestMethod $runUrl -Headers $headers
    $run.state
} while ($run.state -notin @('SUCCEEDED', 'FAILED', 'CANCELLED'))
if ($run.state -eq 'SUCCEEDED') {
    Invoke-RestMethod ($runUrl + '/artifacts/' + $run.final_artifact_id) -Headers $headers
}
Invoke-RestMethod ($runUrl + '/events?after=0&limit=100') -Headers $headers
```

POST returns 201 on creation, 200 for identical replay, and 409 for a key reused
with changed input. Omitted and empty `input_artifacts` are equivalent. Input
artifact ingestion is not implemented; nonempty input references are rejected.
Unknown request fields and invalid contracts return 422. Submission size is
limited to 256 KiB. No worker runs inside the HTTP request.

Events use a per-run sequence. Pass `next_after` as `after` to fetch the next page.
Status includes task states, attempt history, the accepted plan, and final artifact
ID. Artifact bodies are JSON; the final one includes `report_markdown` and review.
Database outages return 503; retry submissions with the same idempotency key.

A repeatable HTTP smoke test checks idempotency, completion, the report checksum,
contradiction handling, and the terminal event:

```powershell
$env:AEP_API_KEY = 'local-development-only'
python scripts/smoke.py
```

## Run Python processes locally

Compose reads `.env`; standalone Python processes use environment variables.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e . -r requirements-dev.txt
docker compose up -d postgres
$env:DATABASE_URL = 'postgresql://aep:aep_local_only@127.0.0.1:5432/aep'
$env:AEP_API_KEY = 'local-development-only'
$env:ARTIFACT_ROOT = './artifacts'
.\.venv\Scripts\python.exe -m aep migrate
.\.venv\Scripts\python.exe -m aep api
```

In another terminal with the same environment, run:

```powershell
.\.venv\Scripts\python.exe -m aep worker
```

Alternatively set `$env:PYTHONPATH = 'src'` when running from the repository without
an editable install. API and workers must use the same artifact root. Wheel
installations include migrations and the versioned contract schema.

## Verify against real PostgreSQL

```powershell
docker compose up -d postgres
$env:TEST_DATABASE_URL = 'postgresql://aep:aep_local_only@127.0.0.1:5432/aep'
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Integration tests create and drop only randomly named `aep_test_*` schemas. The
explicit IPv4 address avoids IPv6 `localhost` connection fallback delays on Windows.
The test database user needs schema creation permission. Without `TEST_DATABASE_URL`,
integration tests explicitly skip; that is not a complete verification run.

The crash test launches a real worker subprocess, waits for its durable claim,
kills it, waits for expiry, then launches replacement processes. It verifies that
the abandoned attempt expires, its replacement increments the attempt number,
and the workflow succeeds. Other tests reject stale completions, verify heartbeat
renewal, race submissions and claims, inject a failed completion transaction, and
check retry exhaustion, ownership, artifact integrity, and API behavior.

## Manually demonstrate crash recovery

Set `AEP_FAKE_DELAY_SECONDS=30` and `AEP_LEASE_SECONDS=5` in `.env`, then:

```powershell
docker compose up -d --force-recreate worker
```

Submit a new request key. Once logs show `attempt.claimed`, kill the worker:

```powershell
docker compose kill -s SIGKILL worker
```

Set `AEP_FAKE_DELAY_SECONDS=0` in `.env` and recreate it:

```powershell
docker compose up -d --force-recreate worker
```

After the abandoned lease expires and retry backoff elapses, the new worker
finishes the run. Events and status retain both attempts. Graceful termination
also leaves an interrupted attempt to expire rather than fabricating success.

## Durability and operational limits

All metadata mutations take a run-row lock before modifying child records. Claims
use `SKIP LOCKED` to avoid waiting behind another scheduler. Handler execution and
file publication happen outside the database transaction. An accepted completion
requires the current worker, attempt, fencing token, active lease, live run, and
unexpired deadlines; task completion, events, dependency admission, and final run
state commit together. PostgreSQL time determines leases and deadlines.

The fixed planner's PLANNING transition and plan admission share one transaction:
a process crash rolls back to SUBMITTED. This is safe because the planner has no
external calls. Model-based planning will require its own leased operations.

PostgreSQL and the artifact volume must both be backed up. A commit with an unknown
client-side outcome is left for durable recovery; workers do not assume failure.
Orphan files from failed/stale completions are invisible through the API. Automated
orphan collection is deferred; do not delete files while workers may publish them.

The scheduler scans active runs and opens short connections; this favors clarity
over high throughput. Fairness, resource concurrency limits, explicit user
cancellation, dependency policy expansion, and scheduler scaling belong to milestone
2. Budgets are recorded but not yet enforced: only free local handlers are enabled.
There are no model calls, tool side effects, approvals, operation ledger, sandboxed
code execution, or Azure deployment. Operation reconciliation remains a later
requirement before any external side-effecting tools are enabled.

Stop local services with `docker compose down`. Named volumes retain state; do not
remove them if you need the execution history and artifacts.
