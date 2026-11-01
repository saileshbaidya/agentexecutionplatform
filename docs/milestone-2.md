# Milestone 2: DAG scheduling and cancellation

Milestone 2 adds a dedicated scheduler, custom validated plans, persisted per-run
concurrency limits, cancellation, and durable input snapshots. Workers remain
separate processes, each executing one task at a time. No model or paid tool calls
are enabled.

## Start or upgrade the existing local deployment

```powershell
docker compose up --build -d --scale worker=2
docker compose ps
```

Compose runs migration `002_dag_scheduling.sql` before starting the new API,
scheduler, and workers. Existing run history and artifacts are retained. The
input snapshots on attempts created before this migration remain empty. The
dedicated scheduler continues planning runs, expiring attempts, checking task/run
deadlines, and admitting dependencies while workers execute handlers. Starting the
worker service also starts its scheduler dependency.

Local Python equivalents (with the environment from the milestone 1 guide):

```powershell
python -m aep migrate
python -m aep scheduler
```

In separate terminals run `python -m aep api` and two instances of
`python -m aep worker`. A standalone worker defaults to embedded maintenance;
`AEP_WORKER_MAINTENANCE=false` disables that behavior when a dedicated scheduler is
running. Compose sets it to false for workers. Multiple schedulers are supported.

## Submit a custom DAG

The existing `POST /runs` request now accepts two optional fields:

- `plan`: a v1 plan, such as `examples/research-plan.json`. If omitted, the original
  deterministic research workflow runs.
- `max_concurrency`: the maximum number of RUNNING tasks in this run. Omission uses
  `AEP_RUN_CONCURRENCY_LIMIT` (default 4); requests cannot exceed that deployment
  limit. The accepted limit is stored with the run and survives restarts. Changing
  the deployment setting affects newly submitted runs only. Valid limits are 1–32.

PowerShell example:

```powershell
$headers = @{ 'X-API-Key' = 'local-development-only' }
$submission = @{
    schema_version = '1'
    idempotency_key = 'dag-' + [guid]::NewGuid().ToString('N')
    request = 'Compare synthetic evidence'
    budget_micro_usd = 0
    timeout_seconds = 600
    max_concurrency = 2
    plan = Get-Content examples/research-plan.json -Raw | ConvertFrom-Json
} | ConvertTo-Json -Depth 20
$submitted = Invoke-RestMethod http://localhost:8000/runs -Method Post -Headers $headers -ContentType application/json -Body $submission
$runId = $submitted.run_id
Invoke-RestMethod ('http://localhost:8000/runs/' + $runId) -Headers $headers
```

Graph validation rejects duplicate task IDs, unknown dependencies, cycles,
self-dependencies, invalid retry settings, and invalid report/review topology.
Custom plans are validated before a run is created and again during transactional
admission. Reusing an idempotency key with a changed plan or limit returns 409.

Only registered fake handlers are accepted: `fake-research-v1`, `fake-review-v1`,
`fake-writer-v1`, and `fake-utility-v1`. Handler/kind mismatches, unsupported
capabilities, and external input artifacts are rejected. Utility tasks pass along
findings and allow deeper diamond graphs. Plans cannot submit executable code.

## Dependency, concurrency, and timeout behavior

Tasks become READY only after **all** dependencies have committed SUCCEEDED.
Independent branches can run on different workers; fan-in tasks wait for every
parent. Failed dependencies never unlock a child. Permanent failure or retry
exhaustion stops admission and drains the remaining run.

Each successful claim takes the run-row lock, checks its concurrency cap again,
and records an attempt with a new fencing token. Least-recently-claimed eligible
runs are preferred, and capped runs do not block other runs. This is best-effort
fairness, not a global scheduling SLA. Cluster capacity is bounded by the number
of worker processes; the cap is per run, not a global resource quota.

Each attempt stores `input_snapshot`: the task spec, original request, and output
references/results of all successful ancestors, deduplicated by task ID. Retries
see the same immutable dependency results. This is explicit execution context;
model token selection, summarization, and long-term memory remain later milestones.

A task's timeout spans its lifetime from first claim, including retries, READY
queue time after retry, and backoff. Before the first claim, the run deadline bounds
queue time. A task timeout is permanent and fails the run after other attempts
drain. The run deadline cancels the run. PostgreSQL time is authoritative. The
scheduler can enforce deadlines even when all workers are busy.

Lease expiration allows safe retry; expired or superseded workers cannot heartbeat,
commit output, or acknowledge a newer attempt. Automatic retries remain safe here
because handlers have no external side effects.

## Cancel a run

```powershell
curl.exe -i -X POST "http://localhost:8000/runs/<run_id>/cancel" `
  -H "X-API-Key: local-development-only"
```

Replace `<run_id>` with the submission response ID. No request body is required.

- 202: cancellation is persisted, active attempts are draining.
- 200: the run is already CANCELLED or cancellation completed immediately.
- 409: success/failure has already committed, or failure draining started first.
- 404: no run owned by the caller; 401: missing or invalid API key.

Repeated cancellation is idempotent. Pending, blocked, and retry-wait tasks are
cancelled immediately. Active workers discover cancellation through their
heartbeat, stop processing, and acknowledge it. A lost worker's attempt is cancelled
when its lease expires. Only after all active attempts finish or expire does the
run become CANCELLED. Status includes `cancel_requested_at`, `cancel_requested_by`,
`cancel_reason`, task dependencies, and attempt input snapshots.

Cancellation and completion serialize on the same run row. The first terminal
commit wins; an accepted cancellation prevents subsequent success. An earlier
failure cause is retained rather than changed by a later cancel request.

Cancellation and timeouts revoke permission to commit. The worker checks for stop
before/after a handler and during simulated work. They do not forcibly terminate
arbitrary blocking Python code or undo external actions. Real handlers will need
cooperative stop checks; isolated tool execution is a later milestone.

## Verify milestone 2

```powershell
$env:TEST_DATABASE_URL = 'postgresql://aep:aep_local_only@127.0.0.1:5432/aep'
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The integration suite covers two real worker processes overlapping research tasks,
fan-in ordering, deeper custom DAGs, simultaneous claimers under a cap, concurrent
schedulers, stale lease rejection, durable retry timing, task/run timeouts,
cancellation acknowledgements, dead workers, and completion/cancellation races.
Each test uses an isolated schema; existing application runs are untouched.

For an HTTP test with visible task overlap and active cancellation, temporarily
enable a two-second fake delay and start two workers:

```powershell
$env:AEP_FAKE_DELAY_SECONDS = '2'
docker compose up -d --scale worker=2
$env:AEP_API_KEY = 'local-development-only'
.\.venv\Scripts\python.exe scripts/smoke.py --check-parallelism --check-cancellation
```

The output should report SUCCEEDED, `parallelism_checked: true`, and
`cancellation_checked: true`. Restore normal execution afterward:

```powershell
$env:AEP_FAKE_DELAY_SECONDS = '0'
docker compose up -d --scale worker=2
```

Inspect activity using `docker compose logs -f scheduler worker`, or query
`runs`, `tasks`, `dependencies`, `attempts`, and `events` in PostgreSQL. Existing
health endpoints and the default smoke test remain available.
