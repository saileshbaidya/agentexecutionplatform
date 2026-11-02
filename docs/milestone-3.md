# Milestone 3: shared budgets and paid-operation recovery

The platform now enforces one shared USD budget per run. All enabled paid calls
pass through `Budgets.call()` in `src/aep/budgets.py`. Milestone 3 uses a durable
fake provider: its charges are synthetic accounting values, with no real LLM,
network provider, or monetary charge. Default research workflows remain free.

## Start or upgrade

```powershell
docker compose up --build -d --scale worker=2
docker compose ps
```

Migration `003_shared_budgets.sql` adds budget accounts, operations, reservations,
the append-only usage ledger, and fake-provider receipts. Existing run budgets are
backfilled from their original submission, with zero historical charges: earlier
milestones made no paid calls. Existing history and artifacts are retained.

All new monetary inputs are nonnegative integer micro-USD, up to
9,000,000,000,000,000; 1 USD = 1,000,000 micro-USD. Fractional and floating-point
amounts are not accepted. Infrastructure bills are outside this budget.

## How a call runs

1. Validate the provider, input/output token caps, and per-call cost ceiling.
2. Persist a stable logical operation and reserve its upper cost bound atomically.
3. Commit DISPATCHED and its dispatch token before invoking the provider.
4. Accept durable provider usage evidence, settle actual cost, and release the
   unused reservation in one transaction.
5. If the outcome is uncertain, retain the reservation and park the attempt in
   RECONCILIATION. The scheduler resolves evidence and resumes execution without
   blindly repeating the call.

The admission invariant is:

```text
settled_micro_usd + reserved_micro_usd + requested_bound <= limit_micro_usd
```

Concurrent calls lock the same run and atomically update its account. A run with
100 units cannot hold two 60-unit reservations. If the first call actually costs
40, settlement frees 20 and makes 60 available for another reservation.

The logical key is `(run_id, task_id, call_key)`, independent of worker attempt.
Reusing it with the same canonical request and quote returns the same operation;
changed data conflicts. Settled success is cached. Known charged failures are also
cached and are not automatically called again. A deliberate new paid retry needs
a new logical key and fresh admission; the current fixture supports one call per
task. Real agents can use the internal gateway for multiple calls in milestone 4.

## Submit the example

```powershell
$headers = @{ 'X-API-Key' = 'local-development-only' }
$submission = @{
    schema_version = '1'
    idempotency_key = 'paid-' + [guid]::NewGuid().ToString('N')
    request = 'Exercise bounded synthetic paid calls'
    budget_micro_usd = 120
    timeout_seconds = 600
    plan = Get-Content examples/paid-research-plan.json -Raw | ConvertFrom-Json
} | ConvertTo-Json -Depth 20
$created = Invoke-RestMethod http://localhost:8000/runs -Method Post -Headers $headers -ContentType application/json -Body $submission
$runId = $created.run_id
```

Each research task has an optional `paid_call`:

```json
{
  "call_key": "primary",
  "provider": "fake-v1",
  "input": "abcdefghij",
  "max_input_tokens": 10,
  "max_output_tokens": 8,
  "output_tokens": 4,
  "max_cost_micro_usd": 60
}
```

The fake provider defines one UTF-8 byte as one input token. Its immutable price
version `fake-usd-v1` charges 2 micro-USD per input token and 5 per output token.
The example reserves `10*2 + 8*5 = 60` and actually charges `10*2 + 4*5 = 40` for
each research task. Two calls settle at 80, leaving 40 of the 120-unit budget.
These are artificial test rates, not prices for any real model.

Unsupported providers, oversized inputs, excessive output, and a price bound
above `max_cost_micro_usd` are rejected before admission. The adapter independently
checks the frozen quote and limits at acceptance. The gateway cannot enable an
unbounded provider by changing a task's JSON.

## Inspect accounting

All endpoints below require the configured `X-API-Key`:

```powershell
curl.exe "http://localhost:8000/runs/<run_id>/budget" -H "X-API-Key: local-development-only"
curl.exe "http://localhost:8000/runs/<run_id>/operations" -H "X-API-Key: local-development-only"
curl.exe "http://localhost:8000/runs/<run_id>/budget/ledger" -H "X-API-Key: local-development-only"
```

Replace `<run_id>` with the submission ID. Run status also includes the current
budget. Operation and ledger listings accept `after` and `limit`, returning
`next_after` for pagination. Quotes record the price version and bounds; operation
events carry the operation ID for correlation.

To ask the server to reconcile from trusted provider evidence:

```powershell
curl.exe -X POST "http://localhost:8000/runs/<run_id>/operations/<operation_id>/reconcile" `
  -H "X-API-Key: local-development-only"
```

202 means still pending; 200 means closed. This endpoint never accepts a caller's
assertion of usage. Settlement verifies the fake provider's durable receipt.

Owners can record an explicit billing correction through
`POST /runs/{run_id}/operations/{operation_id}/adjustments`:

```json
{
  "adjustment_key": "provider-credit-001",
  "delta_micro_usd": -5,
  "reason": "Verified provider credit"
}
```

This is a trusted operator action, not an agent tool. Corrections are append-only,
idempotent by adjustment key, cannot make the operation's total charge negative,
and preserve the original charge. Ledger UPDATE, DELETE, and TRUNCATE are blocked.
The local single-owner API key has operator authority; a separate role model is
required before multi-tenant use.

## Failure, cancellation, and reconciliation

- Crash before dispatch: keep the unused reservation for a replacement attempt,
  or release it if the task/run stops. No provider call was authorized yet.
- Crash after dispatch: absence of a response does not release funds or authorize
  redispatch. Look for authoritative provider evidence.
- Crash after provider acceptance or settlement: use the same logical key and
  recorded result, with no second charge.
- Unknown outcome: keep funds reserved while the stopped attempt waits. Task/run
  deadlines continue to apply, and parked tasks count toward run concurrency.
- Cancellation: stop new work and release undispatched reservations. Dispatched
  uncertain liabilities survive cancellation and are reconciled on terminal runs.
- Provider bound violation: record actual usage, freeze further paid admission,
  and fail active execution after draining. Never conceal an overrun by clamping
  the ledger. `breached` is sticky, including after a later credit.

The scheduler reconciles a bounded batch ordered by last check, including terminal
runs. A resumed attempt consumes the existing task retry allowance; allow enough
attempts for crash/reconciliation recovery. Exhaustion can fail execution while
accounting reconciliation continues independently.

Insufficient funds produce structured `BudgetDenied` details (`code`, requested,
and available amounts) for future agent adaptation. Today's deterministic worker
fails its task with `budget_exhausted` rather than waiting for a budget increase.
There is no budget top-up/resume API yet; submit a new run with a suitable budget.
Reconciliation, free handlers, and cleanup require no new paid admission.

The fake adapter supports `success`, `known_failure`, `timeout_after_accept`, and
`timeout_before_accept` modes. `reconcile_delay_seconds` delays receipt visibility
after accepted timeouts. Its durable receipt store and fenced acceptance protocol
can definitively close a never-accepted fake request. This is specific to the fake
adapter: missing receipts, HTTP timeouts, or a worker's death are not proof of zero
charge for a real provider. Real provider lookup must run outside DB transactions
and honor provider-specific idempotency, usage, and cancellation semantics.

## Test

```powershell
$env:TEST_DATABASE_URL = 'postgresql://aep:aep_local_only@127.0.0.1:5432/aep'
.\.venv\Scripts\python.exe -m unittest discover -s tests -v

$env:AEP_API_KEY = 'local-development-only'
.\.venv\Scripts\python.exe scripts/budget_smoke.py
```

Integration tests race reservations, provider acceptance, reconciliation, and
settlement; inject transaction faults; kill a process after provider acceptance;
exercise immutable corrections, per-call bounds, and uncertain liabilities after
cancellation. Each test uses an isolated schema.

The HTTP smoke test runs a paid synthetic workflow, recovers a lost response once,
reconciles an uncertain liability after cancellation, and proves insufficient
funds prevent dispatch. It makes no real paid calls. The existing free smoke test
continues to work.
