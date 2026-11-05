# Milestone 6: context, provenance, retention, and evaluation

New agent runs now persist a deterministic, bounded context for each task before
any model call. Results identify their exact context snapshot and hashes. You can
inspect provenance, validate saved outputs offline, remove eligible operational
payloads, and compare versioned evaluation runs with controlled failure injection.

## Start and verify

```powershell
docker compose up --build -d --wait --scale worker=2
$env:AEP_API_KEY = 'local-development-only'
.\.venv\Scripts\python.exe scripts/context_smoke.py
```

Use your configured API key if changed. If you enabled tools in milestone 5, keep
the same `AEP_TOOL_IMAGE`, capabilities, and `compose.tools.yaml` override when
rebuilding. Restart all runtime services together so normal workers enforce the
new execution-pool filter. The smoke test uses recorded synthetic responses,
verifies five snapshot hashes and byte bounds, and replays all five agent outputs
with zero provider or tool calls. It does not test live model quality.

## Context assembly

Agent submission accepts an optional byte limit:

```json
"agent": {"backend": "recorded", "context_byte_limit": 48000}
```

Allowed range: 2,048–128,000 canonical UTF-8 JSON bytes; default 48,000. This is an
assembly/memory bound, not a tokenizer estimate. The existing model preflight
still checks the complete prompt, schema, context, and any repair payload against
the model token limit before reservation or generation.

The `evidence-prefix-v1` recipe reads the authoritative attempt input snapshot:

1. Validate original document checksums and apply the role's source assignments.
2. Retain the full request, task/plan fields, and necessary review information.
3. Select complete findings in claim-ID order. Writer conflict/unsupported claim
   references are mandatory; they are never silently dropped. Findings not seen
   by the reviewer cannot be included by the writer.
4. Reserve up to 4 KiB (at most a quarter of the limit) for source excerpts while
   choosing optional findings, then allocate remaining bytes across documents in
   document-ID order. Use exact source prefixes, never generated summaries.
5. Record selected/omitted claim IDs, excluded-by-assignment documents, exact
   original character ranges, omitted ranges, excerpt hashes, and source hashes.

If required fields or mandatory claims cannot fit, the task fails before model
spend. Prefix selection can omit relevant evidence; omissions are explicit and
original documents remain available until retention. This recipe is intentionally
simple and reproducible, rather than a claim of semantic retrieval quality.

One `context_snapshots` row per run/task stores the context, recipe, input digest,
context digest, manifest digest, originating attempt, upstream artifact references,
request hash, profile hash, and prompt/schema hash. A retry uses the same saved
snapshot. Changed inputs, changed prompts, or corrupted snapshots fail closed.
Completion rejects forged context identifiers/hashes. All evidence remains
untrusted data under the agent prompt boundary.

Existing pre-milestone-6 run profiles keep their original context behavior, avoiding
changed model-call identities during recovery. They do not gain retrospective
snapshots or offline replay support. New runs freeze the new recipe in their profile.

## Inspect and replay

All endpoints require `X-API-Key` and enforce run ownership:

```powershell
$runId = '<run UUID from the smoke output>'
$headers = @{ 'X-API-Key' = $env:AEP_API_KEY }
Invoke-RestMethod "http://localhost:8000/runs/$runId/contexts/planner" -Headers $headers
Invoke-RestMethod "http://localhost:8000/runs/$runId/provenance" -Headers $headers
Invoke-RestMethod -Method Post "http://localhost:8000/runs/$runId/replay" -Headers $headers
```

The provenance response connects request/profile hashes → selected sources and
upstream artifacts → context/prompt hashes → model operation hashes → output
artifacts and attempts. Contexts can contain supplied private data, so treat the
context endpoint as sensitive even though hashes/manifests are smaller.

Replay validates a successful new agent run using stored bytes. It checks original
source checksums, snapshots/manifests, every referenced model request, prompt/schema
identity, provider receipts, immutable accounting evidence, normalized outputs,
artifact hashes, and rendered report evidence. It does not call models or tools,
and it does not assert that quoted evidence proves a claim semantically true.
Corruption returns an integrity/conflict error. Purged content returns HTTP 410.

## Run the versioned evaluation dataset

Run evaluation inside the API container so it shares the same artifact volume as
the serving API. The API server remains available; the CLI starts a separate
process and an isolated worker pool:

```powershell
docker compose exec -T api python -m aep evaluate `
  --dataset /app/evaluations/evidence-v1.json `
  --key baseline-m6 --label baseline `
  --output /artifacts/evaluations/baseline-m6.json
```

The default is recorded, with synthetic accounting and no external inference.
The built-in dataset contains six cases: contradictory evidence, expired worker
lease, worker loss after a settled model receipt, context corruption, cancellation
after claim, and budget denial. All use the packaged, hand-authored demonstration
fixture; its checksum is pinned in the dataset. This is a small regression/recovery
dataset, not a representative model benchmark.

For an explicitly paid evaluation against your configured live model, add `--live`.
Existing model opt-in, rates, per-call ceilings, and shared run budgets still apply.
Literal expected statements are strict labels and may reject valid paraphrases;
define the expectations appropriately when creating a new dataset version.

The batch stores dataset/version/content hash, actual runtime source fingerprint,
Python/dependency versions, context recipe, model/pricing configuration, and the
human label. Labels do not change prompts or execution. To compare code, prompt,
context, or model versions, deploy/configure the candidate and create another key:

```powershell
docker compose exec -T api python -m aep evaluate `
  --dataset /app/evaluations/evidence-v1.json `
  --key candidate-m6 --label candidate `
  --output /artifacts/evaluations/candidate-m6.json

docker compose exec -T api python -m aep compare-evaluations `
  '<baseline batch UUID>' '<candidate batch UUID>' `
  --output /artifacts/evaluations/comparison.json
```

Copy the batch IDs printed in each evaluation report. Comparisons require the same
dataset content hash and evaluator version. Reports show per-case regressions and
deltas in cost, usage, duration, and attempts, together with the actual configurations.
Different hosts, package versions, or load make latency comparisons noisy.

Checks include expected terminal state/failure reason, minimum findings, literal
statement labels, unresolved conflict count, exact citation ranges/hashes, offline
replay, operation limits, completed injection, and settled budget. Metrics include
input/output usage, micro-USD cost, attempts, duration, and context hashes. These
are structural/evidence/recovery checks, not an LLM judge or proof of factual truth.

Batch state `SUCCEEDED` means all cases finished scoring; inspect `passed` for
whether expectations passed. The evaluate command exits nonzero on failed checks.

## Resume and failure injection

Repeating the same `--key` and configuration resumes saved runs/cases without
repeating completed cases or spending again. A session advisory lock prevents two
evaluators from running the same batch concurrently and releases on process exit.
Use the same runtime/dependency/model/rate configuration to resume; a changed
version requires a new key. The case timeout defaults to 120 seconds and is bounded
to 10–600 with `--timeout-seconds`; on timeout the runner requests cancellation.

Run IDs and completed metrics are persisted independently. Ordinary workers claim
only the `default` pool; each evaluation uses `eval-<batch UUID>` and a private worker
loop. Fault controls are fixed harness code, not public submission fields, arbitrary
scripts, model directives, or deployment-wide fault switches. Lease expiry and
injection events commit durably; saved model receipts are reused after worker loss.
The context-corruption case must be detected before any model operation is admitted.

Read saved results without rerunning:

```powershell
docker compose exec -T api python -m aep evaluation-report '<batch UUID>'
Invoke-RestMethod 'http://localhost:8000/evaluations/<batch UUID>' -Headers $headers
```

## Explicit retention

Retention is disabled until an operator invokes it. Start with the default dry run:

```powershell
docker compose exec -T api python -m aep retention --older-than-days 30
# Only after reviewing eligible_run_ids:
docker compose exec -T api python -m aep retention --older-than-days 30 --apply
```

Eligibility requires a terminal run, the requested age since its last update,
settled operations, completed tool cleanup, and no incomplete evaluation using it.
The CLI considers at most 100 candidates per pass. No active/unknown financial
liability is discarded. There is no public purge endpoint.

Applying retention atomically tombstones operational payloads in submissions,
attempt input snapshots, task specs/results, source content/titles, contexts,
operation requests/results, provider response bodies, and tool arguments/results.
Hashes/manifests, artifact metadata, events, prices/usage, approval decisions, and
immutable financial evidence remain. Artifact/context/replay reads then return 410.
Known committed artifact files are deleted outside the transaction using validated
UUID/digest paths under the configured artifact root. File failures leave a durable
PENDING cleanup job; the next `--apply` pass resumes it idempotently. Payloads are
already inaccessible through normal artifact APIs while cleanup is pending.

New `usage-v2` charge evidence retains usage and a result hash instead of generated
text, allowing new output payloads to expire without rewriting the ledger. Historical
ledger evidence can still contain generated output and remains immutable. Explicit
accounting adjustment reasons, versioned evaluation datasets/reports, backups,
orphan files from uncommitted artifact writes, and provider-side stored responses
are outside this retention command. It is operational payload cleanup, **not a
complete privacy-erasure or secure-disk-erasure mechanism**. Completed evaluation
metrics remain comparable after underlying run payloads expire; replay then cannot
recover erased inputs.

## Tests

```powershell
$env:TEST_DATABASE_URL = 'postgresql://aep:aep_local_only@127.0.0.1:5432/aep'
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_context.py -v
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_evaluation.py -v
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The integration tests use fresh random schemas, synthetic evidence, and no paid
model calls. Include the milestone 5 sandbox image environment variable when you
also want real-container isolation tests in the full suite.
