# Milestone 4: model-backed agent integration

The platform now has a versioned agent registry, model adapter, bounded planner,
research agents, evidence reviewer, and report writer. Use the recorded
demonstration without credentials, or opt into real OpenAI calls over supplied
documents. All generations use the milestone 3 shared budget.

## Start and try the recorded demonstration

```powershell
docker compose up --build -d --scale worker=2
$env:AEP_API_KEY = 'local-development-only'
.\.venv\Scripts\python.exe scripts/agent_smoke.py
```

The script verifies completion, source ownership, exact quotes, the report
checksum, unresolved conflicts, and accounting. Expected backend: `recorded`;
five tasks and normally five operations. Charges are synthetic. The recorded
backend is restricted to the packaged demonstration request and two documents;
it uses hand-authored response fixtures rather than a live model.

Submit the same example with curl:

```powershell
curl.exe -i -X POST http://localhost:8000/runs `
  -H 'Content-Type: application/json' `
  -H 'X-API-Key: local-development-only' `
  --data-binary '@examples/agent-submission.json'
```

Change `idempotency_key` to create another run; repeating unchanged input returns
the existing run. Check status and retrieve the final artifact through the
existing run/artifact APIs. Source documents use the same authenticated artifact
route and include checksum ETags.

## Enable a real model

Set these values in your local `.env` or deployment environment. Keep keys outside
source control and use a model available to your API project that supports
Responses background mode and Structured Outputs:

```text
AEP_ENABLE_OPENAI=true
OPENAI_API_KEY=<your API key>
AEP_OPENAI_MODEL=<supported model snapshot ID>
AEP_MODEL_PRICE_VERSION=<your approved price-card version>
AEP_MODEL_INPUT_RATE=<integer micro-USD per million uncached input tokens>
AEP_MODEL_CACHED_INPUT_RATE=<integer micro-USD per million cached input tokens>
AEP_MODEL_OUTPUT_RATE=<integer micro-USD per million output tokens>
```

Use the [official model catalog](https://developers.openai.com/api/docs/models)
and your account's approved rates. No default model or live price is hard-coded.
Rates must include positive input/output amounts. A blank cached rate uses the
uncached input rate conservatively; an explicit zero means cached input is free.
Price configuration and model identity are snapshotted when a run is accepted,
so changing environment settings does not alter existing operations.

For unit conversion, a rate of `$R` per million tokens is `R * 1,000,000`
micro-USD per million tokens. Reserve at the uncached input rate and output cap;
settle using reported input, cached-input, and output usage. Integer division
rounds each call's total upward to a micro-USD. These are usage-based accounting
amounts under the configured card, not a replacement for provider invoices,
credits, or taxes. A rate/model mismatch can invalidate a cost guarantee; verify
the configured contract before live use.

Then rebuild/recreate services and explicitly choose the live smoke test:

```powershell
docker compose up --build -d --scale worker=2
$env:AEP_API_KEY = 'local-development-only'
.\.venv\Scripts\python.exe scripts/agent_smoke.py --live --timeout 600
```

`--live` uses real API calls bounded by the example submission's 1,000,000
micro-USD ($1) run budget and 100,000 micro-USD per-call ceiling. The default
command never chooses live generation. For your own work, supply a submission
JSON file with `--submission path/to/request.json`, or POST it directly to `/runs`.
The live backend accepts your request and documents; it is not limited to the
demonstration corpus. An ordinary API subscription/key is required; ChatGPT app
access is not configured by this implementation.

This backend uses the fixed `https://api.openai.com/v1` origin. An Azure OpenAI
adapter or alternate provider needs separate API/pricing/recovery validation;
deploying these containers on Azure does not change the model endpoint.

## Submission and workflow

`agent` is optional. Existing default and custom fake DAG submissions still work.
In agent mode, provide `documents` containing unique `document_id`, `title`, and
`text` fields. Documents are immutable inputs held in PostgreSQL and assigned
owned artifact IDs and SHA-256 hashes. The API permits up to 20 documents, each
up to 20,000 characters, subject to its overall 256 KiB body limit.

Optional agent settings:

| Field                     | Default      | Allowed                                |
| -----------------------   | --------     | ------------------------------------   |
| backend                   | required     | recorded or openai                     |
| research_branches         | 2            | 1–4; recorded demonstration requires 2 |
| max_input_tokens | 16,000 | 1,024–64,000 |
| max_output_tokens | 2,048 | 512–8,192    |
| max_call_cost_micro_usd   | 100,000      | Nonnegative supported integer amount   |

The admitted DAG is:

```text
                  planner
                 /       \
          research-1   research-2 ...
                 \       /
                   review
                     |
                   report
```

Topology and branch count are bounded at admission. The model planner chooses
objectives and source assignments and must cover all documents. It runs as a
leased utility task after admission, so planning itself has durable attempts,
budgets, cancellation, and recovery. Model output cannot introduce code, arbitrary
handlers, tools, or DAG revisions. The research branches can run concurrently.

`GET /agents` with `X-API-Key` lists the four registered versioned handlers and
whether OpenAI is enabled. Agent profiles are server-owned and cannot be supplied
through arbitrary custom tasks. Prompt versions, model profile, source snapshots,
exact requests, raw responses, and operation IDs are recorded for inspection.
Existing versions must be preserved when adding new prompts/agent protocols.

## Evidence and report checks

- Research emits statements with a supplied document ID and exact nonempty quote.
  The runtime verifies the quote against that owned document, assigns the claim
  ID, and records a `chars:start:end` character locator and source hash.
  These offsets are zero-based Unicode code points, with an exclusive end offset.
- The reviewer identifies unsupported claims and conflicts using known claim IDs.
  Resolved conflicts require an explanation; unresolved conflicts remain visible.
- The writer cites supported known claims and acknowledges every unresolved review
  conflict. The renderer appends evidence links and all review conflict notes,
  including uncertainty and excluded-claim diagnostics.
- Invalid structured or semantic output allows one budgeted repair, then fails.
  Refusals, provider failures, and budget denials do not initiate repair generation.

Quote checks prove provenance, not that a statement logically follows from its
quote or is true. Review is model-based and quality evaluation remains milestone 6.
Research uses supplied documents only; external web search and tool execution are
not enabled until the secure tool-gateway work.

## Model-call identity and crash recovery

Call keys are generated by the agent runtime, for example `research-v1.step-0`
and `research-v1.step-1` for its one optional repair. Together with run and task
IDs, these identify logical calls across worker attempts. Retry never generates a
fresh random key. A changed prompt, source snapshot, output schema, or model/card
under the same key conflicts rather than silently making another paid call.

At most `2 * (research_branches + 3)` generations can be requested, further bounded
by the shared budget. The input-token count endpoint checks the exact model input,
instructions, and output schema before reservation. Oversized context fails
explicitly; the runtime does not silently discard evidence. POST generation uses
an explicit output cap, no tools or conversation history, disabled truncation,
default service tier, and no automatic HTTP retries.

OpenAI responses run in background mode with `store=true`. Their response ID is
persisted immediately and subsequent recovery polls GET rather than recreating
the generation. Stored response retention and project data policies must permit
this workflow; consult the [official background-mode documentation](https://developers.openai.com/api/docs/guides/background).

If the create response is lost before its ID reaches PostgreSQL, the operation
stays UNKNOWN and its funds remain reserved. No automatic resend occurs. If the
response ID is available in provider logs, the trusted owner can associate it:

```powershell
@'
{"response_id":"resp_REPLACE_WITH_ACTUAL_ID"}
'@ | curl.exe -X POST "http://localhost:8000/runs/<run_id>/operations/<operation_id>/response" `
  -H 'Content-Type: application/json' `
  -H 'X-API-Key: local-development-only' --data-binary '@-'
```

The server fetches the response and requires matching operation ID, request hash,
and model metadata before accepting it. A missing/expired response or terminal
response without usage is not proof of zero charge. Such liabilities can remain
held pending authoritative recovery. Directly rejected invalid/auth requests are
recorded as not accepted. Cancellation never wipes an accepted/uncertain charge.

Queued/unknown calls park attempts; reconciliation and computation resumption
consume the task's bounded retry allowance. Accounting can finish on cancelled or
failed runs without reopening execution. Six attempts per agent stage allow for
ordinary polling/recovery and a repair, with a 600-second stage lifetime.

## Processes and tests

Compose now starts `api`, `worker` replicas, `scheduler`, `reconciler`, and
PostgreSQL. The reconciler performs provider polling separately so network delays
cannot block scheduler deadline maintenance. Standalone development defaults to
combined maintenance; set `AEP_INLINE_RECONCILIATION=false` and run
`python -m aep reconciler` separately to match Compose.

```powershell
$env:TEST_DATABASE_URL = 'postgresql://aep:aep_local_only@127.0.0.1:5432/aep'
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The suite includes the recorded pipeline, owned citations, planner coverage,
review/writer references, bounded repairs, background polling, lost-create ID
attachment, usage rounding, frozen price cards, cancellation during generation,
budget denial, and missing usage/404 liability retention. OpenAI HTTP is mocked;
these tests make no real model calls. Live smoke verification requires your
configured API key and explicit `--live` command.

The architecture decision is documented in [ADR 0002](decisions/0002-agent-integration.md).
