# Acceptance scenarios

These are implementation gates. Milestones 0–4 are covered by automated tests;
security policy, systematic quality evaluation, and Azure scenarios remain future gates.
See [milestone 1](milestone-1.md), [milestone 2](milestone-2.md), and
[milestone 3](milestone-3.md), and [milestone 4](milestone-4.md) for scope and tests.

## Milestone 0 (implemented contract checks)

- Research workflow validates against v1 schemas and graph rules.
- Unknown dependencies, duplicate task IDs, self-dependencies, cycles, multiple
  sinks, and non-writer sinks are rejected.
- State machines, transaction boundaries, threat model, and budget semantics are
  reviewable without choosing a model provider.

## Durable execution and dependencies

- Submit the same request key twice: one run. Reuse the key with changed input:
  conflict, no second run.
- Kill a worker after claiming work: after lease expiry another worker claims a
  new attempt. The old token cannot commit.
- Crash after external success but before completion: reconcile by operation key;
  do not blindly repeat a non-idempotent side effect.
- Run two schedulers and multiple workers against one database: one live claim per
  task, independent researchers overlap, reviewer starts only after both commit.
- Exhaust one research task's retries: writer does not run; pending work is
  cancelled and run fails after active attempts drain.
- Restart the scheduler during backoff: retry timing and attempt count survive.
- Cancel during execution: no new work is admitted; late output cannot make the
  run succeed. Test concurrent completion and cancellation transactions.

## Budget and policy

- With limit 100 and two simultaneous reservations of 60, exactly one is admitted.
- Settle an operation twice: one charge. Release twice: no extra available funds.
- Provider timeout with unknown charge: funds remain reserved until reconciliation.
- Restart after reservation but before dispatch: recover without duplicate spend.
- Change tool arguments after approval: dispatch denied until new approval.
- Retrieve content asking to upload secrets: gateway denies unauthorized egress.

## Evidence, context, and evaluation

- Researchers disagree on a claim: review artifact links both claims and evidence;
  final report explicitly preserves unresolved uncertainty.
- Task context exceeds its limit: deterministic selection/summarization records
  omissions and provenance without deleting original evidence.
- Replay a recorded run: no model or external tool calls occur.
- Evaluate a new prompt/model version: create a separate run, measure completion,
  citation support, contradiction handling, latency, usage, and recovery outcomes.

## Secure tools (milestone 5 implemented)

- A forged worker capability list cannot override the stored task spec.
- A task capability and an independently enabled deployment capability are both
  required. Unknown tools, extra arguments, and host/network options are rejected.
- Human approval requires a separate credential and binds exactly to the reviewed
  action digest, owner/run/task/call key, image, policy, arguments, and expiry.
- Concurrent dispatch consumes one approval once. Replays cannot extend expiry.
- Pending approvals hold no lease. Denial, expiry, cancellation, and task deadlines
  prevent dispatch and wake or terminalize the waiting task.
- A gateway crash removes the prior container before marking UNKNOWN; no automatic
  rerun is permitted. Failed cleanup remains a durable obligation.
- Real-container checks confirm network isolation, read-only root, nonroot UID,
  dropped capabilities, absent credentials/mounts, and bounded compute/output.

## Context and evaluation (milestone 6 implemented)

- Context selection is deterministic and bounded, records omissions, preserves
  exact original source ranges, and never silently drops mandatory review claims.
- Retries reuse a saved context; altered inputs, prompts, snapshots, manifests,
  or forged output context markers are rejected before untracked spending/results.
- Authenticated provenance traces source hashes through model calls to artifacts.
- Offline replay validates saved receipts and outputs without provider/tool calls.
- Retention defaults to dry-run, rejects active/unsettled/evaluating work, returns
  410 for purged payloads, and resumes failed file cleanup without changing ledger history.
- Default workers cannot claim evaluation pool work. Public input cannot enable faults.
- Versioned datasets exercise lease loss, worker loss after receipt, corrupted
  contexts, cancellation, and budget denial. Identical keys resume without new spend.
- Matching datasets compare across runtime/model versions; incompatible datasets
  and incomplete batches are rejected. Saved metrics survive operational retention.

## Azure pilot

- Authenticated deployment runs the example, recovers after worker termination,
  rejects unauthorized access, and exports correlated traces with redacted data.
- Deployment documentation includes estimated charges, scale limits, resource
  cleanup, and infrastructure alerts separate from per-run budget enforcement.
