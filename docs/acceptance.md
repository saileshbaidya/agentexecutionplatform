# Acceptance scenarios

These are implementation gates, not claims of completed runtime tests.

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

## Azure pilot

- Authenticated deployment runs the example, recovers after worker termination,
  rejects unauthorized access, and exports correlated traces with redacted data.
- Deployment documentation includes estimated charges, scale limits, resource
  cleanup, and infrastructure alerts separate from per-run budget enforcement.
