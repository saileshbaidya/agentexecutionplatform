# Execution state machines

All transitions are durable compare-and-set operations with an event appended in
the same transaction. Terminal states are immutable. Runtime SQL and database
constraints will implement these rules in milestone 1.

## Run

| From | To | Condition |
| --- | --- | --- |
| SUBMITTED | PLANNING | Planner claims execution |
| PLANNING | RUNNING | Valid plan persisted atomically |
| SUBMITTED, PLANNING, RUNNING | CANCELLING | Authorized cancellation or deadline |
| PLANNING | FAILED | Invalid plan or exhausted planning attempts |
| RUNNING | SUCCEEDED | All accepted tasks succeeded and final artifact committed |
| RUNNING | FAILED | Required task failed; other tasks quiesced |
| CANCELLING | CANCELLED | No live execution leases; pending tasks cancelled |

Failure uses fail-fast scheduling: stop admitting new work, cancel pending work,
and request cancellation of active attempts. Keep the run RUNNING with a failure
cause while draining; mark FAILED once no active lease remains. Cancellation
similarly stops scheduling immediately. External actions may finish after lease
expiry; record late outcomes for reconciliation without changing terminal state.
Concurrent success/cancel requests are serialized by the run row: the first
committed terminal transition wins. A persisted cancellation prevents success.

## Task

| From | To | Condition |
| --- | --- | --- |
| BLOCKED | READY | All dependencies succeeded |
| READY | RUNNING | Worker claim creates attempt and lease |
| RUNNING | SUCCEEDED | Current unexpired lease commits output |
| RUNNING | RETRY_WAIT | Retryable failure or lease loss; attempts remain |
| RETRY_WAIT | READY | Persisted backoff deadline reached |
| RUNNING | FAILED | Permanent failure or attempts exhausted |
| BLOCKED, READY, RETRY_WAIT | CANCELLED | Run cancellation or fail-fast drain |
| RUNNING | CANCELLED | Cancellation acknowledged or lease expires during drain |

Root tasks start READY; other tasks start BLOCKED. No partial-success dependency
policy exists in v1. An ambiguous side effect moves the attempt to RECONCILIATION;
its task stays RUNNING but cannot be claimed again until reconciled. The scheduler
does not treat this as an expired ordinary attempt. Reconciliation resolves it to
success, safe retry, or failure; run cancellation can terminalize the task while
the operation's accounting remains open.

## Attempt and operation

Attempts: ACTIVE -> SUCCEEDED | FAILED | EXPIRED | CANCELLED | RECONCILIATION.
RECONCILIATION -> SUCCEEDED | FAILED | CANCELLED. A safe retry creates a new attempt.
Worker heartbeats extend only the current ACTIVE lease. Every new claim increments
the task fencing token. Completion checks token, owner, expiry, and run state.

Operations: RESERVED -> DISPATCHED -> SUCCEEDED | FAILED | UNKNOWN.
UNKNOWN -> SUCCEEDED | FAILED after reconciliation. Reservations are SETTLED after
known usage, or RELEASED only when no charge is established. UNKNOWN retains funds.
Approval denial prevents dispatch and releases any unused reservation.

Retries use capped exponential backoff with jitter and a persisted next-run time.
Validation, policy denial, and permanent provider errors are not transient retries.
Task timeout applies across attempts; lease duration is a shorter liveness window.
