# ADR 0001: Execution foundation

Status: accepted for the initial implementation; revisit before production use.

## Decision

Use a narrow PostgreSQL-backed executor in Python. The project is intended to
develop execution infrastructure as well as agent behavior. A single transactional
store makes state transitions, leases, dependency admission, and budget reservations
auditable without cross-service coordination in the first release.

Use at-least-once task attempts. Never promise exactly-once external side effects.
Lease fencing protects database commits, but cannot undo an external request.
Operations need stable idempotency keys, provider reconciliation, or human recovery.
An ambiguous non-idempotent operation is not automatically retried.

Store append-only execution events alongside current-state tables. This is an
audit journal, not a claim that full event-sourced rebuilding is implemented.

## Budget rules

Amounts use integer micro-USD (1 USD = 1,000,000 units), never floating point.
Admission atomically checks `settled + outstanding reservations + requested <= limit`.
Each logical operation has at most one active reservation. Retries that can incur
additional charges require additional accounting and admission. Each call sets
provider-enforced output limits and records the price version used for estimation.

A timeout with uncertain billing keeps its reservation until reconciled. Exhausted
funds block new paid operations and require an explicit budget increase or run
cancellation. Free cleanup and reconciliation may continue. A hard upper bound is
only possible when the provider offers a trustworthy per-call bound; otherwise the
system must reject strict-budget calls or explicitly use a soft-budget policy.
Version 1 uses strict mode. Infrastructure charges are separate from model/tool
budgets and are controlled by deployment limits and monitoring.

## Alternatives

- Azure Durable Task / Durable Functions: strong managed durability; requires
  replay-safe orchestration and integration with our policy/accounting controls.
- Temporal: mature workflow semantics; adds service operations and SDK constraints.
- In-memory queues: simple, but fail the crash-recovery requirement.

The custom executor requires failure-injection tests and careful transaction
design. Before expanding into timers, signals, complex graph mutation, or multiple
regions, compare this maintenance burden with adopting a mature runtime.

## Scope limits

Single region, one trusted deployment owner, fixed DAGs, all-success dependencies,
bounded tasks, no arbitrary user code, no compensation workflows, and no guarantee
of bit-for-bit model reproducibility. Multi-tenant isolation requires a separate
design before accepting untrusted users.
