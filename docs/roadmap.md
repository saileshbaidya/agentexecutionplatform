# Implementation roadmap

0. **Design foundation:** contracts, graph validation, architecture decisions,
   state machines, threat model, and acceptance scenarios.
1. **Durable core:** PostgreSQL migrations, submission/status API, fake worker,
   attempt leases, crash recovery, artifacts, events, and integration tests.
2. **DAG execution:** concurrent workers, dependency scheduling, retry backoff,
   deadlines, cancellation, fail-fast draining, and race tests.
3. **Budget control:** operation admission, atomic reservations, ledger,
   reconciliation, bounded provider adapter contract, and concurrency tests.
4. **Agent integration:** bounded planner, agent registry, model adapter,
   evidence contracts, review and report generation, recorded fixtures.
5. **Secure tools:** gateway, capability enforcement, scoped approvals, isolated
   execution backend, and negative security tests.
6. **Context and evaluation:** bounded context snapshots, retention, provenance,
   replay, versioned evaluation datasets, and quality/cost comparisons.
7. **Azure pilot:** deployment architecture decision, infrastructure as code,
   authenticated access, secrets, tracing, recovery checks, and cleanup guide.

Milestones 1–3 use deterministic handlers. Begin structured telemetry in milestone
1 and explicit artifact context in milestone 2. No external side-effecting tool
is enabled before its gateway, accounting, and recovery semantics exist.

Deferred: dynamic DAG revisions, multi-region recovery, untrusted multi-tenancy,
long-term semantic memory, compensation workflows, and production availability SLOs.
