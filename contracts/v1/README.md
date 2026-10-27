# Version 1 contracts

`platform.schema.json` uses JSON Schema Draft 2020-12. Its root validates a plan.
Other messages reference `$defs/submission`, `$defs/taskResult`, or `$defs/review`.
Unknown fields are rejected; incompatible changes require a new contract version.
The schema is a wire contract, not a database migration.

`src/aep/contracts.py` adds cross-task graph validation that JSON Schema cannot
express. Both schema and semantic validation are mandatory at admission.

- IDs are scoped to their owner: task IDs within a plan, claim IDs within a run.
- Handler IDs resolve to a trusted versioned registry; plans cannot supply code.
- Capabilities are requested privileges, intersected with owner and deployment
  policy. Inclusion in a plan does not grant permission.
- `depends_on` controls readiness and exposes dependency output artifact references
  to context assembly. `input_artifacts` references already-persisted inputs.
- Every accepted DAG has exactly one sink, a report writer. Therefore all tasks
  lead to that writer. A review task must be a direct writer dependency and have
  all research tasks among its ancestors. V1 has one report writer and one review.
- `timeout_seconds` for a task covers its lifetime from first claim, including
  retries and backoff. The run timeout starts at submission and is authoritative.
- Retry jitter uses a uniform value from zero to the capped exponential delay;
  `max_attempts` includes the first attempt. Persist the chosen retry timestamp.
- Results are accepted only with the runtime lease/fencing credentials, supplied
  separately from the result body. The gateway, not a worker result, owns charges.
- Artifact references must resolve to authorized immutable content. A schema-valid
  hash does not establish existence, ownership, or trust.
- Semantic result validation must resolve claim references, require review output
  for the reviewer, and ensure writer citations refer to supplied evidence. These
  runtime checks are deferred to agent integration; schema validation alone does
  not establish factual correctness.

Runtime API responses, events, approval messages, and database models will be
specified with their implementing milestones. They are not frozen by this schema.
