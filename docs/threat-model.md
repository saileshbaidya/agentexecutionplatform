# Initial threat model

Trust boundary: model outputs, retrieved content, tool responses, and agent plans
are untrusted data. Only the execution control plane authorizes actions.

| Threat | Required control | Verification |
| --- | --- | --- |
| Prompt injection requests extra privileges | Gateway checks explicit task capabilities; content cannot grant rights | Injected instruction is denied and audited |
| Secret or evidence exfiltration | Deny-by-default egress, destination allowlists, scoped credentials, redaction | Disallowed destination and redirect blocked |
| Duplicate destructive tool request | Stable operation key, request digest, provider idempotency or reconciliation | Retry cannot silently repeat a known side effect |
| Approval reused for another action | Bind approval to owner, run, operation, canonical action digest, expiry; consume atomically | Changed arguments require new approval |
| Malicious code accesses host | Arbitrary code disabled initially; later isolated short-lived execution with CPU/memory/time/filesystem/egress limits | Escape and resource-abuse tests before enabling code |
| Worker returns stale result | Lease fencing and run-state checks | Expired worker commit rejected |
| Concurrent agents overspend | Transactional reservation and bounded calls | Parallel admission never exceeds limit |
| Sensitive traces persist indefinitely | Redacted logs, artifact access checks, retention/deletion policy | Secrets absent from exported telemetry |
| Cross-user artifact access | Authenticated ownership checks on all run/artifact APIs | Foreign artifact references rejected |

Local development assumes a trusted machine and single owner. Before any publicly
reachable Azure deployment, require authenticated APIs, managed identity where
supported, private storage, TLS, secret management, and least-privilege roles.
Containers alone are not a sufficient boundary for hostile multi-tenant code.

Tool specifications declare allowed argument schema, risk class, permissions,
timeout, cost bound, and idempotency behavior. Sensitive actions require explicit
human approval of the exact action. Approval waits hold no worker lease or database
transaction. Expiry or cancellation prevents later dispatch. V1 initially exposes
only deterministic local read-only tools; the gateway milestone adds external tools.
