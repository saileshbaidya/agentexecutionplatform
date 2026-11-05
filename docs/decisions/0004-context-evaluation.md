# ADR 0004: frozen evidence contexts and isolated evaluations

Status: accepted for milestone 6, single-owner development.

## Decision

Persist one context per agent task before model spending. Read authoritative
attempt inputs, validate source hashes, select whole findings and exact source
prefixes under a canonical byte budget, and preserve mandatory review/conflict
references. Keep selection omissions, source ranges/hashes, upstream artifact
references, request/profile/prompt hashes, and snapshot hashes in a manifest.
Retried tasks reuse the saved snapshot; they cannot silently adopt new prompts,
inputs, or selection logic. Keep the provider token preflight as a separate,
authoritative gate for full request size.

New profiles freeze `evidence-prefix-v1`. Legacy profiles continue their existing
protocol rather than changing an admitted logical call's identity. Future recipes
must use new versions and preserve support or fail explicitly on older profiles.
Semantic ranking, generated summaries, and long-term vector memory are deferred.

Replay verifies saved request/receipt/output relationships and report citations
without external calls. It establishes provenance/structural reproducibility, not
semantic truth. A runtime source hash and actual Python/dependency/model/rate
configuration accompany evaluations. Version datasets by both label and content
hash; pin the synthetic fixture hash. Compare only matching dataset content and
evaluator versions. Increment the evaluator version when metric definitions change.

Fault injection belongs only to the evaluation harness, never user-facing run
arguments or a production-wide switch. Use private server-owned execution pools
so default workers cannot claim injected work. Persist case/run identity, completed
metrics, and injection events. Use a session advisory lock to serialize the same
batch without holding a long database transaction. Resume identical keys/configs;
create new keys for new versions. Default to recorded and require explicit live opt-in.

Retention is an explicit operator action with a dry-run default. Tombstone eligible
terminal payloads atomically, retain original hashes and audit metadata, and persist
validated artifact-file deletion jobs for retry. Block deletion with unresolved
operations, tool cleanup, or incomplete evaluations. Serve 410 for expired payloads.
Do not rewrite immutable ledger history. New `usage-v2` charge evidence stores a
result hash and usage, rather than generated payload text.

## Consequences

Deterministic prefix selection can omit relevant evidence and must expose that
limitation. Bounded byte context does not replace token counting. Stored snapshots
and full receipts are sensitive and require ownership checks. Purged content cannot
be replayed even though saved evaluation metrics remain comparable.

Retention covers operational payloads and committed artifact files. Historical
ledger payloads, explicit financial adjustment reasons, evaluation datasets/reports,
provider-side stored responses, backups, and orphan files from uncommitted writes
need separate policies. It is not a complete personal-data erasure mechanism.
Initial evaluations use a small synthetic corpus and structural/label metrics;
representative task quality and broader failure coverage require more datasets.
