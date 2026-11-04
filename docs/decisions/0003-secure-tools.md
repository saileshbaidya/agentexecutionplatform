# ADR 0003: durable isolated tool gateway

Status: accepted for milestone 5, single-owner local development.

## Decision

Use a separate database-backed gateway process as the only holder of Docker
daemon access. Workers hold execution leases and may only propose the tool call
already authorized in the stored task. The API controls human approvals using a
credential that is never injected into worker, gateway, or sandbox environments.

Start with `text.stats` and opt-in `python.run`. Both execute in immutable local
Linux container images with bounded compute/output, no egress, and no persistent
or host filesystem access. No billed or external side-effecting tool is registered.
There is no host subprocess fallback for running submitted code.

Bind human decisions to a canonical digest of owner/run/task/call key, validated
arguments, tool name, image ID, policy version, limits, and risk requirement.
Recheck the entire snapshot at dispatch; consume the decision atomically with
the action state transition. Replays cannot refresh decisions. Policy/runtime
changes that alter execution semantics must increment the policy version.

Park tasks without worker leases while awaiting approval or tool execution.
Actions survive control-plane restart. Completed results are cached and returned
on continuation. Never automatically redispatch a RUNNING action after a crash;
remove its named container before declaring UNKNOWN. Persist cleanup obligations
so even terminal actions can be physically cleaned up later.

## Consequences

Docker daemon access makes the gateway a highly trusted part of the control plane.
Containers are suitable for this trusted-machine pilot, not a sufficient isolation
claim for hostile multi-tenancy. A gateway outage can delay wall-time enforcement;
the runtime resource limits still apply, and restart performs cleanup. Monetary
model budgets remain separate from fixed-cost local compute infrastructure.

This milestone establishes the authorization boundary for future agent-selected
tools. It does not expand the milestone 4 fixed agent workflow to dynamic tool
selection or introduce browser/network/publication tools. External tools require
specific idempotency, accounting, credential, and egress policies before admission.
