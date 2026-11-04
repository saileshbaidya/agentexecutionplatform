# Milestone 5: secure tools

The execution control plane now authorizes tool calls through a durable gateway.
Tools are disabled in the default deployment. The opt-in deployment runs each
action in a short-lived Linux Docker container. No model credentials, database
credentials, host paths, Docker socket, or artifact volume enter that container.

## Start the opt-in deployment

Docker Desktop must use Linux containers. Run from the repository in PowerShell:

```powershell
docker build -f sandbox/Dockerfile -t aep-tool-sandbox:local .
$env:AEP_TOOL_IMAGE = (docker image inspect aep-tool-sandbox:local --format '{{.Id}}').Trim()
$env:AEP_TOOL_CAPABILITIES = 'text.stats'
docker compose -f compose.yaml -f compose.tools.yaml up --build -d --scale worker=2
$env:AEP_API_KEY = 'local-development-only'
.\.venv\Scripts\python.exe scripts/tool_smoke.py
```

Use your configured API key if you changed the local default. Image IDs must be
`sha256:` followed by 64 lowercase hexadecimal characters. No tags, implicit image
pulls, or client-selected images are accepted. Rebuilding requires inspecting the
new ID and recreating the services. Pending actions under the old image/policy
are denied; submit a new run and review its new action.

The gateway is the only service with the Docker socket. It is a privileged,
trusted control-plane service: daemon access effectively grants host authority.
API, workers, scheduler, and sandbox containers do not receive the socket.
This remains a trusted-machine, single-owner development deployment. Linux
containers share a kernel; hostile multi-tenant execution requires a stronger
boundary such as dedicated machines or microVMs before deployment.

## Tools and capability policy

`GET /tools` with `X-API-Key` lists the server registry and enabled capabilities:

| Tool | Arguments | Approval | Result |
| --- | --- | --- | --- |
| `text.stats` | `{"text":"..."}` (20 KB UTF-8 maximum) | No | Character, word, line counts |
| `python.run` | `{"code":"...","stdin":"..."}` (12 KB code, 20 KB stdin) | Required | Bounded stdout and exit code |

The task must use handler `tool-v1`, kind `utility`, and explicitly declare the
tool capability. The deployment must independently enable that same capability.
Unknown tools, extra arguments, missing capabilities, and tool calls hidden on
other handlers are denied. The worker's claim is only a lease credential: the
gateway reads the authoritative task spec from PostgreSQL, rather than trusting
worker-supplied capabilities or arguments. Rejected runtime requests are audited
as `tool.denied`, without copying raw code or input into the event log.

Example task (insert into a valid DAG before the evidence reviewer):

```json
{
  "task_id": "calculation",
  "kind": "utility",
  "handler": "tool-v1",
  "instructions": "Compute the reviewed value in an isolated container.",
  "depends_on": [],
  "input_artifacts": [],
  "capabilities": ["python.run"],
  "timeout_seconds": 300,
  "context_token_limit": 1000,
  "retry": {"max_attempts": 3, "initial_backoff_seconds": 1, "max_backoff_seconds": 10},
  "tool_call": {
    "call_key": "calculation-v1",
    "tool": "python.run",
    "arguments": {"code": "print(6 * 7)", "stdin": ""}
  }
}
```

There is one immutable tool call per tool task in v1. Downstream task input
snapshots receive its `tool_output` result, including the action ID and digest.
Model-backed research agents still use their milestone 4 fixed document workflow;
they do not yet dynamically request tools. Future agent tool selection must enter
this gateway and cannot acquire rights through prompts or tool output.

No HTTP, shell-on-host, external publication, or privileged filesystem tools are
registered. There is no egress allowlist to bypass: every sandbox uses network
`none`. Python may perform computation and write disposable files in its own
`/tmp`, but receives no external action credentials or persistent mounts.

## Review and approve an exact action

To enable Python, configure a separate random approval secret and recreate:

```powershell
$env:AEP_TOOL_CAPABILITIES = 'text.stats,python.run'
$approvalBytes = New-Object byte[] 32
$rng = [Security.Cryptography.RandomNumberGenerator]::Create()
$rng.GetBytes($approvalBytes)
$rng.Dispose()
$env:AEP_TOOL_APPROVAL_KEY = [Convert]::ToBase64String($approvalBytes)
docker compose -f compose.yaml -f compose.tools.yaml up --build -d --scale worker=2
```

Keep this credential in the human/operator client. Only the API container receives
it; the Compose override never gives it to workers, the gateway, or tools. It
must differ from `AEP_API_KEY` and contain at least 24 characters. With no approval
secret configured, the approval endpoint denies all requests.

After submitting a valid DAG with the Python task, retrieve the actions:

```powershell
$runId = '<run UUID>'
$headers = @{ 'X-API-Key' = $env:AEP_API_KEY }
$review = Invoke-RestMethod "http://localhost:8000/runs/$runId/tool-actions" -Headers $headers
$review.actions | ConvertTo-Json -Depth 20
```

Read the complete `action`: code, stdin, tool name, owner, run/task/call key,
policy version, image ID, resource limits, and approval requirement. Do not grant
approval on the basis of the task description alone. Select the reviewed action's
ID and copy its `action_digest` into the approval body:

```powershell
$actionId = '<reviewed action UUID>'
$digest = '<reviewed action_digest>'
$approvalHeaders = @{ 'X-API-Key' = $env:AEP_API_KEY; 'X-Approval-Key' = $env:AEP_TOOL_APPROVAL_KEY }
$body = @{ action_digest = $digest; decision = 'approve'; ttl_seconds = 60 } | ConvertTo-Json
Invoke-RestMethod -Method Post "http://localhost:8000/runs/$runId/tool-actions/$actionId/approval" `
  -Headers $approvalHeaders -ContentType 'application/json' -Body $body
```

Use `decision = 'deny'` to reject. Decisions cannot be replaced. Replaying a
decision never refreshes its expiry or consumption. Approval lifetime is 1–300
seconds, bounded by action expiry (at most five minutes), task deadline, and run
deadline. Expired actions require a new run; there is no approval refresh shortcut.

The gateway checks the complete canonical action again immediately before
dispatch, including current deployment capabilities and image/policy. Approval
consumption and the `RUNNING` transition occur atomically under the run lock.
Concurrent dispatchers cannot consume the same approval twice. Changed arguments,
policy, image, task, call key, or run do not inherit approval.

## Isolation, recovery, and accounting

Each action receives: 0.5 CPU, 128 MiB RAM with no extra swap, 32 PIDs, read-only
root, unprivileged UID/GID 65534, all Linux capabilities dropped, no privilege
escalation, private IPC, no network, no mounts, and 16 MiB ephemeral `/tmp`.
Docker's default seccomp policy remains enabled. Wall time is at most ten seconds
while the gateway is healthy; Python's child invocation has an additional
eight-second timeout. Combined output is limited to 64 KiB, with Docker log
storage disabled. Output exceeding the bound fails the action.

Docker runtime details: [run restrictions](https://docs.docker.com/reference/cli/docker/container/run/),
[resource limits](https://docs.docker.com/engine/containers/resource_constraints/),
[network none](https://docs.docker.com/engine/network/drivers/none/).

Lifecycle: `PENDING` (human review) or `READY` → `RUNNING` → terminal result.
Workers create the action and park the task in `TOOL_WAIT`, closing the attempt
and releasing the concurrency slot. No worker lease or database transaction is
held during approval or container execution. The scheduler wakes the task when
the action completes. Resuming after a tool wait is a continuation, rather than a
failed-handler retry, so it can add an attempt beyond the retry limit.

Cancellation/deadlines prevent later dispatch and result publication. A healthy
gateway observes cancellation while running and forcibly removes the container.
Cleanup failures retain a durable `cleanup_required` flag and are retried. If the
gateway crashes after dispatch, it never reruns that action: after 30 seconds a
gateway removes the named container, marks the outcome `UNKNOWN`, and fails the
task on resumption. A stopped gateway cannot enforce wall time; CPU/memory/PID
limits remain, and cleanup resumes when the gateway restarts. Run cancellation
may finish before container cleanup; inspect the action's cleanup flag when
verifying physical teardown. Use the Compose restart policy and monitor gateway
availability. Container names derive solely from action UUIDs.

These tools have no external per-call bill or persistent external side effect.
Their receipts and events are durable, but they do not manufacture token charges
in the model usage ledger. GPU/CPU hosting and Docker overhead remain outside the
per-run monetary budget. A future billed or external side-effecting tool must
also implement atomic monetary reservation and provider-specific recovery before
being enabled.

## Verification

```powershell
$env:TEST_DATABASE_URL = 'postgresql://aep:aep_local_only@127.0.0.1:5432/aep'
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
$env:AEP_TEST_TOOL_IMAGE = $env:AEP_TOOL_IMAGE
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_tool_sandbox.py' -v
```

The regular tests verify denied capabilities, forged claims, exact-action binding,
owner scope, separate approval credentials, approval replay/expiry/consumption,
concurrent dispatch, parked leases, cancellation, deadlines, cached results, and
gateway crash recovery. Opt-in real-container tests verify effective Linux
identity, capabilities, network interfaces, read-only filesystem, absence of
control-plane secrets/mounts, cgroup limits, output/memory abuse, cancellation,
and timeout behavior. Tests use random PostgreSQL schemas and never truncate
application tables.
