# Agent Execution Platform

A platform that separates agent reasoning from durable, policy-controlled execution.

Milestones 0–2 provide the design contracts and a PostgreSQL-backed execution
core with an API, concurrent deterministic workers, DAG scheduling, cancellation,
leases, retries, and crash recovery.
Model calls, external tools, and Azure deployment are later milestones.

## Run the platform

```powershell
Copy-Item .env.example .env
docker compose up --build -d --scale worker=2
```

See the [milestone 1 guide](docs/milestone-1.md) for task submission, report retrieval,
local Python setup, integration tests, and a worker-kill recovery demonstration.
See the [milestone 2 guide](docs/milestone-2.md) for custom DAGs, parallel workers,
concurrency limits, scheduler behavior, timeouts, and cancellation.

## Design

- [Architecture and repository boundaries](docs/architecture.md)
- [Execution state machines](docs/state-machines.md)
- [Reliability and security decisions](docs/decisions/0001-execution-foundation.md)
- [Threat model](docs/threat-model.md)
- [Acceptance scenarios](docs/acceptance.md)
- [Implementation roadmap](docs/roadmap.md)
- [Version 1 contracts](contracts/v1/platform.schema.json)
- [Example research plan](examples/research-plan.json)

## Validate the design contracts

Use Python 3.10 or newer:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Tests validate contracts and local artifact integrity. Set `TEST_DATABASE_URL` to
run the PostgreSQL integration and subprocess crash-recovery tests; see the guide.

## Repository layout

```text
contracts/v1/       Versioned wire contracts
docs/              Architecture, decisions, and acceptance criteria
examples/          Deterministic workflow fixtures
src/aep/           API, execution core, workers, storage, migrations, and contracts
tests/             Contract, storage, API, PostgreSQL, and crash-recovery checks
```

The planner and handlers are deterministic fixtures. Gateway and real agent
orchestration modules will be added in later milestones.
