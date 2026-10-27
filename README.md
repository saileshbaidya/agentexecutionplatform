# Agent Execution Platform

A platform that separates agent reasoning from durable, policy-controlled execution.

Milestone 0 defines contracts and failure semantics. No API, scheduler, model calls,
or deployment resources are implemented yet.

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
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Tests validate the example against JSON Schema and exercise graph validation.
Runtime behavior described in the acceptance scenarios remains to be implemented.

## Repository layout

```text
contracts/v1/       Versioned wire contracts
docs/              Architecture, decisions, and acceptance criteria
examples/          Deterministic workflow fixtures
src/aep/           Shared contract validation; later runtime modules live here
tests/             Contract checks; later integration and failure-injection tests
```

Planned runtime modules are `orchestration`, `execution`, `gateway`, and `storage`.
They will be added when implemented, rather than represented by empty services.
