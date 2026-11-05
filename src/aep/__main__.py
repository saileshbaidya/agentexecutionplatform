import argparse
import logging
import signal
import threading
import json
from pathlib import Path
import sys
from uuid import uuid4


def write_report(value, output=None):
    from fastapi.encoders import jsonable_encoder
    text = json.dumps(jsonable_encoder(value), indent=2, sort_keys=True)
    if output:
        import os
        import tempfile
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as stream:
            stream.write(text + "\n")
            name = stream.name
        try:
            os.replace(name, path)
        finally:
            Path(name).unlink(missing_ok=True)
    print(text)

from .config import Settings


def main():
    parser = argparse.ArgumentParser(description="Agent Execution Platform durable core")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="Apply checksummed PostgreSQL migrations")
    api = commands.add_parser("api", help="Run the submission and status API")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    worker = commands.add_parser("worker", help="Run deterministic tasks and recover expired attempts")
    worker.add_argument("--once", action="store_true", help="Maintain runs and attempt at most one task")
    worker.add_argument("--worker-id", default=None)
    scheduler = commands.add_parser("scheduler", help="Plan runs, admit dependencies, and recover expired work")
    scheduler.add_argument("--once", action="store_true", help="Perform one maintenance pass")
    reconciler = commands.add_parser("reconciler", help="Poll provider evidence and settle accounting independently of scheduling")
    reconciler.add_argument("--once", action="store_true", help="Reconcile one bounded batch")
    gateway = commands.add_parser("tool-gateway", help="Dispatch approved tools into isolated Docker containers")
    gateway.add_argument("--once", action="store_true", help="Dispatch one bounded batch")
    evaluate = commands.add_parser("evaluate", help="Run a versioned dataset in an isolated worker pool")
    evaluate.add_argument("--dataset", required=True)
    evaluate.add_argument("--key", default=None, help="Stable key for idempotent resume")
    evaluate.add_argument("--label", required=True, help="Human label; actual runtime/model versions are also recorded")
    evaluate.add_argument("--live", action="store_true", help="Explicitly enable paid model calls for this evaluation")
    evaluate.add_argument("--timeout-seconds", type=int, default=120)
    evaluate.add_argument("--output")
    report = commands.add_parser("evaluation-report", help="Read a saved evaluation")
    report.add_argument("batch_id")
    report.add_argument("--output")
    compare = commands.add_parser("compare-evaluations", help="Compare matching datasets across runtime/model versions")
    compare.add_argument("baseline_id")
    compare.add_argument("candidate_id")
    compare.add_argument("--output")
    replay = commands.add_parser("replay", help="Validate saved model outputs and provenance with no external calls")
    replay.add_argument("run_id")
    retention = commands.add_parser("retention", help="Preview or apply terminal-run payload retention")
    retention.add_argument("--older-than-days", type=int, default=30)
    retention.add_argument("--apply", action="store_true", help="Remove eligible payloads and retry pending file cleanup")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    if args.command == "migrate":
        from .db import migrate
        migrate(settings.database_url)
        print("Migrations applied")
    elif args.command == "api":
        import uvicorn
        from .api import create_app
        uvicorn.run(create_app(settings), host=args.host, port=args.port)
    elif args.command in {"evaluate", "evaluation-report", "compare-evaluations"}:
        from .evaluation import Evaluations, load_dataset
        evaluator = Evaluations(settings)
        if args.command == "evaluate":
            batch_id = evaluator.start(load_dataset(args.dataset), args.key or "eval-" + uuid4().hex,
                                       args.label, args.live, args.timeout_seconds)
            print("Evaluation batch: " + batch_id, file=sys.stderr)
            result = evaluator.run(batch_id)
        elif args.command == "evaluation-report":
            result = evaluator.report(args.batch_id)
        else:
            result = evaluator.compare(args.baseline_id, args.candidate_id)
        write_report(result, args.output)
        if args.command == "evaluate" and not result["passed"]:
            raise SystemExit(1)
    elif args.command == "replay":
        from .context import Contexts
        from .execution import Execution
        write_report(Contexts(Execution(settings)).replay(args.run_id))
    elif args.command == "retention":
        from .retention import Retention
        from .execution import Execution
        write_report(Retention(Execution(settings)).run(args.older_than_days, args.apply))
    else:
        stop = threading.Event()
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, lambda *_: stop.set())
        if args.command == "scheduler":
            from .scheduler import run_scheduler
            run_scheduler(settings, stop=stop, once=args.once)
        elif args.command == "reconciler":
            from .reconciler import run_reconciler
            run_reconciler(settings, stop=stop, once=args.once)
        elif args.command == "tool-gateway":
            from .tool_gateway import run_gateway
            run_gateway(settings, stop=stop, once=args.once)
        else:
            from .worker import run_worker
            run_worker(settings, stop=stop, once=args.once, worker_id=args.worker_id)


if __name__ == "__main__":
    main()
