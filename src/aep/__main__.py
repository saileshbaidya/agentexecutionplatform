import argparse
import logging
import signal
import threading

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
    else:
        from .worker import run_worker
        stop = threading.Event()
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, lambda *_: stop.set())
        run_worker(settings, stop=stop, once=args.once, worker_id=args.worker_id)


if __name__ == "__main__":
    main()
