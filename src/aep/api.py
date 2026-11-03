"""Single-owner API. Every execution/artifact endpoint requires the configured key."""

import json
import secrets
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response
from starlette.concurrency import run_in_threadpool
import psycopg

from .config import Settings
from .db import connect
from .execution import Conflict, Execution, NotFound
from .budgets import Budgets
from .registry import AGENTS
from .model_provider import ModelProvider


MAX_SUBMISSION_BYTES = 256 * 1024


def create_app(settings=None):
    settings = settings or Settings.from_env()
    engine = Execution(settings)
    budgets = Budgets(engine)
    app = FastAPI(title="Agent Execution Platform", version="0.1.0")

    def authenticated(x_api_key: str = Header(default="")):
        if not secrets.compare_digest(x_api_key.encode(), settings.api_key.encode()):
            raise HTTPException(status_code=401, detail="Invalid API key")

    @app.exception_handler(NotFound)
    async def not_found(request, exc):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(Conflict)
    async def conflict(request, exc):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(psycopg.Error)
    async def database_error(request, exc):
        return JSONResponse(status_code=503, content={"detail": "Database unavailable; retry with the same idempotency key"})

    @app.get("/health/live")
    def live():
        return {"status": "ok"}

    @app.get("/health/ready")
    def ready():
        with connect(settings.database_url) as conn:
            if not conn.execute("SELECT 1 FROM schema_migrations WHERE name='004_agent_integration.sql'").fetchone():
                raise HTTPException(status_code=503, detail="Migrations not applied")
        return {"status": "ready"}

    @app.get("/agents", dependencies=[Depends(authenticated)])
    def registry():
        return {"agents": [{"id": agent_id, **definition} for agent_id, definition in AGENTS.items()],
                "backends": {"recorded": True, "openai": settings.enable_openai}}

    async def read_json(request):
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > MAX_SUBMISSION_BYTES:
                raise HTTPException(status_code=413, detail="Submission too large")
        try:
            return json.loads(body)
        except (ValueError, UnicodeError) as exc:
            raise HTTPException(status_code=422, detail="Invalid JSON body") from exc

    @app.post("/runs", dependencies=[Depends(authenticated)])
    async def submit(request: Request):
        submission = await read_json(request)
        try:
            run_id, created = await run_in_threadpool(engine.submit, submission)
        except (ValueError, UnicodeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return JSONResponse(status_code=201 if created else 200,
                            content={"run_id": run_id, "created": created}, headers={"Location": f"/runs/{run_id}"})

    @app.get("/runs/{run_id}", dependencies=[Depends(authenticated)])
    def status(run_id: UUID):
        return jsonable_encoder(engine.status(run_id))

    @app.post("/runs/{run_id}/cancel", dependencies=[Depends(authenticated)])
    def cancel(run_id: UUID):
        outcome = engine.cancel(run_id)
        return JSONResponse(status_code=202 if outcome["state"] == "CANCELLING" else 200, content=outcome)

    @app.get("/runs/{run_id}/events", dependencies=[Depends(authenticated)])
    def events(run_id: UUID, after: int = Query(default=0, ge=0), limit: int = Query(default=100, ge=1, le=1000)):
        rows = engine.events(run_id, after, limit)
        return jsonable_encoder({"events": rows, "next_after": rows[-1]["sequence"] if rows else after})

    @app.get("/runs/{run_id}/budget", dependencies=[Depends(authenticated)])
    def budget(run_id: UUID):
        return budgets.summary(run_id)

    @app.get("/runs/{run_id}/operations", dependencies=[Depends(authenticated)])
    def operations(run_id: UUID, after: int = Query(default=0, ge=0), limit: int = Query(default=100, ge=1, le=1000)):
        rows = budgets.history(run_id, after=after, limit=limit)
        return jsonable_encoder({"operations": rows, "next_after": rows[-1]["sequence"] if rows else after})

    @app.get("/runs/{run_id}/budget/ledger", dependencies=[Depends(authenticated)])
    def ledger(run_id: UUID, after: int = Query(default=0, ge=0), limit: int = Query(default=100, ge=1, le=1000)):
        rows = budgets.history(run_id, kind="ledger", after=after, limit=limit)
        return jsonable_encoder({"entries": rows, "next_after": rows[-1]["sequence"] if rows else after})

    @app.post("/runs/{run_id}/operations/{operation_id}/reconcile", dependencies=[Depends(authenticated)])
    def reconcile(run_id: UUID, operation_id: UUID):
        operation = budgets.reconcile_operation(operation_id, run_id=run_id, owner="local")
        pending = operation["state"] in {"RESERVED", "DISPATCHED", "UNKNOWN"}
        return JSONResponse(status_code=202 if pending else 200, content=jsonable_encoder(operation))

    @app.post("/runs/{run_id}/operations/{operation_id}/adjustments", dependencies=[Depends(authenticated)])
    async def adjustment(run_id: UUID, operation_id: UUID, request: Request):
        body = await read_json(request)
        try:
            entry = await run_in_threadpool(budgets.adjust, run_id, operation_id, body)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return jsonable_encoder(entry)

    @app.post("/runs/{run_id}/operations/{operation_id}/response", dependencies=[Depends(authenticated)])
    async def attach_response(run_id: UUID, operation_id: UUID, request: Request):
        body = await read_json(request)
        if not isinstance(body, dict) or set(body) != {"response_id"}:
            raise HTTPException(status_code=422, detail="Expected only response_id")
        try:
            await run_in_threadpool(ModelProvider(engine).attach_response, run_id, operation_id, body["response_id"])
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        operation = await run_in_threadpool(budgets.reconcile_operation, operation_id, run_id, "local")
        return jsonable_encoder(operation)

    @app.get("/runs/{run_id}/artifacts/{artifact_id}", dependencies=[Depends(authenticated)])
    def artifact(run_id: UUID, artifact_id: UUID):
        try:
            metadata, content = engine.artifact(run_id, artifact_id)
        except (OSError, ValueError) as exc:
            raise HTTPException(status_code=503, detail="Artifact unavailable or corrupted") from exc
        return Response(content, media_type=metadata["media_type"], headers={"ETag": f'"{metadata["sha256"]}"'})

    return app
