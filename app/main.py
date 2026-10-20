import json
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import ValidationError

from app.models import FAILURE_CONCLUSIONS, WorkflowRunPayload, normalize_failure
from app.config import get_settings
from app.db import make_engine, make_sessions
from app.adapters.aws import aws_client
from app.adapters.sqs import SQSQueue
from app.repositories.failure_jobs import FailureJobRepository
from app.services.ingestion import IngestionService, IngestionUnavailable


@asynccontextmanager
async def lifespan(app):
    settings = get_settings()
    engine, client = None, None
    app.state.ingestion = None
    try:
        if settings.database_url and settings.aws_region and settings.sqs_queue_url:
            engine = make_engine(settings)
            client = await asyncio.to_thread(aws_client, "sqs", settings)
            app.state.ingestion = IngestionService(
                FailureJobRepository(make_sessions(engine)), SQSQueue(client, settings.sqs_queue_url),
                settings.enqueue_lease_seconds,
            )
        yield
    finally:
        if engine:
            await engine.dispose()
        if client:
            await asyncio.to_thread(client.close)


app = FastAPI(title="ci-repair", lifespan=lifespan)


def get_ingestion(request: Request):
    if request.app.state.ingestion is None:
        raise IngestionUnavailable("Durable ingestion is not configured")
    return request.app.state.ingestion

logger = logging.getLogger("ci_repair.webhook")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False

RequiredHeader = Annotated[str, Header(min_length=1, pattern=r"\S")]


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhook")
async def webhook(
    request: Request,
    x_github_event: RequiredHeader,
    x_github_delivery: RequiredHeader,
):
    try:
        body = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Request body must be valid JSON") from None

    if x_github_event == "ping":
        return {"status": "ok", "event": "ping"}
    if x_github_event != "workflow_run":
        return {"status": "ignored", "reason": "unsupported_event"}

    try:
        payload = WorkflowRunPayload.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail=exc.errors(include_input=False, include_url=False, include_context=False),
        ) from None

    if payload.action != "completed":
        return {"status": "ignored", "reason": "action_not_completed"}
    if payload.workflow_run.conclusion not in FAILURE_CONCLUSIONS:
        return {"status": "ignored", "reason": "non_failure_conclusion"}

    failure = normalize_failure(payload, x_github_delivery)
    logger.info(json.dumps({"event": "workflow_failure_accepted", **failure.model_dump(mode="json")}))
    try:
        return await get_ingestion(request).ingest(failure)
    except Exception:
        # Exception text may contain credentials, SQL parameters, or signed URLs.
        raise HTTPException(status_code=503, detail="Durable ingestion unavailable; retry delivery") from None
