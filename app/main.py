import json
import logging
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import ValidationError

from app.models import FAILURE_CONCLUSIONS, WorkflowRunPayload, normalize_failure

app = FastAPI(title="ci-repair")

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
    # PostgreSQL placeholder — implement in Class 3:
    # await failure_repository.create(failure)
    # AWS SQS placeholder — implement in Class 3:
    # await queue_publisher.publish(failure)
    return {"status": "accepted", "delivery_id": failure.delivery_id, "run_id": failure.run_id}
