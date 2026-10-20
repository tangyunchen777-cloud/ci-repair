import asyncio
import json
import os
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text, update

from app.adapters.aws import aws_client
from app.adapters.s3 import S3Artifacts, artifact_keys
from app.adapters.sqs import SQSQueue
from app.config import Settings
from app.db import make_engine, make_sessions
from app.db_models import RepairJob
from app.main import app
from app.models import QueueEnvelope, WorkflowRunPayload, normalize_failure
from app.repositories.failure_jobs import FailureJobRepository, LeaseLost
from app.services.ingestion import IngestionService, IngestionUnavailable
from worker.main import Worker

pytestmark = [pytest.mark.integration, pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="Run through Docker Compose")]
PAYLOAD = json.loads((Path(__file__).parents[1] / "fixtures/workflow_run_failure.json").read_text())
RAW_ZIP = b"PK\x03\x04unmodified-test-archive"


@pytest_asyncio.fixture
async def stack():
    settings = Settings()
    engine = make_engine(settings)
    sessions = make_sessions(engine)
    sqs, s3 = await asyncio.gather(
        asyncio.to_thread(aws_client, "sqs", settings), asyncio.to_thread(aws_client, "s3", settings),
    )
    suffix = uuid4().hex
    dlq = (await asyncio.to_thread(sqs.create_queue, QueueName=f"test-dlq-{suffix}"))["QueueUrl"]
    dlq_arn = (await asyncio.to_thread(sqs.get_queue_attributes, QueueUrl=dlq, AttributeNames=["QueueArn"]))["Attributes"]["QueueArn"]
    url = (await asyncio.to_thread(sqs.create_queue, QueueName=f"test-events-{suffix}", Attributes={
        "VisibilityTimeout": "300", "ReceiveMessageWaitTimeSeconds": "20",
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"}),
    }))["QueueUrl"]
    settings = settings.model_copy(update={"sqs_queue_url": url})
    repository = FailureJobRepository(sessions)
    queue = SQSQueue(sqs, url)
    stack = SimpleNamespace(settings=settings, repository=repository, sessions=sessions, engine=engine,
                            sqs=sqs, s3=s3, queue=queue, dlq=dlq,
                            artifacts=S3Artifacts(s3, settings.s3_bucket),
                            ingestion=IngestionService(repository, queue))
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE repair_jobs"))
    try:
        yield stack
    finally:
        await asyncio.to_thread(sqs.delete_queue, QueueUrl=url)
        await asyncio.to_thread(sqs.delete_queue, QueueUrl=dlq)
        await asyncio.gather(asyncio.to_thread(sqs.close), asyncio.to_thread(s3.close))
        await engine.dispose()


def api_client(stack, monkeypatch):
    monkeypatch.setattr("app.main.get_settings", lambda: stack.settings)
    return TestClient(app)


async def test_alembic_schema_and_bootstrap(stack):
    async with stack.engine.connect() as connection:
        assert (await connection.execute(text("SELECT version_num FROM alembic_version"))).scalar_one() == "0001_repair_jobs"
        columns = (await connection.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='repair_jobs'"))).scalars().all()
    assert set(RepairJob.__table__.columns.keys()) == set(columns)
    config = await asyncio.to_thread(stack.sqs.get_queue_attributes, QueueUrl=os.environ["SQS_QUEUE_URL"], AttributeNames=["All"])
    attrs = config["Attributes"]
    assert attrs["VisibilityTimeout"] == "300"
    assert attrs["ReceiveMessageWaitTimeSeconds"] == "20"
    assert json.loads(attrs["RedrivePolicy"])["maxReceiveCount"] in (3, "3")
    privacy = await asyncio.to_thread(stack.s3.get_public_access_block, Bucket=stack.settings.s3_bucket)
    assert all(privacy["PublicAccessBlockConfiguration"].values())


async def test_extended_api_contract_and_persist_enqueue(stack, monkeypatch):
    headers = {"X-GitHub-Event": "workflow_run", "X-GitHub-Delivery": "integration-delivery"}
    with api_client(stack, monkeypatch) as client:
        assert client.get("/health").json() == {"status": "ok"}
        assert client.post("/webhook", headers=headers | {"X-GitHub-Event": "ping"}, json={}).json() == {"status": "ok", "event": "ping"}
        assert client.post("/webhook", headers=headers | {"X-GitHub-Event": "push"}, json={}).json() == {"status": "ignored", "reason": "unsupported_event"}
        assert client.post("/webhook", headers=headers, content=b"{").status_code == 400
        assert client.post("/webhook", headers=headers, json={}).status_code == 422
        assert client.post("/webhook", json=PAYLOAD).status_code == 422
        ignored = {**PAYLOAD, "action": "in_progress"}
        assert client.post("/webhook", headers=headers, json=ignored).json()["reason"] == "action_not_completed"
        success = {**PAYLOAD, "workflow_run": PAYLOAD["workflow_run"] | {"conclusion": "success"}}
        assert client.post("/webhook", headers=headers, json=success).json()["reason"] == "non_failure_conclusion"
        result = client.post("/webhook", headers=headers, json=PAYLOAD)
        assert result.status_code == 200
        body = result.json()
        assert body == {"status": "accepted", "delivery_id": "integration-delivery", "run_id": 123, "job_id": body["job_id"]}
        duplicate = client.post("/webhook", headers=headers, json=PAYLOAD)
        assert duplicate.json() == body | {"status": "duplicate"}
    message = await stack.queue.receive()
    envelope = QueueEnvelope.model_validate_json(message["Body"])
    assert str(envelope.job_id) == body["job_id"]
    job = await stack.repository.get(envelope.job_id)
    assert job.status == "queued"
    assert job.sqs_message_id == message["MessageId"]
    assert job.enqueued_at is not None
    assert envelope.failure == job.failure_event()
    remaining = await asyncio.to_thread(stack.sqs.receive_message, QueueUrl=stack.queue.queue_url, WaitTimeSeconds=1)
    assert not remaining.get("Messages")


async def test_concurrent_duplicate_deliveries_have_one_row_and_publish(stack, failure):
    async def ingest():
        try:
            return await stack.ingestion.ingest(failure)
        except IngestionUnavailable:
            return None  # Publication lease busy: caller must retry, never an early ack.
    responses = await asyncio.gather(*(ingest() for _ in range(12)))
    async with stack.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(RepairJob)) == 1
    accepted = [body for body in responses if body and body["status"] == "accepted"]
    assert len(accepted) == 1
    assert (await stack.ingestion.ingest(failure))["status"] == "duplicate"
    first = await stack.queue.receive()
    assert first
    second = await asyncio.to_thread(stack.sqs.receive_message, QueueUrl=stack.queue.queue_url, WaitTimeSeconds=1)
    assert not second.get("Messages")


async def test_enqueue_failure_persisted_and_operational_retry(stack, failure):
    service = IngestionService(stack.repository, SimpleNamespace(publish=AsyncMock(side_effect=RuntimeError("token=secret"))))
    with pytest.raises(IngestionUnavailable):
        await service.ingest(failure)
    job = await stack.repository.create_or_get(failure)
    assert job.last_error == "sqs_publish_failed"
    assert job.sqs_message_id is None
    assert job.id in await stack.repository.unenqueued_ids()
    assert (await stack.ingestion.publish_job(job.id))["status"] == "accepted"
    assert (await stack.repository.get(job.id)).sqs_message_id


async def test_claim_retry_completion_and_fencing(stack, failure):
    job = await stack.repository.create_or_get(failure)
    claim = await stack.repository.claim(job.id, 240)
    assert claim.outcome == "claimed"
    assert (await stack.repository.claim(job.id, 240)).outcome == "busy"
    async with stack.sessions.begin() as session:
        await session.execute(update(RepairJob).where(RepairJob.id == job.id).values(lease_expires_at=func.now() - timedelta(seconds=1)))
    new_claim = await stack.repository.claim(job.id, 240)
    assert new_claim.token != claim.token
    with pytest.raises(LeaseLost):
        await stack.repository.mark_ready_for_agent(job.id, claim.token, "old", "old")
    await stack.repository.record_retry(job.id, new_claim.token, "github_download_failed")
    retry = await stack.repository.get(job.id)
    assert (retry.status, retry.retry_count, retry.last_error) == ("retrying", 1, "github_download_failed")
    claim = await stack.repository.claim(job.id, 240)
    await stack.repository.mark_ready_for_agent(job.id, claim.token, "s3://bucket/event", "s3://bucket/logs")
    ready = await stack.repository.get(job.id)
    assert ready.status == "ready_for_agent" and ready.completed_at
    assert ready.last_error is None
    assert (await stack.repository.claim(job.id, 240)).outcome == "already_complete"


async def test_worker_full_path_and_terminal_duplicate(stack, failure):
    result = await stack.ingestion.ingest(failure)
    message = await stack.queue.receive()
    github = SimpleNamespace(download=AsyncMock(return_value=RAW_ZIP))
    worker = Worker(stack.repository, stack.queue, github, stack.artifacts, stack.settings)
    await worker.process(message)
    envelope = QueueEnvelope.model_validate_json(message["Body"])
    job = await stack.repository.get(envelope.job_id)
    assert str(job.id) == result["job_id"]
    assert job.status == "ready_for_agent"
    keys = artifact_keys(failure)
    for key, content_type, expected in ((keys[0], "application/json", failure.model_dump_json().encode()), (keys[1], "application/zip", RAW_ZIP)):
        obj = await asyncio.to_thread(stack.s3.get_object, Bucket=stack.settings.s3_bucket, Key=key)
        assert obj["ContentType"] == content_type
        assert obj["ServerSideEncryption"] == "AES256"
        assert await asyncio.to_thread(obj["Body"].read) == expected
    assert job.s3_event_uri == f"s3://{stack.settings.s3_bucket}/{keys[0]}"
    assert job.s3_logs_uri == f"s3://{stack.settings.s3_bucket}/{keys[1]}"
    # Re-insert the envelope: simulates SQS at-least-once or the final-enqueue-commit gap.
    await stack.queue.publish(envelope)
    await worker.process(await stack.queue.receive())
    github.download.assert_awaited_once()
    remaining = await asyncio.to_thread(stack.sqs.receive_message, QueueUrl=stack.queue.queue_url, WaitTimeSeconds=1)
    assert not remaining.get("Messages")


@pytest.mark.parametrize("failure_stage", ["github_download", "s3_upload", "database_complete"])
async def test_worker_failure_records_retry_without_deleting(stack, failure, failure_stage):
    await stack.ingestion.ingest(failure)
    message = await stack.queue.receive()
    github = SimpleNamespace(download=AsyncMock(return_value=RAW_ZIP))
    artifacts = stack.artifacts
    if failure_stage == "github_download":
        github.download.side_effect = RuntimeError("Bearer SECRET")
    elif failure_stage == "s3_upload":
        artifacts = SimpleNamespace(store=AsyncMock(side_effect=RuntimeError("signed-url=SECRET")))
    else:
        stack.repository.mark_ready_for_agent = AsyncMock(side_effect=RuntimeError("DATABASE_SECRET"))
    await Worker(stack.repository, stack.queue, github, artifacts, stack.settings).process(message)
    job_id = QueueEnvelope.model_validate_json(message["Body"]).job_id
    job = await stack.repository.get(job_id)
    assert (job.status, job.retry_count, job.last_error) == ("retrying", 1, f"{failure_stage}_failed")
    await asyncio.to_thread(stack.sqs.change_message_visibility, QueueUrl=stack.queue.queue_url, ReceiptHandle=message["ReceiptHandle"], VisibilityTimeout=0)
    redelivery = await stack.queue.receive()
    assert redelivery["MessageId"] == message["MessageId"]


async def test_malformed_message_redrives_after_three_receives(stack):
    await asyncio.to_thread(stack.sqs.send_message, QueueUrl=stack.queue.queue_url, MessageBody='{"schema_version":99}')
    worker = Worker(stack.repository, stack.queue, None, stack.artifacts, stack.settings)
    receives = 0
    for _ in range(10):
        response = await asyncio.to_thread(stack.sqs.receive_message, QueueUrl=stack.queue.queue_url, WaitTimeSeconds=1, VisibilityTimeout=1)
        if response.get("Messages"):
            message = response["Messages"][0]
            receives += 1
            await worker.process(message)
            await asyncio.to_thread(stack.sqs.change_message_visibility, QueueUrl=stack.queue.queue_url, ReceiptHandle=message["ReceiptHandle"], VisibilityTimeout=0)
        dlq = await asyncio.to_thread(stack.sqs.receive_message, QueueUrl=stack.dlq, WaitTimeSeconds=1)
        if dlq.get("Messages"):
            assert receives == 3
            assert dlq["Messages"][0]["Body"] == '{"schema_version":99}'
            break
    else:
        pytest.fail("Malformed message did not reach DLQ")


def test_live_compose_api_health_and_ping():
    import httpx
    url = os.environ["TEST_API_URL"]
    assert httpx.get(f"{url}/health").json() == {"status": "ok"}
    response = httpx.post(f"{url}/webhook", headers={"X-GitHub-Event": "ping", "X-GitHub-Delivery": "compose-ping"}, json={})
    assert response.status_code == 200 and response.json() == {"status": "ok", "event": "ping"}
