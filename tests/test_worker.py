from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4
import asyncio

import pytest

from app.config import Settings
from app.models import QueueEnvelope
from worker.main import Worker


def setup_worker(failure, outcome="claimed"):
    envelope = QueueEnvelope(job_id=uuid4(), failure=failure)
    job = SimpleNamespace(failure_event=lambda: failure)
    repository = SimpleNamespace(
        claim=AsyncMock(return_value=SimpleNamespace(outcome=outcome, job=job, token=uuid4())),
        record_retry=AsyncMock(), mark_ready_for_agent=AsyncMock(),
    )
    queue = SimpleNamespace(delete=AsyncMock())
    github = SimpleNamespace(download=AsyncMock(return_value=b"raw"))
    artifacts = SimpleNamespace(store=AsyncMock(return_value=("s3://bucket/event", "s3://bucket/logs")))
    worker = Worker(repository, queue, github, artifacts, Settings())
    return worker, {"Body": envelope.model_dump_json(), "ReceiptHandle": "receipt", "MessageId": "message"}


@pytest.mark.parametrize("outcome,deleted", [("already_complete", True), ("busy", False)])
async def test_claim_boundaries(failure, outcome, deleted):
    worker, message = setup_worker(failure, outcome)
    await worker.process(message)
    assert worker.queue.delete.await_count == int(deleted)
    worker.github.download.assert_not_awaited()
    worker.artifacts.store.assert_not_awaited()


async def test_ack_only_after_completion_commit(failure):
    worker, message = setup_worker(failure)
    steps = []
    worker.artifacts.store.side_effect = lambda *args: (steps.append("uploads") or ("event", "logs"))
    worker.repository.mark_ready_for_agent.side_effect = lambda *args: steps.append("commit")
    worker.queue.delete.side_effect = lambda *args: steps.append("ack")
    await worker.process(message)
    assert steps == ["uploads", "commit", "ack"]


@pytest.mark.parametrize("target,stage", [("github", "github_download"), ("artifacts", "s3_upload"), ("repository", "database_complete")])
async def test_failures_are_sanitized_and_unacknowledged(failure, target, stage, caplog):
    worker, message = setup_worker(failure)
    operation = {"github": "download", "artifacts": "store", "repository": "mark_ready_for_agent"}[target]
    getattr(getattr(worker, target), operation).side_effect = RuntimeError("DO_NOT_LOG_TOKEN_OR_URL")
    await worker.process(message)
    worker.queue.delete.assert_not_awaited()
    assert worker.repository.record_retry.call_args.args[-1] == f"{stage}_failed"
    assert "DO_NOT_LOG_TOKEN_OR_URL" not in caplog.text


async def test_delete_failure_preserves_committed_ready_state(failure):
    worker, message = setup_worker(failure)
    worker.queue.delete.side_effect = RuntimeError("receipt expired")
    await worker.process(message)
    worker.repository.mark_ready_for_agent.assert_awaited_once()
    worker.repository.record_retry.assert_not_awaited()


async def test_database_outage_leaves_message_and_lease_to_expire(failure):
    worker, message = setup_worker(failure)
    worker.artifacts.store.side_effect = RuntimeError()
    worker.repository.record_retry.side_effect = RuntimeError()
    await worker.process(message)
    worker.queue.delete.assert_not_awaited()


async def test_shutdown_finishes_current_work_and_stops_receiving(failure):
    worker, message = setup_worker(failure)
    stop = asyncio.Event()
    worker.queue.receive = AsyncMock(return_value=message)
    worker.github.download.side_effect = lambda *args: (stop.set() or b"raw")
    await worker.run(stop)
    worker.queue.receive.assert_awaited_once()
    worker.queue.delete.assert_awaited_once()
