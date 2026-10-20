from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.services.ingestion import IngestionService, IngestionUnavailable


def service_for(failure, enqueued=False):
    job = SimpleNamespace(id=uuid4(), delivery_id=failure.delivery_id, run_id=failure.run_id,
                          sqs_message_id="existing" if enqueued else None, failure_event=lambda: failure)
    repository = SimpleNamespace(
        create_or_get=AsyncMock(return_value=job), get=AsyncMock(return_value=job),
        reserve_enqueue=AsyncMock(return_value=uuid4()), mark_enqueued=AsyncMock(), record_enqueue_error=AsyncMock(),
    )
    queue = SimpleNamespace(publish=AsyncMock(return_value="new-message"))
    return IngestionService(repository, queue), job


async def test_duplicate_does_not_publish(failure):
    service, job = service_for(failure, enqueued=True)
    assert await service.ingest(failure) == {
        "status": "duplicate", "delivery_id": failure.delivery_id, "run_id": failure.run_id, "job_id": str(job.id),
    }
    service.queue.publish.assert_not_awaited()


async def test_publish_maps_only_persisted_job(failure):
    service, job = service_for(failure)
    changed = failure.model_copy(update={"head_sha": "changed-duplicate"})
    assert (await service.ingest(changed))["status"] == "accepted"
    envelope = service.queue.publish.call_args.args[0]
    assert envelope.job_id == job.id and envelope.failure == failure
    service.repository.mark_enqueued.assert_awaited_once()


async def test_inflight_enqueue_is_retryable_not_accepted(failure):
    service, _ = service_for(failure)
    service.repository.reserve_enqueue.return_value = None
    with pytest.raises(IngestionUnavailable):
        await service.ingest(failure)
    service.queue.publish.assert_not_awaited()


async def test_publish_error_is_recorded_without_exception_secrets(failure):
    service, _ = service_for(failure)
    service.queue.publish.side_effect = RuntimeError("SECRET")
    with pytest.raises(IngestionUnavailable):
        await service.ingest(failure)
    assert service.repository.record_enqueue_error.call_args.args[-1] == "sqs_publish_failed"


async def test_final_commit_failure_does_not_claim_sqs_failed(failure):
    service, _ = service_for(failure)
    service.repository.mark_enqueued.side_effect = RuntimeError("DB down")
    with pytest.raises(RuntimeError):
        await service.ingest(failure)
    service.queue.publish.assert_awaited_once()
    service.repository.record_enqueue_error.assert_not_awaited()
