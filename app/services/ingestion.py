from app.models import FailureEvent, QueueEnvelope


class IngestionUnavailable(RuntimeError):
    pass


class IngestionService:
    def __init__(self, repository, queue, enqueue_lease_seconds: int = 120):
        self.repository, self.queue = repository, queue
        self.enqueue_lease_seconds = enqueue_lease_seconds

    async def ingest(self, failure: FailureEvent) -> dict:
        job = await self.repository.create_or_get(failure)
        return await self.publish_job(job.id)

    async def publish_job(self, job_id) -> dict:
        job = await self.repository.get(job_id)
        if job.sqs_message_id:
            return self._response(job, "duplicate")
        token = await self.repository.reserve_enqueue(job.id, self.enqueue_lease_seconds)
        if token is None:
            job = await self.repository.get(job.id)
            if job.sqs_message_id:
                return self._response(job, "duplicate")
            raise IngestionUnavailable("Publication already in progress; retry later")
        # Always publish persisted data, never a possibly changed duplicate payload.
        envelope = QueueEnvelope(job_id=job.id, failure=job.failure_event())
        try:
            message_id = await self.queue.publish(envelope)
        except Exception:
            await self.repository.record_enqueue_error(job.id, token, "sqs_publish_failed")
            raise IngestionUnavailable("Queue publication failed") from None
        # Intentionally outside the publish exception handler: if this commit fails,
        # SQS may already own the message. Keep the lease until expiry; retry is safe
        # through worker idempotency, but can publish a second message (no outbox).
        await self.repository.mark_enqueued(job.id, token, message_id)
        return self._response(job, "accepted")

    @staticmethod
    def _response(job, status: str):
        return {"status": status, "delivery_id": job.delivery_id, "run_id": job.run_id, "job_id": str(job.id)}
