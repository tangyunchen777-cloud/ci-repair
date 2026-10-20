from dataclasses import dataclass
from datetime import timedelta
from typing import Literal
from uuid import UUID, uuid4

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from app.db_models import RepairJob
from app.models import FailureEvent


class LeaseLost(RuntimeError):
    pass


@dataclass(frozen=True)
class ClaimResult:
    outcome: Literal["claimed", "already_complete", "busy"]
    job: RepairJob
    token: UUID | None = None


class FailureJobRepository:
    def __init__(self, sessions):
        self.sessions = sessions

    async def create_or_get(self, failure: FailureEvent) -> RepairJob:
        values = failure.model_dump(mode="json")
        values["event_timestamp"] = failure.event_timestamp
        async with self.sessions.begin() as session:
            await session.execute(insert(RepairJob).values(id=uuid4(), **values)
                                  .on_conflict_do_nothing(index_elements=[RepairJob.delivery_id]))
            # READ COMMITTED sees the winning concurrent insert after ON CONFLICT waits.
            job = await session.scalar(select(RepairJob).where(RepairJob.delivery_id == failure.delivery_id))
        return job

    async def get(self, job_id: UUID) -> RepairJob:
        async with self.sessions() as session:
            job = await session.get(RepairJob, job_id)
            if job is None:
                raise LookupError("Job does not exist")
            return job

    async def reserve_enqueue(self, job_id: UUID, seconds: int) -> UUID | None:
        async with self.sessions.begin() as session:
            job = await session.scalar(select(RepairJob).where(RepairJob.id == job_id).with_for_update())
            now = await session.scalar(select(func.now()))
            if job.sqs_message_id or (job.enqueue_lease_until and job.enqueue_lease_until > now):
                return None
            token = uuid4()
            job.enqueue_token = token
            job.enqueue_lease_until = now + timedelta(seconds=seconds)
            job.updated_at = now
        return token

    async def mark_enqueued(self, job_id: UUID, token: UUID, message_id: str):
        async with self.sessions.begin() as session:
            result = await session.execute(update(RepairJob).where(
                RepairJob.id == job_id, RepairJob.enqueue_token == token,
            ).values(sqs_message_id=message_id, enqueued_at=func.now(), updated_at=func.now(),
                     enqueue_token=None, enqueue_lease_until=None))
            if result.rowcount != 1:
                raise LeaseLost()

    async def record_enqueue_error(self, job_id: UUID, token: UUID, error: str):
        async with self.sessions.begin() as session:
            await session.execute(update(RepairJob).where(
                RepairJob.id == job_id, RepairJob.enqueue_token == token,
            ).values(last_error=error, updated_at=func.now(), enqueue_token=None, enqueue_lease_until=None))

    async def claim(self, job_id: UUID, seconds: int) -> ClaimResult:
        async with self.sessions.begin() as session:
            job = await session.scalar(select(RepairJob).where(RepairJob.id == job_id).with_for_update())
            if job is None:
                raise LookupError("Job does not exist")
            if job.status in {"ready_for_agent", "failed"}:
                return ClaimResult("already_complete", job)
            now = await session.scalar(select(func.now()))
            if job.status == "processing" and job.lease_expires_at and job.lease_expires_at > now:
                return ClaimResult("busy", job)
            token = uuid4()
            job.status = "processing"
            job.lease_token = token
            job.lease_expires_at = now + timedelta(seconds=seconds)
            job.started_at = now
            job.updated_at = now
        return ClaimResult("claimed", job, token)

    async def record_retry(self, job_id: UUID, token: UUID, error: str):
        async with self.sessions.begin() as session:
            result = await session.execute(update(RepairJob).where(
                RepairJob.id == job_id, RepairJob.status == "processing", RepairJob.lease_token == token,
            ).values(status="retrying", retry_count=RepairJob.retry_count + 1, last_error=error,
                     updated_at=func.now(), lease_token=None, lease_expires_at=None))
            if result.rowcount != 1:
                raise LeaseLost()

    async def mark_ready_for_agent(self, job_id: UUID, token: UUID, event_uri: str, logs_uri: str):
        async with self.sessions.begin() as session:
            result = await session.execute(update(RepairJob).where(
                RepairJob.id == job_id, RepairJob.status == "processing", RepairJob.lease_token == token,
                RepairJob.lease_expires_at > func.now(),
            ).values(status="ready_for_agent", s3_event_uri=event_uri, s3_logs_uri=logs_uri,
                     completed_at=func.now(), updated_at=func.now(), last_error=None,
                     lease_token=None, lease_expires_at=None))
            if result.rowcount != 1:
                raise LeaseLost()

    async def unenqueued_ids(self, limit: int = 100) -> list[UUID]:
        async with self.sessions() as session:
            return list(await session.scalars(select(RepairJob.id).where(
                RepairJob.sqs_message_id.is_(None), RepairJob.status.in_(["queued", "retrying"]),
            ).order_by(RepairJob.created_at).limit(limit)))
