from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.models import FailureEvent


class Base(DeclarativeBase):
    pass


class RepairJob(Base):
    __tablename__ = "repair_jobs"
    __table_args__ = (
        CheckConstraint("status IN ('queued','processing','retrying','ready_for_agent','failed')", name="ck_repair_jobs_status"),
        CheckConstraint("retry_count >= 0", name="ck_repair_jobs_retry_count"),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    delivery_id: Mapped[str] = mapped_column(Text, unique=True)
    repository_full_name: Mapped[str] = mapped_column(Text)
    repository_clone_url: Mapped[str] = mapped_column(Text)
    workflow_name: Mapped[str] = mapped_column(Text)
    run_id: Mapped[int] = mapped_column(BigInteger)
    run_url: Mapped[str] = mapped_column(Text)
    logs_url: Mapped[str] = mapped_column(Text)
    head_branch: Mapped[str] = mapped_column(Text)
    head_sha: Mapped[str] = mapped_column(Text)
    conclusion: Mapped[str] = mapped_column(Text)
    event_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(24), server_default="queued")
    retry_count: Mapped[int] = mapped_column(Integer, server_default="0")
    sqs_message_id: Mapped[str | None] = mapped_column(Text)
    s3_event_uri: Mapped[str | None] = mapped_column(Text)
    s3_logs_uri: Mapped[str | None] = mapped_column(Text)
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    enqueued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Tokens fence stale workers and publishers without holding a transaction over I/O.
    lease_token: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    enqueue_token: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True))
    enqueue_lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    def failure_event(self) -> FailureEvent:
        return FailureEvent.model_validate({name: getattr(self, name) for name in FailureEvent.model_fields})
