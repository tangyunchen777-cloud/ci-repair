"""Durable Class 4 ingestion jobs and fenced short leases."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_repair_jobs"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "repair_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        *[sa.Column(name, sa.Text(), nullable=False) for name in (
            "delivery_id", "repository_full_name", "repository_clone_url", "workflow_name",
            "run_url", "logs_url", "head_branch", "head_sha", "conclusion",
        )],
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("event_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(24), nullable=False, server_default="queued"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        *[sa.Column(name, sa.Text()) for name in (
            "sqs_message_id", "s3_event_uri", "s3_logs_uri", "last_error",
        )],
        *[sa.Column(name, sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())
          for name in ("created_at", "updated_at")],
        *[sa.Column(name, sa.DateTime(timezone=True)) for name in (
            "enqueued_at", "started_at", "completed_at", "lease_expires_at", "enqueue_lease_until",
        )],
        sa.Column("lease_token", postgresql.UUID(as_uuid=True)),
        sa.Column("enqueue_token", postgresql.UUID(as_uuid=True)),
        sa.UniqueConstraint("delivery_id", name="uq_repair_jobs_delivery_id"),
        sa.CheckConstraint("status IN ('queued','processing','retrying','ready_for_agent','failed')", name="ck_repair_jobs_status"),
        sa.CheckConstraint("retry_count >= 0", name="ck_repair_jobs_retry_count"),
    )


def downgrade():
    op.drop_table("repair_jobs")
