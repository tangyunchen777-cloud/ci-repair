"""One bounded operator sweep of jobs with no recorded SQS message ID."""
import asyncio
import json
from uuid import UUID
import argparse

from app.adapters.aws import aws_client
from app.adapters.sqs import SQSQueue
from app.config import get_settings
from app.db import make_engine, make_sessions
from app.repositories.failure_jobs import FailureJobRepository
from app.services.ingestion import IngestionService


async def main(job_id: UUID | None):
    settings = get_settings()
    settings.require("database_url", "aws_region", "sqs_queue_url")
    engine = make_engine(settings)
    client = await asyncio.to_thread(aws_client, "sqs", settings)
    try:
        repository = FailureJobRepository(make_sessions(engine))
        service = IngestionService(repository, SQSQueue(client, settings.sqs_queue_url), settings.enqueue_lease_seconds)
        ids = [job_id] if job_id else await repository.unenqueued_ids()
        failed = False
        for candidate in ids:
            try:
                print(json.dumps(await service.publish_job(candidate)))
            except Exception:
                failed = True
                print(json.dumps({"event": "enqueue_retry_failed", "job_id": str(candidate)}))
        if failed:
            raise SystemExit(1)
    finally:
        await engine.dispose()
        await asyncio.to_thread(client.close)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", type=UUID)
    asyncio.run(main(parser.parse_args().job_id))
