import asyncio
import json
import logging
import signal

from app.adapters.aws import aws_client
from app.adapters.github import GitHubLogs
from app.adapters.s3 import S3Artifacts
from app.adapters.sqs import SQSQueue
from app.config import get_settings
from app.db import make_engine, make_sessions
from app.models import QueueEnvelope
from app.repositories.failure_jobs import FailureJobRepository

logger = logging.getLogger("ci_repair.worker")


def log(event: str, **identifiers):
    logger.info(json.dumps({"event": event, **identifiers}))


class Worker:
    def __init__(self, repository, queue, github, artifacts, settings):
        self.repository, self.queue = repository, queue
        self.github, self.artifacts, self.settings = github, artifacts, settings

    async def process(self, message: dict):
        # Never log the raw body, receipt handle, token, download URL, or exception.
        try:
            envelope = QueueEnvelope.model_validate_json(message["Body"])
            receipt = message["ReceiptHandle"]
        except Exception:
            log("invalid_queue_message", message_id=message.get("MessageId"))
            return  # No valid job ID to update; SQS redrives poison messages.
        identifiers = {"job_id": str(envelope.job_id), "message_id": message.get("MessageId")}
        claim = None
        stage = "claim"
        try:
            async with asyncio.timeout(self.settings.operation_timeout_seconds):
                claim = await self.repository.claim(envelope.job_id, self.settings.worker_lease_seconds)
                if claim.outcome == "busy":
                    log("job_busy", **identifiers)
                    return  # Another worker's active lease must not be acknowledged.
                if claim.outcome == "already_complete":
                    await self.queue.delete(receipt)
                    log("terminal_duplicate_acknowledged", **identifiers)
                    return
                if claim.job.failure_event() != envelope.failure:
                    raise ValueError("Queue data does not match durable job")
                stage = "github_download"
                raw_logs = await self.github.download(envelope.failure)
                stage = "s3_upload"
                event_uri, logs_uri = await self.artifacts.store(envelope.failure, raw_logs)
                stage = "database_complete"
                await self.repository.mark_ready_for_agent(envelope.job_id, claim.token, event_uri, logs_uri)
                stage = "sqs_delete"
                await self.queue.delete(receipt)
                log("job_ready_for_agent", **identifiers)
        except Exception:
            if claim and claim.outcome == "claimed" and stage != "sqs_delete":
                try:
                    await self.repository.record_retry(envelope.job_id, claim.token, f"{stage}_failed")
                except Exception:
                    # A DB outage may prevent recording this attempt. Leave the
                    # message unacknowledged and let the fenced lease expire.
                    log("retry_record_failed", **identifiers)
            log("job_attempt_failed", stage=stage, **identifiers)
            # If DeleteMessage failed after commit, redelivery takes terminal path.

    async def run(self, stop: asyncio.Event):
        while not stop.is_set():
            try:
                message = await self.queue.receive()
                if message:
                    # SIGTERM during a poll: finish this single bounded operation.
                    await self.process(message)
            except Exception:
                log("queue_receive_failed")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=5)
                except TimeoutError:
                    pass


async def main():
    settings = get_settings()
    settings.require("database_url", "aws_region", "sqs_queue_url", "s3_bucket", "github_token")
    engine = make_engine(settings)
    sqs, s3 = await asyncio.gather(
        asyncio.to_thread(aws_client, "sqs", settings), asyncio.to_thread(aws_client, "s3", settings),
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        worker = Worker(FailureJobRepository(make_sessions(engine)), SQSQueue(sqs, settings.sqs_queue_url),
                        GitHubLogs(settings), S3Artifacts(s3, settings.s3_bucket), settings)
        await worker.run(stop)
    finally:
        await engine.dispose()
        await asyncio.gather(asyncio.to_thread(sqs.close), asyncio.to_thread(s3.close))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    # Suppress HTTP URLs (including signed redirect query strings) in library logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    asyncio.run(main())
