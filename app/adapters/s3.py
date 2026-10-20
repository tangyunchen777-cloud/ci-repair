import asyncio
import re

from app.models import FailureEvent


def artifact_keys(failure: FailureEvent) -> tuple[str, str]:
    # GitHub owner/repo names cannot contain '/', '__' is the required separator.
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", failure.repository_full_name):
        raise ValueError("Invalid repository name")
    owner_repo = failure.repository_full_name.replace("/", "__")
    prefix = f"repositories/{owner_repo}/runs/{failure.run_id}"
    return f"{prefix}/ingestion.json", f"{prefix}/logs.zip"


class S3Artifacts:
    def __init__(self, client, bucket: str):
        self.client, self.bucket = client, bucket

    async def store(self, failure: FailureEvent, raw_logs: bytes) -> tuple[str, str]:
        event_key, logs_key = artifact_keys(failure)
        for key, body, content_type in (
            (event_key, failure.model_dump_json().encode("utf-8"), "application/json"),
            (logs_key, raw_logs, "application/zip"),
        ):
            await asyncio.to_thread(
                self.client.put_object, Bucket=self.bucket, Key=key, Body=body,
                ContentType=content_type, ServerSideEncryption="AES256",
            )
        return f"s3://{self.bucket}/{event_key}", f"s3://{self.bucket}/{logs_key}"
