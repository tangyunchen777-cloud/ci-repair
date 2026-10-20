import json
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.adapters.s3 import artifact_keys
from app.models import QueueEnvelope


def test_envelope_roundtrip_contains_durable_handoff(failure):
    envelope = QueueEnvelope(job_id=uuid4(), failure=failure)
    serialized = envelope.model_dump_json()
    assert QueueEnvelope.model_validate_json(serialized) == envelope
    body = json.loads(serialized)
    assert body == {"schema_version": 1, "job_id": str(envelope.job_id), "failure": failure.model_dump(mode="json")}
    assert {"logs_url", "repository_clone_url", "head_sha"} <= body["failure"].keys()
    assert "token" not in serialized


@pytest.mark.parametrize("change", [{"schema_version": 2}, {"job_id": "bad"}, {"unexpected": "field"}])
def test_invalid_envelopes_rejected(failure, change):
    body = QueueEnvelope(job_id=uuid4(), failure=failure).model_dump(mode="json") | change
    with pytest.raises(ValidationError):
        QueueEnvelope.model_validate(body)


def test_deterministic_artifact_keys(failure):
    assert artifact_keys(failure) == (
        "repositories/octocat__ci-repair-example/runs/123/ingestion.json",
        "repositories/octocat__ci-repair-example/runs/123/logs.zip",
    )
    assert artifact_keys(failure.model_copy(update={"delivery_id": "redelivery"})) == artifact_keys(failure)


def test_invalid_repository_cannot_escape_key(failure):
    with pytest.raises(ValueError):
        artifact_keys(failure.model_copy(update={"repository_full_name": "owner/../../repo"}))
