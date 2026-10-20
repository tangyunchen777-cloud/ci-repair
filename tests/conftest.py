import json
from pathlib import Path

import pytest

from app.models import WorkflowRunPayload, normalize_failure


@pytest.fixture
def failure():
    payload = json.loads((Path(__file__).parent / "fixtures/workflow_run_failure.json").read_text())
    return normalize_failure(WorkflowRunPayload.model_validate(payload), "unit-delivery")
