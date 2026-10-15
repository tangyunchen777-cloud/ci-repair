import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app, logger
from app.models import FailureEvent, WorkflowRunPayload, normalize_failure

DELIVERY_ID = "72d3162e-cc78-11e3-81ab-4c9367dc0958"
HEADERS = {"X-GitHub-Event": "workflow_run", "X-GitHub-Delivery": DELIVERY_ID}
FIXTURE = Path(__file__).parent / "fixtures" / "workflow_run_failure.json"


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def payload():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def capture_events(caplog):
    logger.addHandler(caplog.handler)
    yield
    logger.removeHandler(caplog.handler)


def test_health(client):
    assert app.title == "ci-repair"
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ping(client, caplog):
    response = client.post("/webhook", headers={**HEADERS, "X-GitHub-Event": "ping"}, json={})
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "event": "ping"}
    assert not caplog.records


def test_unsupported_event(client, caplog):
    response = client.post("/webhook", headers={**HEADERS, "X-GitHub-Event": "push"}, json={})
    assert response.status_code == 200
    assert response.json() == {"status": "ignored", "reason": "unsupported_event"}
    assert not caplog.records


@pytest.mark.parametrize("action", ["requested", "in_progress"])
@pytest.mark.parametrize("conclusion", [None, "failure"])
def test_uncompleted_runs_are_ignored(client, payload, caplog, action, conclusion):
    payload["action"] = action
    payload["workflow_run"]["conclusion"] = conclusion
    response = client.post("/webhook", headers=HEADERS, json=payload)
    assert response.status_code == 200
    assert response.json() == {"status": "ignored", "reason": "action_not_completed"}
    assert not caplog.records


@pytest.mark.parametrize("conclusion", ["success", "cancelled", "neutral", "skipped", "stale", None])
def test_non_failure_runs_are_ignored(client, payload, caplog, conclusion):
    payload["workflow_run"]["conclusion"] = conclusion
    response = client.post("/webhook", headers=HEADERS, json=payload)
    assert response.status_code == 200
    assert response.json() == {"status": "ignored", "reason": "non_failure_conclusion"}
    assert not caplog.records


def expected_failure():
    return {
        "delivery_id": DELIVERY_ID,
        "repository_full_name": "octocat/ci-repair-example",
        "workflow_name": "CI",
        "run_id": 123,
        "run_url": "https://github.com/octocat/ci-repair-example/actions/runs/123",
        "head_branch": "feature/repair",
        "head_sha": "0123456789abcdef0123456789abcdef01234567",
        "conclusion": "failure",
        "event_timestamp": "2026-10-14T18:30:00Z",
    }


@pytest.mark.parametrize("conclusion", ["failure", "timed_out", "startup_failure"])
def test_accepted_failures_log_only_normalized_metadata(client, payload, caplog, conclusion):
    payload["workflow_run"]["conclusion"] = conclusion
    response = client.post("/webhook", headers=HEADERS, json=payload)
    assert response.status_code == 200
    assert response.json() == {"status": "accepted", "delivery_id": DELIVERY_ID, "run_id": 123}
    assert len(caplog.records) == 1
    record = json.loads(caplog.records[0].getMessage())
    assert record == {"event": "workflow_failure_accepted", **expected_failure(), "conclusion": conclusion}
    assert "unused-payload-sentinel" not in caplog.text


def test_exact_normalization(payload):
    failure = normalize_failure(WorkflowRunPayload.model_validate(payload), DELIVERY_ID)
    assert isinstance(failure, FailureEvent)
    assert failure.model_dump(mode="json") == expected_failure()


@pytest.mark.parametrize("missing", [["X-GitHub-Event"], ["X-GitHub-Delivery"], list(HEADERS)])
def test_missing_headers(client, payload, missing, caplog):
    response = client.post(
        "/webhook", headers={k: v for k, v in HEADERS.items() if k not in missing}, json=payload
    )
    assert response.status_code == 422
    assert {error["loc"][-1].lower() for error in response.json()["detail"]} == {
        header.lower() for header in missing
    }
    assert not caplog.records


@pytest.mark.parametrize("header", list(HEADERS))
@pytest.mark.parametrize("value", ["", "   "])
def test_empty_headers(client, payload, header, value):
    response = client.post("/webhook", headers={**HEADERS, header: value}, json=payload)
    assert response.status_code == 422


@pytest.mark.parametrize("event", ["workflow_run", "ping", "push"])
@pytest.mark.parametrize("body", [b'{"broken":', b"", b"\xff"])
def test_invalid_json(client, caplog, event, body):
    response = client.post("/webhook", headers={**HEADERS, "X-GitHub-Event": event}, content=body)
    assert response.status_code == 400
    assert response.json() == {"detail": "Request body must be valid JSON"}
    assert not caplog.records


REQUIRED_PATHS = [
    ("action",), ("repository",), ("repository", "full_name"), ("workflow_run",),
    *(("workflow_run", field) for field in (
        "id", "name", "html_url", "head_branch", "head_sha", "conclusion", "updated_at"
    )),
]


@pytest.mark.parametrize("path", REQUIRED_PATHS)
def test_missing_required_fields(client, payload, caplog, path):
    parent = payload
    for field in path[:-1]:
        parent = parent[field]
    del parent[path[-1]]
    response = client.post("/webhook", headers=HEADERS, json=payload)
    assert response.status_code == 422
    assert any(error["loc"] == list(path) for error in response.json()["detail"])
    assert "input" not in response.json()["detail"][0]
    assert not caplog.records


@pytest.mark.parametrize("body", [None, [], "invalid", 123])
def test_non_object_workflow_payload(client, body):
    response = client.post("/webhook", headers=HEADERS, content=json.dumps(body))
    assert response.status_code == 422


@pytest.mark.parametrize("field,value", [
    ("id", "123"), ("id", True), ("id", 0), ("name", ""), ("head_branch", None),
    ("html_url", "not-a-url"), ("updated_at", "invalid"),
    ("updated_at", "2026-10-14T18:30:00"),
])
def test_malformed_workflow_fields(client, payload, caplog, field, value):
    payload["workflow_run"][field] = value
    response = client.post("/webhook", headers=HEADERS, json=payload)
    assert response.status_code == 422
    assert not caplog.records
