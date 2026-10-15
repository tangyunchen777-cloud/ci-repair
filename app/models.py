"""Focused GitHub input models and the future queue message contract."""

from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, Field, HttpUrl, StringConstraints

NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
FailureConclusion = Literal["failure", "timed_out", "startup_failure"]
FAILURE_CONCLUSIONS = frozenset({"failure", "timed_out", "startup_failure"})


class Repository(BaseModel):
    full_name: NonEmptyString


class WorkflowRun(BaseModel):
    id: Annotated[int, Field(strict=True, gt=0)]
    name: NonEmptyString
    html_url: HttpUrl
    head_branch: NonEmptyString
    head_sha: NonEmptyString
    conclusion: NonEmptyString | None
    updated_at: AwareDatetime


class WorkflowRunPayload(BaseModel):
    action: NonEmptyString
    repository: Repository
    workflow_run: WorkflowRun


class FailureEvent(BaseModel):
    delivery_id: NonEmptyString
    repository_full_name: NonEmptyString
    workflow_name: NonEmptyString
    run_id: Annotated[int, Field(strict=True, gt=0)]
    run_url: HttpUrl
    head_branch: NonEmptyString
    head_sha: NonEmptyString
    conclusion: FailureConclusion
    event_timestamp: AwareDatetime


def normalize_failure(payload: WorkflowRunPayload, delivery_id: str) -> FailureEvent:
    """Normalize an accepted run; updated_at is the source event timestamp."""
    run = payload.workflow_run
    return FailureEvent(
        delivery_id=delivery_id,
        repository_full_name=payload.repository.full_name,
        workflow_name=run.name,
        run_id=run.id,
        run_url=run.html_url,
        head_branch=run.head_branch,
        head_sha=run.head_sha,
        conclusion=run.conclusion,
        event_timestamp=run.updated_at,
    )
