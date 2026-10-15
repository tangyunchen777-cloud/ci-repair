# ci-repair — Class 2

A small Python 3.12 FastAPI service that receives GitHub webhooks, validates focused
workflow-run fields, and normalizes completed CI failures into a `FailureEvent`.
This class implements only the webhook-ingestion boundary, in one application.

## Intended end-state architecture

GitHub Actions failure → public FastAPI webhook → AWS SQS → long-running worker
on Kubernetes/EKS → LangGraph (diagnose → retrieve context → generate patch →
validate, with bounded self-correction) → isolated sandbox → draft pull request
or human escalation. PostgreSQL will hold operational state; S3 will hold large
logs and artifacts. The API will produce messages and the worker will consume
them. Generated changes must never be merged directly to main.

Only FastAPI ingestion and normalization exist today. There is no queue,
persistence, worker, code cloning, LLM call, sandbox, GitHub API call, AWS deployment,
Kubernetes, EKS, Terraform, autoscaling, or dashboard implementation.

## HTTP behavior

`GET /health` returns HTTP 200 and exactly `{"status":"ok"}`.

`POST /webhook` requires nonempty `X-GitHub-Event` and `X-GitHub-Delivery` headers
and valid JSON. Header names are case insensitive.

| Input | HTTP | JSON response |
| --- | --- | --- |
| `ping` | 200 | `{"status":"ok","event":"ping"}` |
| Other event types | 200 | `{"status":"ignored","reason":"unsupported_event"}` |
| `workflow_run`, action other than `completed` (including `requested` / `in_progress`) | 200 | `{"status":"ignored","reason":"action_not_completed"}` |
| Completed run with another conclusion (including `success`, `cancelled`, or null) | 200 | `{"status":"ignored","reason":"non_failure_conclusion"}` |
| Completed run with `failure`, `timed_out`, or `startup_failure` | 200 | `{"status":"accepted","delivery_id":"...","run_id":123}` |
| Invalid JSON / encoding | 400 | Clear `detail` message |
| Missing / empty headers or malformed workflow-run fields | 422 | Validation `detail` list |

All workflow-run payloads must pass schema validation before filtering. Unknown
GitHub fields are ignored. The required fields are top-level `action`,
`repository.full_name`, and `workflow_run.id`, `name`, `html_url`, `head_branch`,
`head_sha`, `conclusion` (nullable), and `updated_at` (timezone required).

Accepted events produce one JSON log containing only normalized metadata and
the event label `workflow_failure_accepted`. Full payloads are never logged.
Validation responses omit payload values. Uvicorn also emits ordinary server
and access logs. No webhook-secret or HMAC validation is implemented, as required
for this class. An accepted response means normalized and logged; it does not
mean durable delivery, deduplication, or repair execution.

## Normalized contract

`app/models.py` defines `FailureEvent`. `run_url` comes from `html_url`;
`event_timestamp` comes from GitHub's `workflow_run.updated_at`, rather than the
server receipt time. The contract's JSON serialization is
`failure.model_dump(mode="json")` (or `failure.model_dump_json()` for a string).

```json
{
  "delivery_id": "72d3162e-cc78-11e3-81ab-4c9367dc0958",
  "repository_full_name": "octocat/ci-repair-example",
  "workflow_name": "CI",
  "run_id": 123,
  "run_url": "https://github.com/octocat/ci-repair-example/actions/runs/123",
  "head_branch": "feature/repair",
  "head_sha": "0123456789abcdef0123456789abcdef01234567",
  "conclusion": "failure",
  "event_timestamp": "2026-10-14T18:30:00Z"
}
```

IDs are positive integers; URLs must be HTTP(S) URLs. Conclusions in the normalized
contract are limited to the three accepted failure values. All other fields are
nonempty strings except the timezone-aware event timestamp. Requirements use
bounded compatible version ranges; they are not an exact dependency lockfile.

## Docker-only local workflow

Prerequisite: Docker with a running Linux container engine. Run commands from
the repository root. Dependencies are installed only inside the image.

```sh
docker build -t ci-repair:class2 .
docker run --rm ci-repair:class2 python -m pytest -q -p no:cacheprovider
docker run --detach --rm --name ci-repair-class2 -e PORT=8080 -p 8080:8080 ci-repair:class2
```

From the host, after Uvicorn starts:

```sh
curl --fail-with-body -i http://localhost:8080/health
curl --fail-with-body -i -X POST http://localhost:8080/webhook -H 'Content-Type: application/json' -H 'X-GitHub-Event: ping' -H 'X-GitHub-Delivery: local-ping-001' --data '{}'
```

In Windows PowerShell, use `curl.exe` in place of `curl`. Health should return
200 with `{"status":"ok"}`; ping should return 200 with
`{"status":"ok","event":"ping"}`.

To exercise an accepted failure from the host:

```sh
curl --fail-with-body -i -X POST http://localhost:8080/webhook -H 'Content-Type: application/json' -H 'X-GitHub-Event: workflow_run' -H 'X-GitHub-Delivery: local-failure-001' --data-binary '@tests/fixtures/workflow_run_failure.json'
docker logs ci-repair-class2
docker stop ci-repair-class2
```

The image runs as an unprivileged numeric user. `EXPOSE 8000` documents the default;
the start command binds `0.0.0.0` to `${PORT:-8000}`, so `PORT=8080` changes the
actual listener. No `railway.json` is required.

## Manual GitHub and Railway checklist

1. Create a **private** GitHub repository in your account. Commit this project's
   source files and push the branch you intend Railway to deploy. Confirm the
   repository displays **Private**. Exclude `.env`, `work/`, and `outputs/`.
2. In Railway, create a project from **GitHub Repo**, authorize access to that
   private repository, and select it and the deployment branch. Use the repository
   root as the service root. Confirm Railway detects the root `Dockerfile`; keep
   the image's start command. No AWS resources or extra services are needed.
3. Set the service's health-check path to `/health`. Keep Railway's injected
   `PORT`; the Docker command already reads it and binds to `0.0.0.0`. Deploy and
   inspect build/startup logs.
4. Under the service's Settings → Networking, choose **Generate Domain**.
   Use the listener's port if Railway requests a target port. Confirm the public
   `https://YOUR-DOMAIN/health` returns 200 and `{"status":"ok"}`.
5. In the GitHub repository whose workflow runs you want to monitor, open
   Settings → Webhooks → Add webhook. Set the payload URL to
   `https://YOUR-DOMAIN/webhook`, content type to `application/json`, and keep SSL
   verification enabled. Leave Secret empty for this class because validation
   is deliberately unimplemented. Choose individual events and select
   **Workflow runs**; leave the webhook active and save it.
6. Inspect Recent Deliveries: the initial ping should return 200. Run an existing
   CI workflow, confirm successful runs are ignored, and inspect a completed
   failed run's delivery for the accepted response and normalized service log.
   This webhook subscription is separate from a workflow's YAML triggers.

See [Railway repository and Dockerfile deployment](https://docs.railway.com/services),
[health checks and PORT](https://docs.railway.com/deployments/healthchecks),
[public domains](https://docs.railway.com/networking/public-networking), and
[GitHub webhook setup](https://docs.github.com/en/webhooks/using-webhooks/creating-webhooks).

## Class 3 handoff

In `app/main.py`, `webhook()` constructs the validated `failure: FailureEvent`
and logs it. **Immediately after that log, before returning the accepted
response**, the exact non-executing integration placeholders are:

```python
# PostgreSQL placeholder — implement in Class 3:
# await failure_repository.create(failure)
# AWS SQS placeholder — implement in Class 3:
# await queue_publisher.publish(failure)
```

Class 3 should introduce the publisher there and serialize the normalized
`FailureEvent`, rather than forwarding the raw GitHub payload. It must define
durable acknowledgement, retry/error behavior, and delivery-ID idempotency before
claiming reliable ingestion. Repair work belongs in the future consumer. These
comments do not persist or enqueue anything today.

## Verification status for this workspace

The test suite covers health, ping, unsupported events, requested/in-progress
runs, ignored completed conclusions, every accepted failure conclusion, exact
normalization and log content, missing/empty headers, malformed JSON/encoding,
missing required fields, and malformed workflow-run types.

Docker is unavailable on the local authoring machine. The additional
`.github/workflows/class2-validation.yml` runs the same image build, container
pytest, PORT=8080 startup, exact host curl response checks, and container cleanup
on GitHub's hosted Linux runner for pull requests. Check the PR's Actions result
for the verified outcome. No Python dependencies are installed on either host.
Railway deployment and a live GitHub webhook require the manual account setup
above.
