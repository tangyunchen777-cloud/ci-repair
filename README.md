# ci-repair — Class 4 durable ingestion

Extends the existing `class2-webhook` implementation. The durable handoff is
`ready_for_agent`; Class 5 starts LangGraph there. This class does not clone,
repair, execute target repository tests, call an LLM, create target pull requests,
or deploy Kubernetes/EKS.

## Architecture and code

```mermaid
flowchart LR
  GH[GitHub workflow_run] --> API[Railway FastAPI]
  API --> DB[(PostgreSQL 16 / RDS)]
  API --> Q[SQS standard queue]
  Q --> W[Docker worker]
  W --> LOG[Authenticated GitHub logs download]
  W --> S3[(Private S3 event + raw ZIP)]
  W --> DB
  Q --> DLQ[DLQ after 3 receives]
  DB --> READY[ready_for_agent]
```

Existing `app/main.py`, `app/models.py`, fixture, exact response tests, Dockerfile,
requirements and PR workflow are extended. Added:

```text
app/config.py, db.py, db_models.py
app/repositories/failure_jobs.py
app/services/ingestion.py
app/adapters/aws.py, sqs.py, s3.py, github.py
worker/main.py, retry_enqueue.py
alembic/env.py, versions/0001_repair_jobs.py
localstack/init.py
tests/test_contracts.py, test_github.py, test_ingestion.py, test_worker.py
tests/integration/test_pipeline.py
docker-compose.yml, .env.example, pytest.ini
```

One Python 3.12 image serves API, worker, migrations, enqueue recovery, and tests.
It retains UID/GID 10001 and binds `0.0.0.0:${PORT:-8000}`. boto3 operations in
async application code run through `asyncio.to_thread`. Settings come from the
environment; `.env.example` contains disposable local examples, never cloud secrets.

## HTTP and normalized contract

`GET /health` is a liveness check: exactly HTTP 200 `{"status":"ok"}` even if
durable services are unconfigured. It is not a database/queue readiness guarantee.
`POST /webhook` still requires nonblank `X-GitHub-Event` and `X-GitHub-Delivery`.

| Input | HTTP | Response |
| --- | --- | --- |
| ping | 200 | `{"status":"ok","event":"ping"}` |
| Unsupported event | 200 | `{"status":"ignored","reason":"unsupported_event"}` |
| workflow_run action != completed | 200 | `{"status":"ignored","reason":"action_not_completed"}` |
| Completed nonfailure conclusion | 200 | `{"status":"ignored","reason":"non_failure_conclusion"}` |
| Completed failure / timed_out / startup_failure | 200 | `{"status":"accepted","delivery_id":"...","run_id":123,"job_id":"UUID"}` |
| Previously enqueued delivery | 200 | Same identifiers with `"status":"duplicate"` |
| Invalid JSON / encoding | 400 | Clear detail |
| Missing headers or invalid focused fields | 422 | Details without input values |
| DB/SQS unavailable, or concurrent enqueue in progress | 503 | Sanitized retry detail |

Workflow-run schema validation precedes filtering. Required: `action`,
`repository.full_name`, `repository.clone_url` (HTTP/S), and `workflow_run.id`
(strict positive integer), `name`, `html_url`, `logs_url` (HTTP/S), `head_branch`,
`head_sha`, `conclusion` (nullable), `updated_at` (aware datetime). Unknown GitHub
fields are ignored. No webhook-secret/HMAC validation is required in this class.

`normalize_failure()` still produces the single `FailureEvent` model. It adds
`repository_clone_url` from repository.clone_url and `logs_url` from the run.
`event_timestamp` is run.updated_at. `head_sha` is the exact commit Actions tested.
The one safe `workflow_failure_accepted` log contains this normalized metadata,
never the full payload; this log precedes persistence and alone is not success.

SQS contains JSON (no credentials):

```json
{
  "schema_version": 1,
  "job_id": "5fd03462-1fd2-4cc8-98d3-b90914ed1d55",
  "failure": {
    "delivery_id": "example-delivery",
    "repository_full_name": "octocat/ci-repair-example",
    "repository_clone_url": "https://github.com/octocat/ci-repair-example.git",
    "workflow_name": "CI",
    "run_id": 123,
    "run_url": "https://github.com/octocat/ci-repair-example/actions/runs/123",
    "logs_url": "https://api.github.com/repos/octocat/ci-repair-example/actions/runs/123/logs",
    "head_branch": "feature/repair",
    "head_sha": "0123456789abcdef0123456789abcdef01234567",
    "conclusion": "failure",
    "event_timestamp": "2026-10-14T18:30:00Z"
  }
}
```

## Transactions, state machine, delivery semantics

`queued -> processing -> ready_for_agent`; failures in a claimed attempt use
`processing -> retrying -> processing`. `failed` is reserved for an operator's
terminal decision after DLQ investigation. SQS moving a message to DLQ does not
itself update PostgreSQL; a job may remain retrying (or processing after a crash).

1. `create_or_get` uses PostgreSQL ON CONFLICT on unique delivery_id and commits.
   Concurrent deliveries create one UUID job. Duplicate payloads cannot replace
   its original data. Every repository method owns a short transaction.
2. A conditional enqueue lease serializes publishers for that job. It commits
   before SQS I/O, lasts 120 seconds, and is cleared after recording publication
   or a definite publish error. A competing request receives 503 until it can
   return duplicate. No transaction/row lock spans a network request.
3. Publish the persisted job in the versioned envelope. Only after SQS returns
   do `sqs_message_id` and `enqueued_at` commit, followed by accepted HTTP 200.
   Publish failure records `sqs_publish_failed`, retains the job, and returns 503.
4. **Remaining dual-write limitation:** SQS can succeed while its response is
   lost or the final DB update fails. An expired enqueue lease allows an operator
   retry/redelivery to publish again. This class has no transactional outbox and
   does not promise exactly-once publication.
5. The worker receives one message with a 20-second long poll and 300-second SQS
   visibility. It validates the envelope, atomically claims a 240-second DB lease,
   and checks the envelope against durable event data. Outcomes: claimed,
   already_complete (safe to delete), busy (never delete another active lease).
   Expired processing leases can be reclaimed; tokens fence stale DB writes.
6. A bounded 180-second attempt downloads the GitHub ZIP, then writes event JSON
   and the original ZIP bytes to S3 with SSE-S3 (`AES256`) and application/json /
   application/zip. No extraction, re-encoding, or raw-log printing occurs.
7. Only after both uploads does `ready_for_agent` commit with both URIs. Then
   DeleteMessage acknowledges SQS. A delete failure is safe: the next delivery
   acknowledges the terminal row without repeating downloads/uploads.
8. Download/S3/DB failures record a fixed sanitized stage code, increment retry_count,
   set retrying, and leave the message undeleted. If PostgreSQL itself is down,
   recording the failure is necessarily best effort; the message/lease survives.
   A crash after partial S3 writes repeats deterministic PUTs on retry.

Keys are `repositories/{owner__repo}/runs/{run_id}/ingestion.json` and `logs.zip`.
Different webhook deliveries for the same run intentionally share these keys;
this class stores one current artifact pair per run, not per rerun attempt.
S3 bytes can be rewritten during recovery; durable job completion is fenced.

The downloader authenticates only the exact api.github.com endpoint for the
recorded repository/run, sends Accept and X-GitHub-Api-Version, and follows HTTPS
GitHub/blob redirects without forwarding Authorization. Default connect/read
limits are 10/30 seconds, total attempt 180 seconds, raw ZIP limit 25 MiB. Oversize,
expired, inaccessible, or non-ZIP logs retry and can reach DLQ. Never enable HTTP
wire/debug logging or print token/signed redirect URL/exception text.

## Docker-only local development and full integration

Prerequisite: Docker Engine/Desktop with Linux containers and Compose v2. No
host virtual environment, host pip, or host application/test execution is needed.
From repository root, copy `.env.example` to `.env` (`Copy-Item .env.example .env`
in PowerShell). Local PostgreSQL credentials and AWS dummy credentials in that
file are for this isolated stack only. Configure a real GITHUB_TOKEN there only
when running the real worker; Compose passes it solely to the worker container.
The test service constructs a mocked downloader and needs no real token.

```sh
cp .env.example .env
docker compose build api
docker run --rm ci-repair:class4 python -m pytest -q -m 'not integration' -p no:cacheprovider
docker compose up -d --wait postgres localstack
docker compose run --rm migration
docker compose run --rm migration alembic check
docker compose up -d --wait api
docker compose --profile test run --rm test
```

The LocalStack Python READY hook creates ci-repair-events-dlq and ci-repair-events
with redrive maxReceiveCount=3, long polling=20, visibility=300, and a private
SSE-S3 bucket. All names, region, URLs and bucket are configurable. AWS_ENDPOINT_URL
selects LocalStack. `postgres` exposes no host DB port; LocalStack/API bind only
host loopback. LocalStack resources reset when recreated; PostgreSQL uses a volume.
Keep .env region, queue URL path and selected names consistent if changing examples.

Tests exercise Alembic schema, API persist+enqueue, old health/ping/ignore/invalid
contracts, exact extended event/response/log, concurrent duplicate delivery, claims
and stale tokens, bounded download, both encrypted S3 objects, terminal duplicate
acknowledgement, GitHub/S3/DB failure retry, and actual LocalStack DLQ redrive.
The DLQ test shortens visibility only for its isolated test queue/message to avoid
waiting 300 seconds per attempt. Integration tests truncate the isolated test DB;
**never point the test service at cloud or production resources**. Run suites
serially; do not run integration tests against the same DB as a live worker.

After tests, start the real worker with a selected-repository GitHub token:

```sh
docker compose up -d worker
curl --fail-with-body http://localhost:8080/health
curl --fail-with-body -X POST http://localhost:8080/webhook -H 'Content-Type: application/json' -H 'X-GitHub-Event: workflow_run' -H 'X-GitHub-Delivery: local-failure-001' --data-binary '@tests/fixtures/workflow_run_failure.json'
docker compose logs -f worker
```

Use curl.exe in Windows PowerShell. Fixture run 123 is synthetic; its actual GitHub
download will fail. Use a real failed workflow delivery for a live demonstration;
the integration suite covers successful download with a mock and real local DB/SQS/S3.

`.github/workflows/class2-validation.yml` is extended to Class 4 Docker validation.
It runs these same containers on a GitHub Linux runner, additionally round-trips
Alembic downgrade/upgrade and verifies UID 10001 and graceful worker SIGTERM.
Check the Actions run for actual results; authoring-host Docker is unavailable.

## GitHub token and manual redelivery

Create a fine-grained PAT scoped only to monitored repositories with **Actions:
read** (metadata read is implicit); approve organization access if required.
Use an expiring token, store GITHUB_TOKEN only in the worker's secret environment,
and rotate/revoke it after the demo. A GitHub App installation token with equivalent
permissions also works but its refresh must be handled operationally. The API,
migration, SQS message, and event artifact never need the token. Logs may expire
according to repository retention; confirm the run is still downloadable.

Configure GitHub repository Settings -> Webhooks -> the existing hook: public
API `/webhook`, JSON, SSL verification enabled, Workflow runs selected. Classroom
HMAC validation remains deliberately absent.
GitHub does **not automatically redeliver** failed webhook deliveries. For a 503,
fix service configuration/permissions, open Settings -> Webhooks -> Recent
Deliveries -> failed delivery -> **Redeliver** (same delivery ID stays idempotent).
Alternatively run the operational publisher sweep (up to 100 oldest unenqueued
queued/retrying jobs; rerun for more):

```sh
docker compose run --rm --no-deps api python -m worker.retry_enqueue
docker compose run --rm --no-deps api python -m worker.retry_enqueue --job-id JOB_UUID
```

It returns nonzero if any job could not publish. After an ambiguous final commit,
wait for the 120-second enqueue lease to expire before recovery. It intentionally
uses API credentials and does not need GITHUB_TOKEN or S3. If a job already reached
ready_for_agent but has no recorded SQS ID, no extra publication is needed; an
explicit --job-id sweep is safe (the consumer terminal path acknowledges it).

## AWS Console setup: RDS PostgreSQL 16

Cloud setup is manual and can incur charges. Railway hosts **only the API**;
use real RDS, SQS/DLQ and S3, plus a separate Docker worker host. No cloud resources
are provisioned by this code.

1. Select an approved AWS region. In VPC -> Security groups create a dedicated RDS
   group. Allow TCP 5432 only from approved API static egress CIDR(s) and the worker's
   stable egress /32 or its security group when connected privately. **Never allow
   0.0.0.0/0 or ::/0 on 5432.** Do not substitute Railway's public inbound domain IP.
2. In RDS -> Parameter groups -> Create, choose PostgreSQL 16 family; edit
   `rds.force_ssl=1`. RDS -> Databases -> Create database -> Standard create ->
   PostgreSQL -> version 16; select your VPC, DB subnet group and dedicated SG,
   encryption, backups, a small demo instance, and an approved credential source.
   Associate the custom parameter group and reboot if its change requires it.
3. Prefer private RDS connectivity through an approved network path. If Railway
   requires a publicly addressable RDS endpoint for this demo, restrict the SG to
   its stable outbound IP(s) and worker egress only. Railway static outbound IPs
   require an eligible plan. If stable approved egress/private connectivity is not
   available, stop here and arrange it; never open the DB to the world.
4. Wait for Available. Copy the DNS endpoint/port from Connectivity & security.
   Download AWS's RDS CA bundle from the official RDS certificate documentation.
   Mount it read-only into migration/API/worker containers. Set
   DATABASE_URL=`postgresql+asyncpg://USER:URL_ENCODED_PASSWORD@RDS_DNS:5432/DB`,
   DATABASE_SSL=true, DATABASE_SSL_CA to that mounted path. Do not use `sslmode`
   query parameters. The code verifies the certificate and endpoint hostname.
5. Use a migration-only DB owner. From the approved network, run the image with a
   restricted migration env file and the CA mount:

```sh
docker run --rm --env-file /secure/migration.env --mount type=bind,src=/secure/rds-ca.pem,dst=/certs/rds-ca.pem,readonly ci-repair:class4 alembic upgrade head
docker run --rm --env-file /secure/migration.env --mount type=bind,src=/secure/rds-ca.pem,dst=/certs/rds-ca.pem,readonly ci-repair:class4 alembic check
```

Create separate API and worker DB login roles and grant only CONNECT, schema USAGE,
and required SELECT/INSERT/UPDATE on repair_jobs (worker needs SELECT/UPDATE; API
needs SELECT/INSERT/UPDATE). Neither runtime login needs DDL, table deletion or
migration ownership. Store connection secrets outside git; do not echo them.

## AWS Console setup: SQS/DLQ and private S3

1. SQS -> Queues -> Create queue -> **Standard**, create ci-repair-events-dlq
   first; choose SSE-SQS and a retention period longer than the source (e.g. 14 days).
2. Create ci-repair-events as Standard with Visibility timeout **300 seconds**,
   Receive message wait time **20 seconds**, SSE-SQS, and an appropriate source
   retention (e.g. 4 days). Under Dead-letter queue, enable redrive, select the DLQ,
   set Maximum receives **3**. On DLQ redrive allow policy, allow only this source
   queue. Record the queue URL and both ARNs in your private deployment config.
3. S3 -> Buckets -> Create bucket -> unique configured name in the chosen region.
   Leave **all Block Public Access settings on**, Object Ownership **Bucket owner
   enforced** (ACLs disabled), and default encryption **SSE-S3**. No public bucket
   policy, ACL, website hosting or signed artifact links are created by the app.
   Optionally add a short lifecycle expiration for demo artifacts.
4. Add a bucket policy denying insecure transport (`aws:SecureTransport=false`).
   Restrict the worker's PutObject permission to the repositories/ prefix and,
   optionally, require s3:x-amz-server-side-encryption=AES256. The adapter explicitly
   sends AES256 on both writes. Use separate operator read/delete access.

## Least-privilege credentials and Railway variables

For temporary Railway-to-AWS access, create a **dedicated API IAM principal** with
only sqs:SendMessage on the **one source queue ARN**, and no S3, DLQ, receive,
delete, admin or wildcard resource privileges. Use a separate worker principal.
Create scoped policies in IAM -> Policies -> Create policy, then assign them to
the relevant role/user. If a static key is necessary, IAM -> Users -> selected
dedicated user -> Security credentials -> Create access key; keep it in the
service's secret variables, never source, logs, queue or README. Rotate and delete
temporary credentials after the demo. Class 8 replaces static worker credentials
with **EKS IRSA**; this class does not implement IRSA.

| Principal | Runtime IAM actions | Resource scope |
| --- | --- | --- |
| API / enqueue recovery | sqs:SendMessage | Source queue ARN |
| Worker | sqs:ReceiveMessage, sqs:DeleteMessage | Source queue ARN |
| Worker | s3:PutObject | `arn:aws:s3:::YOUR_BUCKET/repositories/*` |
| Operator only | SQS inspect/redrive, S3 read/delete as needed | Named queues/bucket |

No runtime AWS queue/bucket create, listing, purge, DLQ receive or bucket admin
permission is needed. The worker sets visibility on receive and does not call
ChangeMessageVisibility. For customer-managed KMS, additional key grants would
be needed; this class uses SSE-SQS and SSE-S3 instead. LocalStack's dummy credentials
are not a substitute for a real-AWS IAM audit.

In Railway -> your project -> **API service** -> Variables configure:

| Variable | API | Worker Docker host |
| --- | --- | --- |
| DATABASE_URL (separate least-privilege DB role) | Required | Required |
| DATABASE_SSL=true, DATABASE_SSL_CA | Required | Required |
| AWS_REGION | Required | Required |
| AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (or approved role credentials) | API principal | Worker principal |
| AWS_SESSION_TOKEN | Only for temporary STS credentials | Only for temporary STS credentials |
| SQS_QUEUE_URL | Real source URL | Same source URL |
| S3_BUCKET | Not needed | Required |
| GITHUB_TOKEN | **Do not set** | Required |
| AWS_ENDPOINT_URL | **Unset** | **Unset** |
| PORT | Railway-injected | Not needed |

Make the official CA bundle available at DATABASE_SSL_CA in Railway via an
approved read-only mount or a small deployment-image layer copied from a verified
local CA bundle. The generic image does not silently download trust material at
startup. Set Railway healthcheck `/health`, root Dockerfile, and the image's
normal start command. Run Alembic once from the approved network before routing
webhooks to the Class 4 deployment. Switch the Railway deployment branch only
after DB/networking/secrets are configured; the Class 2 service can remain active
while you review this feature branch. No worker service is hosted on Railway.

## Worker runbook, DLQ recovery, teardown

Build/pull the same reviewed image on an approved Docker host with stable egress.
Use a permission-restricted worker env file and CA mount:

```sh
docker run -d --name ci-repair-worker --init --restart unless-stopped --stop-timeout 240 --env-file /secure/worker.env --mount type=bind,src=/secure/rds-ca.pem,dst=/certs/rds-ca.pem,readonly ci-repair:class4 python -m worker.main
docker logs -f ci-repair-worker
docker stop -t 240 ci-repair-worker
```

SIGTERM/SIGINT stop new receives and finish the current bounded attempt/poll.
Allow 240 seconds before force kill. A hard kill leaves the message unacknowledged;
the DB lease and SQS visibility expire. Database outages may delay retry recording.
Watch structured job_id/message_id/stage logs, retry_count, rows with no SQS ID,
age of queued/processing rows, DLQ visible count, and worker container liveness.
Never copy raw logs or credentials into operational log messages.

For inspection, open SQS -> DLQ -> Send and receive messages -> Poll for messages
with a separate operator identity; receiving there affects receive count/visibility.
Identify job_id from a valid envelope, inspect the database and sanitized error,
fix token/expired logs/S3/IAM/network problems, then use DLQ -> **Start DLQ redrive**
to source queue at a controlled rate. Operator redrive typically needs
sqs:StartMessageMoveTask, sqs:ReceiveMessage, sqs:DeleteMessage,
sqs:GetQueueAttributes on DLQ and sqs:SendMessage on source; add permissions for
customer-managed keys only if used. Malformed envelopes need correction/republication
by an operator; blindly redriving them repeats the failure. DLQ messages with no
valid job ID cannot update a DB row. Do not delete messages until evidence/recovery
is recorded. To abandon a valid job, mark failed explicitly through an approved
DB operator procedure; failed is terminal and subsequent deliveries acknowledge it.

Local teardown (deletes this disposable stack's PostgreSQL volume):

```sh
docker compose --profile test down -v --remove-orphans
```

Cloud teardown: disable the demo GitHub hook; stop the Docker worker and API;
retain any required evidence; remove demo S3 objects/bucket, source queue/DLQ,
RDS instance and unneeded final snapshots/backups, SG rules and temporary IAM
keys/principals. Delete only named demo resources after checking ownership/retention.

## References

- [GitHub workflow-run log download and token permissions](https://docs.github.com/en/rest/actions/workflow-runs#download-workflow-run-logs)
- [GitHub handling failed webhook deliveries](https://docs.github.com/en/webhooks/testing-and-troubleshooting-webhooks/redelivering-webhooks)
- [SQS visibility and at-least-once delivery](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [RDS PostgreSQL TLS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.SSL.html)
- [RDS security groups](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html)
- [Railway static outbound IPs](https://docs.railway.com/networking/static-outbound-ips)
- [LocalStack initialization hooks](https://docs.localstack.cloud/aws/customization/advanced/initialization-hooks/)
