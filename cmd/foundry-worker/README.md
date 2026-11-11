# Azure Foundry background worker

A Python hosted agent that wakes a Temporal worker and immediately returns
HTTP **202** with `{"status":"accepted"}`. It does not wait for Temporal to
connect and does not submit a workflow. Use the existing Go workflow starter
to submit work.

Each server process runs one worker child at a time. Repeated invocations reuse
the current window without extending it. At 295 seconds from acceptance, the
server sends SIGTERM; at 300 seconds it kills any remaining child and reaps it.
Startup and shutdown count toward the limit. The HTTP server stays available,
and a subsequent invocation starts a fresh window. Early worker failures are
logged and require another invocation to restart. Server shutdown also stops
and reaps the child; requests received during shutdown return HTTP 503.

This is a process-local demonstration: each Foundry replica can run its own
worker. Container termination can end background work early. Expiring a worker
does not cancel Temporal workflows; unfinished work needs another compatible
worker, and interrupted activities are subject to their Temporal retry policy.

## Workflow behavior

Like the Go examples, dynamic definitions accept any workflow/activity name and
ignore arguments. The workflow logs its name, executes an activity with that
same name and a one-minute start-to-close timeout, and returns `"success"`.
The activity logs its name, asynchronously sleeps for 15 seconds, and returns
`"Success"`.

Workers use pinned deployment versioning, disabled eager activities, and
workflow/activity concurrency limits of two. A pinned workflow needs its
original deployment version to continue, including after a worker window ends.

## Run locally

Requires Python 3.13, [uv](https://docs.astral.sh/uv/), Go, and the Temporal CLI.
From the repository root, start a development server in one terminal:

```bash
temporal server start-dev
```

Start the HTTP server in another terminal:

```bash
cd cmd/foundry-worker
uv sync --frozen
uv run --frozen python main.py
```

Wake the worker from a third terminal:

```bash
curl -fsS http://localhost:8088/readiness
curl -i -X POST http://localhost:8088/invocations
```

Wait for the worker to connect, then route new workflows to its deployment
version. If the CLI reports the version is not registered yet, retry after a
few seconds. This is an explicit routing change, just as for the Go examples:

```bash
temporal worker deployment set-current-version \
  --deployment-name test --build-id v1 --yes
```

Submit workflows from the repository root:

```bash
go run ./cmd/workflow-starter --workflow-type noop --count 2
temporal workflow show --workflow-id workflow-starter-noop-0
```

Try another name with `--workflow-type another-example`. The workflow result is
`"success"` after its activity completes. The readiness endpoint checks the HTTP
server; a 202 acknowledgment means startup was scheduled, not that Temporal is
reachable. Connection errors and child exits appear in the server's logs.

## Configuration

The child inherits configuration from the server environment.

| Variable | Default | Purpose |
| --- | --- | --- |
| `HOST_PORT` | `127.0.0.1:7233` | Temporal gRPC endpoint |
| `NAMESPACE` | `default` | Temporal namespace |
| `TQ_NAME` | `worker-versioning-sample` | Task queue |
| `DEPLOYMENT_NAME` | `test` | Temporal worker deployment |
| `BUILD_ID` | `v1` | Temporal worker deployment build ID |
| `ENABLE_TLS` | empty | Any nonempty value enables TLS with certificate verification |
| `API_KEY` | empty | Direct Temporal API key, not a secret identifier |
| `PORT` | `8088` | HTTP listen port |

For Temporal Cloud, set `HOST_PORT`, `NAMESPACE`, `ENABLE_TLS=1`, and `API_KEY`.
The hosted container must have network access to the Temporal endpoint.
Localhost inside Azure refers to the hosted container, not your computer.

## Container

Build from this directory. Foundry requires a Linux AMD64 image:

```bash
docker build --platform linux/amd64 -t temporal-foundry-worker:v1 .
docker run --rm -p 8088:8088 \
  -e HOST_PORT=host.docker.internal:7233 \
  temporal-foundry-worker:v1
```

`host.docker.internal` works with Docker Desktop. On Linux, configure the
host-gateway mapping or supply another reachable Temporal address. Invoke the
container with the same curl commands used above. The image runs as a nonroot
user and sets `PYTHONNOUSERSITE=1` before Python starts.

## Deploy to Foundry

The included `azure.yaml` declares a hosted agent using the **invocations**
protocol, version 2.0.0. No LLM or model deployment is needed. Use Azure Developer
CLI 1.27.1 or newer and its `azure.ai.agents` extension 1.0.0-beta.9 or newer,
with access to deploy Foundry hosted agents.

From this directory, sign in and create an azd environment:

```bash
azd auth login
azd env new temporal-worker-example
azd env set HOST_PORT "<namespace>.<region>.api.temporal.io:7233"
azd env set NAMESPACE "<namespace>"
azd env set TQ_NAME "worker-versioning-sample"
azd env set DEPLOYMENT_NAME "test"
azd env set BUILD_ID "v1"
azd env set ENABLE_TLS "1"
azd env set API_KEY "<temporal-api-key>"
azd up
azd ai agent show
```

The `.azure` environment directory is ignored by Git. For an existing deployment,
use `azd deploy` for code changes. These commands create/update Azure resources;
this example does not deploy automatically.

Invoke the deployed agent's invocations endpoint using an authenticated client
and the endpoint returned by your deployment. It accepts an empty body or any
payload and returns the same 202 acknowledgment as the local server. Configure
Temporal deployment routing to `test.v1` using a Temporal CLI profile with access
to that namespace, then submit workflows on `worker-versioning-sample`.

See Microsoft's [hosted-agent runtime contract](https://learn.microsoft.com/en-us/azure/foundry/agents/concepts/hosted-agent-contract)
and [deployment guide](https://learn.microsoft.com/en-us/azure/foundry/agents/how-to/deploy-hosted-agent)
for platform setup and authenticated invocation details.

## Tests

From this directory:

```bash
uv run --frozen pytest -q
TEMPORAL_TEST_ADDRESS=127.0.0.1:7233 uv run --frozen pytest -q
```

Unit tests exercise immediate acknowledgment, concurrent invocations, a fixed
deadline, SIGTERM/SIGKILL cleanup, restart, spawn failure, and server shutdown
with short injected windows. The optional integration test creates a unique
task queue and deployment on a local dev server, routes its version, and checks
arbitrary workflow names, ignored input arguments, the 15-second activity,
activity timeout, and both result strings.

For a full lifecycle smoke test, wake the server, complete a workflow, and leave
it running for five minutes. Confirm the child exits, `/readiness` still returns
200, and another invocation starts a new worker.
