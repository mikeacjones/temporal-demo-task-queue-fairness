# Temporal Task Queue Fairness demo

![Main view during the small-tenant trickle](docs/main-view.png)

One tenant's 2,000-document bulk upload lands on a shared AI document pipeline. Without fairness, every other tenant's documents wait behind it, at every step. With [Task Queue Fairness](https://docs.temporal.io/develop/task-queue-priority-fairness), they're done within seconds.

What's in it:

- Two identical Task Queues side by side, new for every run. `docs-fifo` has no fairness keys; `docs-fair` gives each tenant a fairness key and a weight from its plan.
- One [temporal-agent-harness](https://pypi.org/project/temporal-agent-harness/) agent run per document. The agent reads the document, then calls the extract, classify and summarize tools. Each tool is an Activity.
- [Priority and fairness](https://docs.temporal.io/develop/python/temporal-client#priority) set once, when the Workflow starts. The agent's Activities inherit the key and weight, so the agent code never sets them.
- [Update-With-Start](https://docs.temporal.io/develop/python/message-passing#update-with-start) for every document upload, Signal-With-Start for the presenter's controls, and [Visibility](https://docs.temporal.io/visibility) List for the live view.
- A presenter dashboard in the Temporal dark theme: a low-density main view, a details drawer, a weights act and a wrap-up card.

The model call is **simulated**. It sleeps for a fixed 1.5 s and returns canned tool calls, so every run takes the same time and needs no API keys. The UI shows a "Simulated model calls" badge.

## How it works

| Tenant | Plan | Fairness weight |
|---|---|---|
| Acme Corp | Enterprise | 5 |
| Birchwood Legal | Pro | 2 |
| Cedar Health | Starter | 1 |
| Dune Freight | Starter | 1 |

1. **Act 1: the noisy neighbour.** The presenter clicks **Bulk upload** and Acme uploads 2,000 documents to both queues at once, in a few seconds. Then **Start trickle** sends Birchwood, Cedar and Dune documents one at a time, every 10 to 14 seconds, to both queues. On `docs-fifo` they queue behind Acme's backlog. On `docs-fair`, the matching service gives each fairness key its share of the 8 Worker slots, so a Cedar document starts almost as soon as it arrives.
2. **Act 2: weights.** Every tenant uploads 500 documents to the fair queue. Each tenant's share of Activity starts follows its weight: 5 / 2 / 1 / 1. Fairness shares every dispatch, so the share counts all four Activities of each run, the model call and the three tool calls, not just the first. The presenter can edit the weights before the upload.
3. **Wrap-up.** A card compares the small tenants' p95 time to done with and without fairness, Acme's total time, and the shares from Act 2.

The headline number is a document's time to done: from upload to the end of its agent run. Under saturation each step of a run queues again, so a document that started can still be stuck: on `docs-fifo` its tool calls wait behind Acme's backlog too. Documents not done yet count their time so far. The details drawer also shows each tenant's wait, from upload to the start of the model call. The dashboard polls once a second. The API lists the run in the background, one listing at a time, and every poll gets the latest finished listing. A big run takes a few seconds to list, so its view can be a few seconds old; the header shows how old.

| Component | Command | What it does |
|---|---|---|
| Temporal | `scripts/dev-server.sh` | Dev server with fairness enabled and the demo's Search Attributes |
| Worker | `python -m fairness_demo.worker` | The control Worker. For each run it starts that run's two document Workers (`docs-fifo-…`, `docs-fair-…`) with 8 Activity slots each, and stops them when the run ends |
| API and UI | `python -m fairness_demo.api` | FastAPI app that serves the dashboard on port 8080 |

The `UploadScenario` Workflow runs the uploads, so a bulk upload or a trickle survives an API restart. Its `upload_documents` Activity starts each document with Update-With-Start, 500 documents per side at a time, and heartbeats its progress. The `WorkerSettings` Workflow holds the slot count, which a run reads when its Workers start.

Every run gets its own Task Queues, `docs-fifo-<session>-<run>` and `docs-fair-<session>-<run>`, and its own Workers for them, inside the one Worker process. The first presenter action starts the run's `RunWorkers` Workflow. Its one Activity, `serve_run_queues`, creates the run's two document Workers and heartbeats while they serve. Reset terminates the Workflow; the next heartbeat finds it gone, the Activity is cancelled, and the Workers shut down. A run that starts no Activity for 10 minutes stops its Workers too. If the Worker process dies, the heartbeat times out after 10 seconds and Temporal retries the Activity in the next process, which starts the Workers again.

## Prerequisites

- [Temporal CLI](https://docs.temporal.io/cli) 1.4 or later
- [uv](https://docs.astral.sh/uv/)

## Run it

In three terminals, from the repository root:

```bash
scripts/dev-server.sh
```

```bash
uv run python -m fairness_demo.worker
```

```bash
uv run python -m fairness_demo.api
```

Open http://127.0.0.1:8080. The Temporal UI is at http://localhost:8233.

| Control | Key | What it does |
|---|---|---|
| Bulk upload | `B` | Act 1: Acme uploads its bulk batch to both queues. Act 2: every tenant uploads to the fair queue |
| Start trickle | `T` | Small tenants upload one document at a time to both queues |
| Details | `D` | Opens the drawer: backlog, completions, tenants, Cedar's view (`1`–`4` switch tabs, `Esc` closes) |
| Reset | `R` | Starts a fresh run, terminates the old run's Workflows and applies a new slot count |
| Act 1 / Act 2 / Wrap-up | `1` `2` `3` | Switch acts when the drawer is closed |

**Pause motion** in the header stops the starfield and animations.

For a quick rehearsal, scale it down before starting the Worker and API:

```bash
export ACME_BULK_DOCS=40 TIERED_DOCS_PER_TENANT=60
export TRICKLE_INTERVAL_S=birch=2.5,cedar=3.5,dune=3
```

## Tuning

Set these in the environment of both the Worker and the API.

| Variable | Default | Meaning |
|---|---|---|
| `ACME_BULK_DOCS` | `2000` | Acme's bulk upload in Act 1 |
| `TRICKLE_DOCS` | `birch=12,cedar=9,dune=10` | Documents each small tenant uploads in the trickle |
| `TRICKLE_INTERVAL_S` | `birch=10,cedar=14,dune=12` | Seconds between each small tenant's uploads |
| `TIERED_DOCS_PER_TENANT` | `500` | Each tenant's upload in Act 2 |
| `WORKER_SLOTS` | `8` | Starting Activity slots per queue. The dashboard changes it on Reset, up to 32 |
| `SIM_MODEL_SECONDS` | `1.5` | Simulated model call |
| `SIM_TOOL_SECONDS` | `0.5` | Each simulated tool |
| `TASK_QUEUE` | `docs` | Base name: each run's `<base>-fifo-<session>-<run>` and `<base>-fair-<session>-<run>`, and the shared `<base>-control` |
| `MAX_ACTIVE_RUNS` | `20` | Runs whose Workers one Worker process hosts at once. Each holds a control Worker Activity slot for its lifetime |
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, `TEMPORAL_TLS*` | `localhost:7233`, `default` | Temporal connection. The demo reads env vars only and ignores CLI profile files |
| `API_HOST`, `PORT` | `127.0.0.1`, `8080` | API bind address |
| `SESSION_SECRET` | random per process | Signs the session cookie. Set it when the API runs as more than one replica or must keep sessions across restarts |
| `TEMPORAL_UI_URL` | `http://localhost:8233` | Target of the dashboard's Temporal UI links |

## Temporal Cloud

- Turn on Fairness for the namespace, and add the Search Attributes `DocSession` (Keyword), `DocTenant` (Keyword) and `DocStartedAt` (Datetime).
- Each document costs about 13 Actions: 2 for Update-With-Start, 4 Activities, 1 Search Attribute upsert, and 6 Signals that the agent harness sends to publish each tool's start and end events. A default Act 1 is about 4,060 documents across both queues, about 53,000 Actions. The bulk upload's starts arrive in a burst; if they pass the Namespace's Actions-per-second limit, the client retries the throttled ones. A default Act 2 is 2,000 documents, about 26,000.
- The dashboard reads Visibility List and Describe, which aren't Actions. It runs no Queries.
- Act 2's shares come from the run's Workers, the only place that sees every Activity start. `serve_run_queues` counts starts per side and tenant and puts the last 90 seconds in its heartbeat details, which the dashboard reads with Describe. The SDK sends a heartbeat about every 8 seconds, about 8 Actions a minute per running run.

## Catalog readiness

- **Sessions.** Each browser gets a signed `fairness_session` cookie. The tab picks a run ID, kept in `sessionStorage`. Every Workflow ID, List query, upload and reset is scoped to `fairness-<TASK_QUEUE>-<session>-<run>`.
- **Routes.** `/` serves the dashboard. `/api/settings`, `/api/runs/{run}/view` and `POST /api/runs/{run}/{bulk,trickle,tiered,reset}` are public. There are no internal routes.
- **Smoke check.** `GET /healthz` checks the Temporal connection without changing any state.
- **Build.** One `Dockerfile` builds both processes. The API is the default command; run the Worker with `python -m fairness_demo.worker`.
- **Secrets.** `SESSION_SECRET` and the Temporal credentials.

## Notes

- **Workers per run.** Each run gets its own document Workers and its own slots, so two people running the demo at once don't slow each other down. They share the Worker process, which is fine for simulated model calls that only sleep. The slot setting is global and applies to runs that start after it changes.
- **Late completions on the FIFO side.** On `docs-fifo`, a document's tool Activities queue behind the whole remaining backlog, not just its model call. Completions lag starts, which the completions chart shows.
- **Act 1 uses plan weights.** Act 1 already runs with weights 5 / 2 / 1 / 1. The small tenants still start quickly because each one has documents waiting only now and then.
- **Start time.** The Workflow records `DocStartedAt` when the model call returns. For small-tenant documents that are still running, the API reads the model call's start time with Describe, so their wait is exact.
- **Closing the agent.** temporal-agent-harness 0.7.0 exposes close only as a Signal, and a Workflow can't Signal itself. The agent calls the runner's internal `_handle_close()` after the document is processed.
- **Reset.** Reset terminates the old run's Workflows with one batch operation, including its `RunWorkers`, which stops that run's Workers. The terminated documents' Activity tasks stay in the old run's Task Queues, which nothing polls any more, so they can't hold up the next run.
- **Not yet verified.** A default Act 1 was run locally through the bulk upload and the whole trickle, but not until Acme's backlog drained. A default Act 2 hasn't been run.
