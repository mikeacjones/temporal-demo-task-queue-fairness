# Runbook: Task Queue Fairness demo

This runbook is for presenting the demo. The [README](README.md) explains what it is and how it's built.

## What it shows

One tenant's 2,000-document bulk upload lands on a shared AI document pipeline. Without fairness, every other tenant's documents wait behind it. With Task Queue Fairness, they start within seconds, and each tenant's share of the pipeline follows the weight of its plan.

The demo makes two problems visible:

- **A noisy neighbor stalls everyone else.** Acme's burst fills the shared queue, and a Cedar Health document uploaded a few seconds later waits minutes for its turn. Act 1 shows it, and fairness fixes it on the right-hand side.
- **Paying customers expect a bigger share, without their own infrastructure.** Act 2 gives each plan a weight on one shared queue and one Worker pool, and the dashboard shows the share each tenant actually got.

## Before you start

- Install the [Temporal CLI](https://docs.temporal.io/cli) 1.4 or later and [uv](https://docs.astral.sh/uv/).
- Clone the repo. The first `uv run` installs the dependencies.
- To run against Temporal Cloud instead of the local dev server:
  1. Turn on Fairness for the Namespace.
  2. Add the Search Attributes `DocSession` (Keyword), `DocTenant` (Keyword) and `DocStartedAt` (Datetime).
  3. Set `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` and `TEMPORAL_API_KEY` in the Worker's and the API's terminals. The demo never reads your CLI profile.
  4. Expect about 53,000 Actions for a default Act 1 and 26,000 for Act 2. The README's [Temporal Cloud](README.md#temporal-cloud) section breaks that down.

## Start it

In three terminals, from the repository root:

1. `scripts/dev-server.sh` (skip this on Temporal Cloud)
2. `uv run python -m fairness_demo.worker`
3. `uv run python -m fairness_demo.api`

Open http://127.0.0.1:8080. A healthy start shows the four tenants with no documents, a "Simulated model calls" badge, and the **Noisy neighbor** act selected. The Worker terminal logs `control worker polling queue=docs-control`. The Temporal UI is at http://localhost:8233.

To rehearse in a few minutes instead of a full run, scale the demo down in the Worker's and the API's terminals before starting them:

```bash
export ACME_BULK_DOCS=40 TIERED_DOCS_PER_TENANT=60
export TRICKLE_INTERVAL_S=birch=2.5,cedar=3.5,dune=3
```

## The walkthrough

Each act takes two to four minutes. Press `D` at any point to open the details drawer, and `Esc` to close it.

### 1. Acme's bulk upload fills both queues

1. On **Noisy neighbor** (`1`), press **Start bulk upload** (`B`).
2. Acme's 2,000 documents land on both sides within a few seconds, one square per document. The left side has no fairness keys. The right side gives each tenant its own key. Both sides have the same 8 Worker slots.
3. Say: "Acme just uploaded 2,000 documents to a shared pipeline. Both sides run the same agent on the same Workers. The only difference is one setting when each document starts."

### 2. The small tenants upload, and only one side makes them wait

1. Press **Start small-tenant trickle** (`T`). Birchwood, Cedar and Dune each upload one document every 10 to 14 seconds, to both sides.
2. On **Fairness off**, their documents join the back of the line behind Acme. On **Fairness on**, each one starts almost as soon as it arrives.
3. Open the details drawer (`D`) and pick **Cedar's view** (`4`). Each Cedar document shows its wait on both sides.
4. Say: "Same upload, same Workers. On the left, Cedar waits behind Acme's whole backlog. On the right, the matching service takes turns between tenants, so Cedar's document starts in seconds."
5. For proof, click **Temporal UI ↗** at the bottom of the drawer. It lists this run's documents; open a Cedar run on each side and compare when its model call was scheduled and when it started.

You don't need to wait for Acme's backlog to drain; at the default size the left side takes more than ten minutes. Acme's own finish time is about the same on both sides, which the wrap-up shows.

### 3. Paid tiers get their share of one shared pool

1. Switch to **Paid tiers** (`2`). The plan weights read 5 / 2 / 1 for Enterprise, Pro and Starter. Change them now if you want to show a different split. A weight applies when a document starts, so it can't change during the upload.
2. Press **Start tiered uploads** (`B`). This starts a fresh run, and every tenant uploads 500 documents to the fairness-on side at once.
3. After about a minute, **Actual share of Activities started** settles near **Expected share from weights**: Acme about 56%, Birchwood 22%, Cedar and Dune 11% each.
4. Say: "One queue, one Worker pool. A plan is a weight, so a new Enterprise customer doesn't need its own queue or its own Workers."

### 4. Wrap-up

1. Switch to **Wrap-up** (`3`).
2. The card compares the small tenants' p95 time to done with and without fairness, Acme's total time on each side, and the shares from Act 2.
3. **Open these runs in Temporal UI ↗** lists the run's documents for anyone who wants to check. Press **Run it again** to reset and go back to Act 1.

## Show it in code

All links point at commit [`862b989`](https://github.com/mikeacjones/temporal-demo-task-queue-fairness/commit/862b9894b905a8a3bb39d3567f3c3d75def1b43f). Open a file locally with `code -g <path>:<line>`.

### Give each tenant a fairness key

**When to use it:** after beat 2, or when someone asks what it took to make Cedar's document start.

**Where it is:** [`fairness_demo/tenants.py` lines 28–41](https://github.com/mikeacjones/temporal-demo-task-queue-fairness/blob/862b9894b905a8a3bb39d3567f3c3d75def1b43f/fairness_demo/tenants.py#L28-L41) (`code -g fairness_demo/tenants.py:28`), used in [`fairness_demo/uploads.py` lines 82–98](https://github.com/mikeacjones/temporal-demo-task-queue-fairness/blob/862b9894b905a8a3bb39d3567f3c3d75def1b43f/fairness_demo/uploads.py#L82-L98) (`code -g fairness_demo/uploads.py:82`)

```python
def fairness_key(tenant_id: str) -> str:
    return f"tenant-{tenant_id}"


def document_priority(tenant_id: str, fair: bool, weight: float) -> Priority:
    """The Priority a document's agent run starts with.

    Every Activity and Workflow Task the run schedules inherits these values, so the agent
    code never sees or sets them. The fairness-off queue gets no key: all its tasks share
    the implicit empty key and dispatch first in, first out.
    """
    if not fair:
        return Priority.default
    return Priority(fairness_key=fairness_key(tenant_id), fairness_weight=weight)
```

```python
start = WithStartWorkflowOperation(
    DocumentAgent.run,
    args=[AgentConfig()],
    id=workflow_id,
    task_queue=batch.fair_queue if fair else batch.fifo_queue,
    # The tenant's fairness key and plan weight. The agent never sets them; every
    # Activity it schedules inherits them from here.
    priority=document_priority(batch.tenant_id, fair, batch.weight),
    # ...
)
```

**What you need to understand:**

- Fairness is one `Priority` value passed when the Workflow starts: a key (`tenant-acme`, `tenant-cedar`) and a weight. That is the whole difference between the two sides.
- Each fairness key gets its own virtual queue inside the one Task Queue. When several keys have a backlog, the matching service dispatches from them in turn, weighted by their weights.
- The left side passes `Priority.default`. Its Tasks share the implicit empty key, so they dispatch first in, first out.
- A new tenant is a new key string. It needs no new Task Queue, no new Worker, and no deploy. Use a stable ID for the key, not a display name; keys are limited to 64 bytes.
- Fairness has to be enabled for the Namespace. See [Turn fairness on](#turn-fairness-on).

**Docs:** [Task Queue Priority and Fairness](https://docs.temporal.io/develop/task-queue-priority-fairness), [Python: Priority](https://docs.temporal.io/develop/python/temporal-client#priority)

### The agent code never mentions tenants

**When to use it:** right after the previous entry, or when someone asks whether every Activity call has to pass the key.

**Where it is:** [`fairness_demo/document_agent.py` lines 55–67](https://github.com/mikeacjones/temporal-demo-task-queue-fairness/blob/862b9894b905a8a3bb39d3567f3c3d75def1b43f/fairness_demo/document_agent.py#L55-L67) (`code -g fairness_demo/document_agent.py:55`)

```python
@agent.accepts
async def process(self, document: Document) -> ProcessedDocument:
    """Read an uploaded document, then extract its fields, classify it, and summarize it."""
    plan = await workflow.execute_activity(
        call_model,
        ModelRequest(
            document_id=document.document_id,
            file_name=document.file_name,
            pages=document.pages,
        ),
        # The simulated call takes a fixed 1-2 s; a real model call would need longer.
        start_to_close_timeout=timedelta(seconds=60),
    )
```

**What you need to understand:**

- The model call and the three tool calls are scheduled with no `priority` argument. They inherit the key and weight from the Workflow, and so would any Child Workflow.
- The inheritance is why fairness holds at every step. Under load each Activity queues again; on the left side a document that has started can still wait behind Acme for its tool calls.
- An Activity can override the inherited values by passing its own `priority` in its options. This demo doesn't need to.

### Paid tiers are a weight

**When to use it:** during beat 3, or when someone asks how Enterprise gets more.

**Where it is:** [`fairness_demo/tenants.py` lines 16–25](https://github.com/mikeacjones/temporal-demo-task-queue-fairness/blob/862b9894b905a8a3bb39d3567f3c3d75def1b43f/fairness_demo/tenants.py#L16-L25) (`code -g fairness_demo/tenants.py:16`). The weights you edit on screen arrive in [`fairness_demo/api.py` lines 295–301](https://github.com/mikeacjones/temporal-demo-task-queue-fairness/blob/862b9894b905a8a3bb39d3567f3c3d75def1b43f/fairness_demo/api.py#L295-L301).

```python
TENANTS = [
    Tenant("acme", "Acme Corp", "Enterprise", 5),
    Tenant("birch", "Birchwood Legal", "Pro", 2),
    Tenant("cedar", "Cedar Health", "Starter", 1),
    Tenant("dune", "Dune Freight", "Starter", 1),
]
TENANTS_BY_ID = {t.id: t for t in TENANTS}
SMALL_TENANTS = ["birch", "cedar", "dune"]

PLAN_WEIGHTS = {"Enterprise": 5.0, "Pro": 2.0, "Starter": 1.0}
```

**What you need to understand:**

- Weights are relative. With all four tenants backlogged, 5 / 2 / 1 / 1 gives Acme 5 of every 9 dispatches, about 56%. A tenant with nothing waiting leaves its share to the others, so no capacity sits idle.
- A weight is read when a Task is scheduled. Changing it doesn't reorder Tasks already in the backlog, which is why the dashboard takes new weights before the upload.
- Weights can range from 0.001 to 1000; the API rejects values outside that range. In production, weights for a small set of keys can also be set in Task Queue configuration, which overrides the value in code and changes without a redeploy. This demo sets them only in code.
- The share counts dispatches, not documents and not Worker time. A tenant whose Activities run longer would use more Worker time than its share suggests.
- Fairness is best effort, and it isn't a rate limit or isolation. For a per-tenant cap, use [fairness key rate limits](https://docs.temporal.io/develop/task-queue-priority-fairness#set-rate-limits-at-the-task-queue-level). For hard isolation, a tenant still needs its own Task Queue and Workers.

### Both sides run the same Workers

**When to use it:** when someone suspects the right side has more capacity.

**Where it is:** [`fairness_demo/run_workers.py` lines 126–142](https://github.com/mikeacjones/temporal-demo-task-queue-fairness/blob/862b9894b905a8a3bb39d3567f3c3d75def1b43f/fairness_demo/run_workers.py#L126-L142) (`code -g fairness_demo/run_workers.py:126`)

```python
workers = [
    Worker(
        self._client,
        task_queue=queue,
        workflows=[DocumentAgent],
        activities=[call_model],
        # The constraint the demo is about: a fixed number of Activities at once.
        max_concurrent_activities=input.worker_slots,
        # Keep every in-flight document run cached, so Workflow Tasks don't replay.
        max_cached_workflows=config.ACME_BULK_DOCS + 1000,
        interceptors=[counter],
        # Scale pollers with the backlog instead of a fixed 5 per task type.
        workflow_task_poller_behavior=PollerBehaviorAutoscaling(),
        activity_task_poller_behavior=PollerBehaviorAutoscaling(),
    )
    for queue in (input.fifo_queue, input.fair_queue)
]
```

**What you need to understand:**

- One loop builds both Workers with the same settings. Only the Task Queue name differs.
- `max_concurrent_activities` (8 by default) is what creates the backlog. Fairness only changes which Task goes next when Tasks are waiting; with enough slots for everyone, both sides would look the same.
- You can change **Worker slots per side** at the bottom of the details drawer. It applies to both sides after **Reset**.

### Turn fairness on

**When to use it:** when someone asks what they need to enable.

**Where it is:** [`scripts/dev-server.sh` lines 6–13](https://github.com/mikeacjones/temporal-demo-task-queue-fairness/blob/862b9894b905a8a3bb39d3567f3c3d75def1b43f/scripts/dev-server.sh#L6-L13) (`code -g scripts/dev-server.sh:6`)

```bash
# One partition per Task Queue keeps dispatch order easy to see on a laptop. Temporal
# Cloud partitions Task Queues, which makes fairness ratios less exact (still best effort).
exec temporal --disable-config-file server start-dev \
  --ip 127.0.0.1 \
  --namespace default \
  --dynamic-config-value matching.enableFairness=true \
  --dynamic-config-value matching.numTaskqueueReadPartitions=1 \
  --dynamic-config-value matching.numTaskqueueWritePartitions=1 \
```

**What you need to understand:**

- On the dev server, fairness is a dynamic config flag. On Temporal Cloud it's a toggle on the Namespace. Nothing in the application code turns it on.
- The single-partition setting makes the laptop's ratios exact. On Cloud, Task Queues are partitioned, so the Act 2 shares land close to the weights rather than on them.
- Turning fairness on doesn't reorder a backlog that already exists. That backlog drains in its original order, and fair dispatch applies to new Tasks.

### How the share chart is measured

**When to use it:** when someone asks whether the Act 2 chart shows real numbers.

**Where it is:** [`fairness_demo/run_workers.py` lines 82–115](https://github.com/mikeacjones/temporal-demo-task-queue-fairness/blob/862b9894b905a8a3bb39d3567f3c3d75def1b43f/fairness_demo/run_workers.py#L82-L115) (`code -g fairness_demo/run_workers.py:82`)

```python
class StartCounter(Interceptor):
    """Counts each Activity attempt as it starts, per side, tenant and 5-second bucket."""

    # ...

    def record(self, info: activity.Info) -> None:
        self.last_start = time.monotonic()
        # Document Workflow IDs end in "-<side>-<tenant>-<n>"; see documents.py.
        tenant = info.workflow_id.rsplit("-", 2)[1] if info.workflow_id else ""
        bucket = int(info.started_time.timestamp()) // BUCKET_S * BUCKET_S
        self.counts[(bucket, self._sides[info.task_queue], tenant)] += 1
```

**What you need to understand:**

- A Worker interceptor counts every Activity as it starts on the run's Workers, which are the only place that sees every dispatch. The chart is a count of real dispatches, not a calculation from the weights.
- It counts all four Activities of each document, because fairness shares every dispatch, not only the first.
- The counts travel in the serving Activity's heartbeat, and the API reads them with Describe. Describe isn't a billable Action, and the dashboard runs no Queries.
- This counter is demo instrumentation. A production system doesn't need it.

### What's demo-only

When someone asks what they'd change for their own system, these are the parts to leave out:

- **The model and the tools are simulated.** [`fairness_demo/activities.py` lines 1–6](https://github.com/mikeacjones/temporal-demo-task-queue-fairness/blob/862b9894b905a8a3bb39d3567f3c3d75def1b43f/fairness_demo/activities.py#L1-L6): each Activity sleeps for a fixed time and returns canned output. Replace those bodies with real calls and nothing else changes.
- **Each run gets its own pair of Task Queues and Workers.** [`fairness_demo/run_workers.py` lines 1–7](https://github.com/mikeacjones/temporal-demo-task-queue-fairness/blob/862b9894b905a8a3bb39d3567f3c3d75def1b43f/fairness_demo/run_workers.py#L1-L7): that keeps concurrent presenters apart and lets Reset start clean. A customer runs one fairness-on Task Queue and one Worker pool.
- **The fairness-off side exists only for comparison.**

### If someone asks about priority

The demo uses fairness only; it never sets a priority key. Priority goes in the same `Priority` value, and it's checked first: a lower `priority_key` dispatches ahead of a higher one, and fairness balances tenants within each level. The keys run from 1 to 5 by default, and unset means 3. Rush documents, for example, could start with:

```python
# Not in this repo: how a rush document would jump ahead of every tenant's backlog.
Priority(priority_key=1, fairness_key=fairness_key(tenant_id), fairness_weight=weight)
```

See [Task Queue Priority](https://docs.temporal.io/develop/task-queue-priority-fairness#task-queue-priority) and the [Priority pattern](https://docs.temporal.io/design-patterns/priority-task-queues).

## Reset and tear down

- **Between runs:** press **Reset** (`R`). The screen switches to a fresh run straight away. In the background, the API stops the old run's Workers and terminates its Workflows with one batch operation. **Start bulk upload** and **Start tiered uploads** also reset first when the screen holds a run from the other act.
- **To change the slot count:** set **Worker slots per side** in the details drawer, then press **Reset**.
- **To stop:** press **Reset** first, so the run's documents are terminated instead of left open, then stop the API, the Worker, and the dev server with `Ctrl-C`.
- **If you close the tab without resetting:** the run's Workers stop after 10 minutes with no Activity starts, and its upload Workflow stops accepting actions after 2 hours. On Temporal Cloud, reset before you leave.

## When something goes wrong

| What you see | Likely cause | Fix |
|---|---|---|
| Upload buttons say accepted, but no squares appear | The Worker isn't running; it runs the uploads | Start `uv run python -m fairness_demo.worker` |
| The right side makes small tenants wait as long as the left | Fairness isn't enabled on the server | Start the dev server with `scripts/dev-server.sh`, or turn on the Namespace's Fairness toggle on Cloud, then press **Reset** |
| The dashboard never loads its first view, or the API logs list errors | The Search Attributes are missing, or the API can't reach Temporal | Use `scripts/dev-server.sh`, which creates them; on Cloud, add them. Check `GET /healthz` |
| The header shows the view is several seconds old | A big run takes a few seconds to list | Expected during Act 1 at full size |
| Act 2 shares are far from the weights | Fewer than 60 seconds have passed, or Task Queue partitioning on Cloud | Wait a minute; on Cloud, describe the shares as close to the weights |
| Everything takes too long to rehearse | Default sizes are for a full presentation | Use the scaled-down settings in [Start it](#start-it) |

## Keeping this runbook current

The code links point at a fixed commit. When a linked file changes, update its link, line numbers, and snippet together. Before an event, list the linked files that have changed:

```sh
grep -oE 'blob/[0-9a-f]{40}/[^#)]+' RUNBOOK.md | sort -u | while IFS=/ read -r _ sha file; do
  git diff --quiet "$sha" HEAD -- "$file" || echo "changed since ${sha:0:7}: $file"
done
```
