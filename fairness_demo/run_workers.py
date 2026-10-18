"""One run's document Workers, started for the run and stopped with it.

Each demo run gets its own fairness-off and fairness-on Task Queues and its own Workers for
them, inside the long-running Worker process. A run never inherits another run's backlog:
Reset terminates Workflows, but their queued Activity tasks stay in a Task Queue until they
are dispatched and dropped, and there is no way to purge them. Fresh queues per run avoid
that, and runs going at the same time each get their own slots.

RunWorkers is a per-run Workflow whose one Activity, serve_run_queues, owns the two Workers.
The Activity heartbeats while it serves. Reset terminates the Workflow, the next heartbeat
finds it gone, the Activity is cancelled, and its Workers shut down. If the Worker process
dies, the heartbeat times out and Temporal retries the Activity in the next process, which
starts the Workers again.

The heartbeat also carries the run's Activity start counts for Act 2: fairness shares every
dispatch, the model call and each tool call, and only these Workers see every Activity
start. The dashboard reads the counts from the pending Activity with Describe.
"""

import asyncio
import time
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from temporalio.client import Client
    from temporalio.worker import ActivityInboundInterceptor, ExecuteActivityInput, Interceptor, PollerBehaviorAutoscaling, Worker

    from . import config
    from .activities import call_model
    from .document_agent import DocumentAgent

# Kept a little longer than the dashboard's 60-second window, in 5-second buckets.
BUCKET_S = 5
WINDOW_S = 90
# A lost Worker process is noticed after this long; the SDK heartbeats at 80% of it.
HEARTBEAT_TIMEOUT = timedelta(seconds=10)
# A run whose documents have started no Activity for this long is over.
IDLE_S = 600
RUN_LIFETIME = timedelta(hours=2)


def run_workers_workflow_id(run_key: str) -> str:
    return f"{run_key}-workers"


def run_task_queues(run_key: str) -> tuple[str, str]:
    """The run's fairness-off and fairness-on Task Queues: the base names plus the run's
    session and run ID, the last two parts of its key."""
    suffix = "-".join(run_key.rsplit("-", 2)[1:])
    return f"{config.FIFO_QUEUE}-{suffix}", f"{config.FAIR_QUEUE}-{suffix}"


@dataclass
class RunWorkersInput:
    fifo_queue: str
    fair_queue: str
    worker_slots: int


@workflow.defn
class RunWorkers:
    """Keeps one run's document Workers running until the run is reset or goes idle."""

    @workflow.run
    async def run(self, input: RunWorkersInput) -> None:
        await workflow.execute_activity_method(
            RunWorkerActivities.serve_run_queues,
            input,
            start_to_close_timeout=RUN_LIFETIME,
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            # Retried for as long as the run lasts, so a restarted process picks the run up.
            retry_policy=RetryPolicy(maximum_interval=timedelta(seconds=10)),
        )


class StartCounter(Interceptor):
    """Counts each Activity attempt as it starts, per side, tenant and 5-second bucket."""

    def __init__(self, sides: dict[str, str]) -> None:
        self._sides = sides  # Task Queue -> "off" or "on"
        self.counts: Counter[tuple[int, str, str]] = Counter()
        self.last_start = time.monotonic()

    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        return _CountStart(next, self)

    def record(self, info: activity.Info) -> None:
        self.last_start = time.monotonic()
        # Document Workflow IDs end in "-<side>-<tenant>-<n>"; see documents.py.
        tenant = info.workflow_id.rsplit("-", 2)[1] if info.workflow_id else ""
        bucket = int(info.started_time.timestamp()) // BUCKET_S * BUCKET_S
        self.counts[(bucket, self._sides[info.task_queue], tenant)] += 1

    def window(self) -> list[list]:
        """[bucket, side, tenant, count] rows for the last WINDOW_S seconds."""
        oldest = time.time() - WINDOW_S
        for key in [k for k in self.counts if k[0] < oldest]:
            del self.counts[key]
        return [[b, side, tenant, n] for (b, side, tenant), n in sorted(self.counts.items())]


class _CountStart(ActivityInboundInterceptor):
    def __init__(self, next: ActivityInboundInterceptor, counter: StartCounter) -> None:
        super().__init__(next)
        self._counter = counter

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        self._counter.record(activity.info())
        return await super().execute_activity(input)


class RunWorkerActivities:
    def __init__(self, client: Client) -> None:
        self._client = client

    @activity.defn
    async def serve_run_queues(self, input: RunWorkersInput) -> None:
        """Run the run's two document Workers until cancelled, idle, or one of them fails."""
        counter = StartCounter({input.fifo_queue: "off", input.fair_queue: "on"})
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
        running = [asyncio.create_task(w.run()) for w in workers]
        activity.logger.info(
            "run workers polling queues=%s,%s activity_slots=%d",
            input.fifo_queue, input.fair_queue, input.worker_slots,
        )
        try:
            while time.monotonic() - counter.last_start < IDLE_S:
                activity.heartbeat(counter.window())
                done, _ = await asyncio.wait(running, timeout=1)
                for task in done:
                    task.result()  # a Worker that stopped on its own fails the attempt
            activity.logger.info("run workers idle; stopping queues=%s,%s", input.fifo_queue, input.fair_queue)
        finally:
            # Cancelled by Reset, idle, or failed: stop polling this run's queues either way.
            await asyncio.gather(*(w.shutdown() for w in workers), return_exceptions=True)
            await asyncio.gather(*running, return_exceptions=True)
