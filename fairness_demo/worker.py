"""The Worker process: the control Worker, which starts and stops every run's own Workers.

Each demo run gets its own fairness-off and fairness-on Task Queues. The control Worker runs
the uploads and settings Workflows, plus one RunWorkers Workflow per run, whose Activity
creates that run's two document Workers in this process and stops them when the run ends
(see run_workers.py). The two document Workers are identical: same agent, same tools, same
number of Activity slots. Only the Priority each document run starts with differs.

Run with ``python -m fairness_demo.worker``.
"""

import asyncio
import logging

from temporalio.client import Client
from temporalio.service import RPCError
from temporalio.worker import Worker

from temporal_agent_harness.plugin import AgentHarnessPlugin

from . import config
from .activities import DOCUMENT_TOOLS
from .run_workers import RunWorkerActivities, RunWorkers
from .uploads import UploadActivities, UploadScenario, WorkerSettings

log = logging.getLogger("fairness_demo.worker")


def settings_workflow_id() -> str:
    return f"fairness-{config.TASK_QUEUE}-worker-settings"


async def connect() -> Client:
    # Payloads here are small; offloading to local disk would break a multi-host deploy.
    return await Client.connect(
        **config.client_connect_config(),
        plugins=[AgentHarnessPlugin(tools=DOCUMENT_TOOLS, large_payload_offload=None)],
    )


async def read_worker_slots(client: Client) -> int | None:
    """The shared slot setting, read with Describe so it costs no Action and needs no Worker."""
    try:
        description = await client.get_workflow_handle(settings_workflow_id()).describe()
    except RPCError:
        return None
    slots = (await description.memo()).get("worker_slots")
    return int(slots) if slots else None


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s level=%(levelname)s %(name)s %(message)s")
    client = await connect()
    uploads = UploadActivities(client)
    run_workers = RunWorkerActivities(client)
    control = Worker(
        client,
        task_queue=config.CONTROL_QUEUE,
        workflows=[UploadScenario, WorkerSettings, RunWorkers],
        activities=[uploads.upload_documents, run_workers.serve_run_queues],
        # Each active run holds one slot for its serve_run_queues Activity, for its lifetime.
        max_concurrent_activities=config.MAX_ACTIVE_RUNS + 50,
    )
    log.info("control worker polling queue=%s", config.CONTROL_QUEUE)
    await control.run()


if __name__ == "__main__":
    asyncio.run(main())
