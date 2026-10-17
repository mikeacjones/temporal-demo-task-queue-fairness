"""Workers for both document Task Queues, plus the uploads and settings Workflows.

The fairness-off and fairness-on Workers are identical: same agent, same tools, same number
of Activity slots. Only the Priority each document run starts with differs.

Run with ``python -m fairness_demo.worker``.
"""

import asyncio
import logging

from temporalio.client import Client
from temporalio.service import RPCError
from temporalio.worker import Worker

from temporal_agent_harness.plugin import AgentHarnessPlugin

from . import config
from .activities import DOCUMENT_TOOLS, call_model
from .document_agent import DocumentAgent
from .uploads import UploadActivities, UploadScenario, WorkerSettings

log = logging.getLogger("fairness_demo.worker")

SETTINGS_POLL_S = 2.0


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


def document_workers(client: Client, slots: int) -> list[Worker]:
    return [
        Worker(
            client,
            task_queue=queue,
            workflows=[DocumentAgent],
            activities=[call_model],
            # The constraint the demo is about: a fixed number of Activities at once.
            max_concurrent_activities=slots,
            # Keep every in-flight document run cached, so Workflow Tasks don't replay history.
            max_cached_workflows=config.ACME_BULK_DOCS + 1000,
        )
        for queue in (config.FIFO_QUEUE, config.FAIR_QUEUE)
    ]


async def run_document_workers(client: Client) -> None:
    """Run both document Workers, restarting them when the slot setting changes."""
    slots = await read_worker_slots(client) or config.DEFAULT_WORKER_SLOTS
    while True:
        workers = document_workers(client, slots)
        tasks = [asyncio.create_task(w.run()) for w in workers]
        log.info(
            "document workers polling queues=%s,%s activity_slots=%d",
            config.FIFO_QUEUE, config.FAIR_QUEUE, slots,
        )
        while True:
            await asyncio.sleep(SETTINGS_POLL_S)
            wanted = await read_worker_slots(client)
            if wanted and wanted != slots:
                break
        log.info("worker slots changed from=%d to=%d; restarting document workers", slots, wanted)
        await asyncio.gather(*(w.shutdown() for w in workers))
        await asyncio.gather(*tasks, return_exceptions=True)
        slots = wanted


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s level=%(levelname)s %(name)s %(message)s")
    client = await connect()
    uploads = UploadActivities(client)
    control = Worker(
        client,
        task_queue=config.CONTROL_QUEUE,
        workflows=[UploadScenario, WorkerSettings],
        activities=[uploads.upload_documents],
    )
    log.info("control worker polling queue=%s", config.CONTROL_QUEUE)
    await asyncio.gather(control.run(), run_document_workers(client))


if __name__ == "__main__":
    asyncio.run(main())
