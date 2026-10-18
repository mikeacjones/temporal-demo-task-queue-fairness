"""Simulated tenant uploads: a presenter-driven Workflow that starts document agent runs.

Each upload starts the document's agent run with Update-With-Start: one call starts the
DocumentAgent with its tenant's Priority and hands it the document as its one message.
Both sides get the same documents in the same order, interleaved one by one.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import (
    SearchAttributePair,
    TypedSearchAttributes,
    WorkflowIDConflictPolicy,
    WorkflowIDReusePolicy,
)
from temporalio.exceptions import ApplicationError, WorkflowAlreadyStartedError

with workflow.unsafe.imports_passed_through():
    from temporalio.client import (
        Client,
        WithStartWorkflowOperation,
        WorkflowUpdateStage,
    )

    from temporal_agent_harness.harness.agent_protocol import (
        SEND_AGENT_MESSAGE_UPDATE,
        AgentConfig,
        AgentMessage,
    )

    from .document_agent import Document, DocumentAgent
    from .documents import SIDES, document_workflow_id, file_name, pages
    from .config import MAX_WORKER_SLOTS
    from .search_attributes import DOC_SESSION, DOC_TENANT
    from .tenants import TENANTS, TENANTS_BY_ID, document_priority


@dataclass
class UploadBatch:
    run_key: str
    tenant_id: str
    first: int
    count: int
    sides: list[str]
    weight: float
    fifo_queue: str
    fair_queue: str


# Documents started per heartbeat, all at once. Acme's whole backlog lands in seconds, so it is
# already queued when the presenter starts the trickle. If the burst passes the Namespace's
# Actions-per-second limit, the client retries the throttled starts.
UPLOAD_CONCURRENCY = 500


class UploadActivities:
    def __init__(self, client: Client) -> None:
        self._client = client

    @activity.defn
    async def upload_documents(self, batch: UploadBatch) -> int:
        """Start one agent run per document, on each requested side.

        Safe to retry: a document's Workflow ID is fixed, a duplicate start is refused, and
        the heartbeat records how far a previous attempt got.
        """
        done = activity.info().heartbeat_details[0] if activity.info().heartbeat_details else 0
        for start in range(batch.first + done, batch.first + batch.count, UPLOAD_CONCURRENCY):
            end = min(start + UPLOAD_CONCURRENCY, batch.first + batch.count)
            await asyncio.gather(
                *(self._start_document(batch, n, side) for n in range(start, end) for side in batch.sides)
            )
            activity.heartbeat(end - batch.first)
        return batch.count

    async def _start_document(self, batch: UploadBatch, n: int, side: str) -> None:
        fair = SIDES[side]
        workflow_id = document_workflow_id(batch.run_key, side, batch.tenant_id, n)
        start = WithStartWorkflowOperation(
            DocumentAgent.run,
            args=[AgentConfig()],
            id=workflow_id,
            task_queue=batch.fair_queue if fair else batch.fifo_queue,
            # The tenant's fairness key and plan weight. The agent never sets them; every
            # Activity it schedules inherits them from here.
            priority=document_priority(batch.tenant_id, fair, batch.weight),
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            search_attributes=TypedSearchAttributes(
                [
                    SearchAttributePair(DOC_SESSION, batch.run_key),
                    SearchAttributePair(DOC_TENANT, batch.tenant_id),
                ]
            ),
        )
        message = AgentMessage(
            type="process",
            payload=Document(
                document_id=workflow_id, file_name=file_name(batch.tenant_id, n), pages=pages(n)
            ).model_dump(),
        )
        try:
            await self._client.start_update_with_start_workflow(
                SEND_AGENT_MESSAGE_UPDATE,
                message,
                start_workflow_operation=start,
                id=f"{workflow_id}-upload",
                wait_for_stage=WorkflowUpdateStage.ACCEPTED,
            )
        except WorkflowAlreadyStartedError:
            # A retried upload of a document that already finished: nothing left to do.
            pass


@dataclass
class ScenarioInput:
    run_key: str
    fifo_queue: str
    fair_queue: str
    acme_bulk_docs: int
    trickle_docs: dict[str, int]
    trickle_interval_s: dict[str, float]
    tiered_docs_per_tenant: int


@dataclass
class TieredUploads:
    weights: dict[str, float] = field(default_factory=dict)


# Presenter actions are accepted for this long; a forgotten tab stops costing anything.
SCENARIO_LIFETIME = timedelta(hours=2)
_UPLOAD_TIMEOUT = timedelta(minutes=10)
_UPLOAD_HEARTBEAT = timedelta(seconds=30)


@workflow.defn
class UploadScenario:
    """One run of the demo: Acme's bulk upload, the small tenants' trickle, the tiered uploads."""

    def __init__(self) -> None:
        self._input: ScenarioInput | None = None
        self._started: set[str] = set()

    @workflow.run
    async def run(self, input: ScenarioInput) -> None:
        self._input = input
        try:
            await workflow.wait_condition(lambda: False, timeout=SCENARIO_LIFETIME)
        except asyncio.TimeoutError:
            pass
        await workflow.wait_condition(workflow.all_handlers_finished)

    def _first_time(self, action: str) -> bool:
        if action in self._started:
            return False
        self._started.add(action)
        return True

    async def _upload(self, tenant_id: str, first: int, count: int, sides: list[str], weight: float) -> None:
        assert self._input
        await workflow.execute_activity_method(
            UploadActivities.upload_documents,
            UploadBatch(
                run_key=self._input.run_key,
                tenant_id=tenant_id,
                first=first,
                count=count,
                sides=sides,
                weight=weight,
                fifo_queue=self._input.fifo_queue,
                fair_queue=self._input.fair_queue,
            ),
            start_to_close_timeout=_UPLOAD_TIMEOUT,
            heartbeat_timeout=_UPLOAD_HEARTBEAT,
        )

    @workflow.signal
    async def start_bulk(self) -> None:
        """Acme uploads its whole backlog at once, to both sides."""
        await workflow.wait_condition(lambda: self._input is not None)
        assert self._input
        if self._first_time("bulk"):
            await self._upload("acme", 0, self._input.acme_bulk_docs, ["off", "on"], _plan_weight("acme"))

    @workflow.signal
    async def start_trickle(self) -> None:
        """The small tenants upload one document at a time, each on its own interval."""
        await workflow.wait_condition(lambda: self._input is not None)
        assert self._input
        if not self._first_time("trickle"):
            return
        arrivals = sorted(
            (k * self._input.trickle_interval_s[tenant_id], tenant_id, k)
            for tenant_id, count in self._input.trickle_docs.items()
            for k in range(count)
        )
        began = workflow.time()
        for at, tenant_id, k in arrivals:
            delay = at - (workflow.time() - began)
            if delay > 0:
                await workflow.sleep(delay)
            await self._upload(tenant_id, k, 1, ["off", "on"], _plan_weight(tenant_id))

    @workflow.signal
    async def start_tiered(self, uploads: TieredUploads) -> None:
        """Every tenant uploads at once, on the fairness-on side, weighted by plan."""
        await workflow.wait_condition(lambda: self._input is not None)
        assert self._input
        if not self._first_time("tiered"):
            return
        await asyncio.gather(
            *(
                self._upload(
                    t.id,
                    0,
                    self._input.tiered_docs_per_tenant,
                    ["on"],
                    uploads.weights.get(t.plan, t.weight),
                )
                for t in TENANTS
            )
        )


def _plan_weight(tenant_id: str) -> float:
    return TENANTS_BY_ID[tenant_id].weight


@dataclass
class WorkerSettingsInput:
    worker_slots: int


@workflow.defn
class WorkerSettings:
    """Activity slots per document Worker, shared by every session. The API reads it from the
    memo with Describe when a run starts, and that run's Workers use it."""

    @workflow.init
    def __init__(self, input: WorkerSettingsInput) -> None:
        self._slots = input.worker_slots

    @workflow.run
    async def run(self, input: WorkerSettingsInput) -> None:
        await workflow.wait_condition(lambda: workflow.info().is_continue_as_new_suggested())
        await workflow.wait_condition(workflow.all_handlers_finished)
        workflow.continue_as_new(WorkerSettingsInput(worker_slots=self._slots))

    @workflow.update
    def set_worker_slots(self, slots: int) -> int:
        self._slots = slots
        workflow.upsert_memo({"worker_slots": slots})
        return slots

    @set_worker_slots.validator
    def validate_worker_slots(self, slots: int) -> None:
        if not 1 <= slots <= MAX_WORKER_SLOTS:
            raise ApplicationError(f"worker slots must be 1 to {MAX_WORKER_SLOTS}")
