"""Dashboard API and static UI.

Every presenter action is one request that makes one Temporal call (or one concurrent group).
The dashboard's live view is one Visibility listing per run, refreshed in the background and
shared by every open tab of that run. It never Queries a Workflow, so it costs no Actions and
keeps working while the Workers are down.

Run with ``python -m fairness_demo.api``.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import re
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from temporalio.api.batch.v1 import BatchOperationTermination
from temporalio.api.workflowservice.v1 import StartBatchOperationRequest
from temporalio.client import Client, WithStartWorkflowOperation
from temporalio.common import (
    SearchAttributePair,
    TypedSearchAttributes,
    WorkflowIDConflictPolicy,
)
from temporalio.service import RPCError, RPCStatusCode

from . import config
from .run_workers import RunWorkers, RunWorkersInput, run_task_queues, run_workers_workflow_id
from .search_attributes import DOC_SESSION, DOC_STARTED_AT, DOC_TENANT
from .tenants import PLAN_WEIGHTS, SMALL_TENANTS, TENANTS, fairness_key
from .uploads import ScenarioInput, TieredUploads, UploadScenario, WorkerSettings, WorkerSettingsInput
from .view import build_view
from .worker import connect, read_worker_slots, settings_workflow_id

log = logging.getLogger("fairness_demo.api")
WEB_DIR = Path(__file__).resolve().parent.parent / "web"

SESSION_COOKIE = "fairness_session"
RUN_ID = re.compile(r"^[0-9a-f]{8}$")
# Without SESSION_SECRET, sessions last as long as this process. Set it when hosted.
_secret = (config.SESSION_SECRET or secrets.token_hex(32)).encode()


@dataclass
class RunView:
    """The latest view of one run, and the background refresh that will replace it."""

    result: dict | None = None
    as_of: float = 0.0  # wall-clock time the listing behind result began
    refresh_began: float = 0.0  # monotonic
    refreshing: asyncio.Task | None = None
    seen: float = 0.0  # monotonic time of the last request


class State:
    client: Client
    views: dict[str, RunView] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s level=%(levelname)s %(name)s %(message)s")
    # One Temporal client per process, shared by every request.
    State.client = await connect()
    yield


app = FastAPI(lifespan=lifespan)


def _sign(session: str) -> str:
    return hmac.new(_secret, session.encode(), hashlib.sha256).hexdigest()[:24]


def session_id(request: Request, response: Response) -> str:
    """An opaque per-browser session from a signed cookie; a new one if it is missing or forged."""
    raw = request.cookies.get(SESSION_COOKIE, "")
    session, _, sig = raw.partition(".")
    if re.fullmatch(r"[0-9a-f]{12}", session) and hmac.compare_digest(sig, _sign(session)):
        return session
    session = secrets.token_hex(6)
    response.set_cookie(
        SESSION_COOKIE,
        f"{session}.{_sign(session)}",
        httponly=True,
        samesite="lax",
        secure=request.url.scheme == "https",
        max_age=7 * 24 * 3600,
    )
    return session


def run_key(session: str, run: str) -> str:
    if not RUN_ID.fullmatch(run):
        raise HTTPException(400, "run must be 8 hex characters")
    return f"fairness-{config.TASK_QUEUE}-{session}-{run}"


def scenario_input(key: str) -> ScenarioInput:
    fifo_queue, fair_queue = run_task_queues(key)
    return ScenarioInput(
        run_key=key,
        fifo_queue=fifo_queue,
        fair_queue=fair_queue,
        acme_bulk_docs=config.ACME_BULK_DOCS,
        trickle_docs=config.TRICKLE_DOCS,
        trickle_interval_s=config.TRICKLE_INTERVAL_S,
        tiered_docs_per_tenant=config.TIERED_DOCS_PER_TENANT,
    )


async def start_run_workers(key: str) -> None:
    """Start the run's own document Workers, unless they're already running."""
    fifo_queue, fair_queue = run_task_queues(key)
    slots = await read_worker_slots(State.client) or config.DEFAULT_WORKER_SLOTS
    await State.client.start_workflow(
        RunWorkers.run,
        RunWorkersInput(fifo_queue=fifo_queue, fair_queue=fair_queue, worker_slots=slots),
        id=run_workers_workflow_id(key),
        task_queue=config.CONTROL_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        # Tagged with the run, so Reset terminates it and its Workers stop.
        search_attributes=TypedSearchAttributes([SearchAttributePair(DOC_SESSION, key)]),
    )


async def signal_scenario(key: str, signal: str, args: list) -> None:
    """Signal-With-Start: the run's uploads Workflow starts on the first action, and its
    Workers with it."""
    await asyncio.gather(
        start_run_workers(key),
        State.client.start_workflow(
            UploadScenario.run,
            scenario_input(key),
            id=f"{key}-uploads",
            task_queue=config.CONTROL_QUEUE,
            start_signal=signal,
            start_signal_args=args,
            search_attributes=TypedSearchAttributes([SearchAttributePair(DOC_SESSION, key)]),
        ),
    )


@app.get("/healthz")
async def healthz() -> dict:
    ok = await State.client.service_client.check_health()
    return {"ok": ok}


@app.get("/api/settings")
async def settings(session: str = Depends(session_id)) -> dict:
    slots = await read_worker_slots(State.client) or config.DEFAULT_WORKER_SLOTS
    return {
        "session": session[:4],
        "task_queues": {"off": config.FIFO_QUEUE, "on": config.FAIR_QUEUE},
        "worker_slots": slots,
        "max_worker_slots": config.MAX_WORKER_SLOTS,
        "acme_bulk_docs": config.ACME_BULK_DOCS,
        "trickle_docs": config.TRICKLE_DOCS,
        "trickle_interval_s": config.TRICKLE_INTERVAL_S,
        "tiered_docs_per_tenant": config.TIERED_DOCS_PER_TENANT,
        # One document's own work: the model call and its three tools, with no queueing.
        "doc_work_s": config.SIM_MODEL_SECONDS + 3 * config.SIM_TOOL_SECONDS,
        "plan_weights": PLAN_WEIGHTS,
        "tenants": [
            {"id": t.id, "name": t.name, "plan": t.plan, "weight": t.weight, "fairness_key": fairness_key(t.id)}
            for t in TENANTS
        ],
        "small_tenants": SMALL_TENANTS,
        "temporal_ui": config.TEMPORAL_UI_URL,
        "namespace": State.client.namespace,
    }


async def _model_call_started(workflow_id: str) -> datetime | None:
    try:
        description = await State.client.get_workflow_handle(workflow_id).describe()
    except RPCError:
        return None
    for pending in description.raw_description.pending_activities:
        if pending.activity_type.name == "call_model" and pending.HasField("last_started_time"):
            return pending.last_started_time.ToDatetime(timezone.utc)
    return None


async def _activity_starts(key: str) -> list:
    """The run's Activity start counts, from its Workers' latest heartbeat (run_workers.py)."""
    try:
        description = await State.client.get_workflow_handle(run_workers_workflow_id(key)).describe()
    except RPCError:
        return []
    for pending in description.raw_description.pending_activities:
        if pending.heartbeat_details.payloads:
            (rows,) = await State.client.data_converter.decode(pending.heartbeat_details.payloads)
            return rows
    return []


async def _list_documents(key: str) -> list:
    return [
        wf
        async for wf in State.client.list_workflows(
            f'DocSession = "{key}" AND WorkflowType = "DocumentAgent"', page_size=1000
        )
    ]


async def _list_run(key: str) -> dict:
    executions, activity_starts = await asyncio.gather(_list_documents(key), _activity_starts(key))
    # A document's start reaches Visibility when its model call returns. The few small-tenant
    # documents not marked started yet get one Describe each, all at once, so the screen
    # doesn't count a document that is already running as waiting. This second round trip is
    # the price of not Querying, and stays small: Acme's thousands are never described.
    unmarked = [
        wf.id
        for wf in executions
        if wf.close_time is None
        and wf.typed_search_attributes.get(DOC_TENANT) in SMALL_TENANTS
        and wf.typed_search_attributes.get(DOC_STARTED_AT) is None
    ][:64]
    found = await asyncio.gather(*(_model_call_started(i) for i in unmarked))
    started_now = {i: t for i, t in zip(unmarked, found) if t}
    return build_view(key, executions, datetime.now(timezone.utc), started_now, activity_starts)


async def _refresh(key: str, entry: RunView) -> None:
    began = time.time()
    try:
        entry.result = await _list_run(key)
        entry.as_of = began
    except RPCError as e:
        # Keep serving the last view; its age shows on the dashboard until a refresh succeeds.
        log.warning("view refresh for %s failed: %s", key, e)
    finally:
        entry.refreshing = None


@app.get("/api/runs/{run}/view")
async def view(run: str, session: str = Depends(session_id)) -> dict:
    """The run's latest view, at once. A big run takes more than a poll interval to list, so the
    listing runs in the background, one at a time per run, and a poll never waits on it once a
    first view exists. The dashboard shows age_s, so a slow refresh reads as a stale view."""
    key = run_key(session, run)
    now = time.monotonic()
    entry = State.views.setdefault(key, RunView())
    entry.seen = now
    if entry.refreshing is None and now - entry.refresh_began > 0.8:
        entry.refresh_began = now
        entry.refreshing = asyncio.create_task(_refresh(key, entry))
    for stale in [k for k, v in State.views.items() if now - v.seen > 60]:
        State.views.pop(stale, None)
    if entry.result is None and entry.refreshing is not None:
        # Shielded: the browser aborts a poll after a second, which must not cancel the listing.
        await asyncio.shield(entry.refreshing)
    if entry.result is None:
        raise HTTPException(503, "the run's view isn't ready yet")
    return {
        **entry.result,
        "run": run,
        "temporal_query": f'DocSession="{key}"',
        "task_queues": dict(zip(("off", "on"), run_task_queues(key))),
        "age_s": max(0.0, time.time() - entry.as_of),
    }


@app.post("/api/runs/{run}/bulk", status_code=202)
async def start_bulk(run: str, session: str = Depends(session_id)) -> dict:
    await signal_scenario(run_key(session, run), "start_bulk", [])
    return {"accepted": "bulk", "documents_per_side": config.ACME_BULK_DOCS}


@app.post("/api/runs/{run}/trickle", status_code=202)
async def start_trickle(run: str, session: str = Depends(session_id)) -> dict:
    await signal_scenario(run_key(session, run), "start_trickle", [])
    return {"accepted": "trickle", "documents": config.TRICKLE_DOCS}


class TieredRequest(BaseModel):
    weights: dict[str, float] = Field(default_factory=lambda: dict(PLAN_WEIGHTS))


@app.post("/api/runs/{run}/tiered", status_code=202)
async def start_tiered(run: str, body: TieredRequest, session: str = Depends(session_id)) -> dict:
    weights = {plan: body.weights.get(plan, w) for plan, w in PLAN_WEIGHTS.items()}
    if not all(0.001 <= w <= 1000 for w in weights.values()):
        raise HTTPException(400, "weights must be between 0.001 and 1000")
    await signal_scenario(run_key(session, run), "start_tiered", [TieredUploads(weights=weights)])
    return {"accepted": "tiered", "documents_per_tenant": config.TIERED_DOCS_PER_TENANT, "weights": weights}


class ResetRequest(BaseModel):
    next_run: str
    # Sent only when the presenter changed it, so a normal Reset needs no Worker.
    worker_slots: int | None = None


async def terminate_run(key: str) -> None:
    """Stop every running Workflow of this run (documents and uploads) with one batch job."""
    # The batch job works through thousands of Workflows in no set order, so stop the run's
    # Workers first: terminating RunWorkers cancels their Activity, which shuts them down.
    try:
        await State.client.get_workflow_handle(run_workers_workflow_id(key)).terminate("dashboard reset")
    except RPCError:
        pass  # not started, or already stopped
    query = f'DocSession = "{key}" AND ExecutionStatus = "Running"'
    try:
        await State.client.workflow_service.start_batch_operation(
            StartBatchOperationRequest(
                namespace=State.client.namespace,
                job_id=str(uuid.uuid4()),
                reason="dashboard reset",
                visibility_query=query,
                termination_operation=BatchOperationTermination(),
            )
        )
    except RPCError as e:
        if e.status != RPCStatusCode.RESOURCE_EXHAUSTED:
            raise
        # The Namespace allows only so many batch jobs at once; fall back to one call per
        # Workflow, a few at a time.
        gate = asyncio.Semaphore(16)

        async def terminate(workflow_id: str) -> None:
            async with gate:
                try:
                    await State.client.get_workflow_handle(workflow_id).terminate("dashboard reset")
                except RPCError:
                    pass

        ids = [wf.id async for wf in State.client.list_workflows(query)]
        await asyncio.gather(*(terminate(i) for i in ids))


async def set_worker_slots(slots: int) -> int:
    """Update-With-Start on the shared settings Workflow; its validator refuses bad values."""
    start = WithStartWorkflowOperation(
        WorkerSettings.run,
        WorkerSettingsInput(worker_slots=slots),
        id=settings_workflow_id(),
        task_queue=config.CONTROL_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        memo={"worker_slots": slots},
    )
    return await State.client.execute_update_with_start_workflow(
        WorkerSettings.set_worker_slots, slots, start_workflow_operation=start
    )


@app.post("/api/runs/{run}/reset")
async def reset(run: str, body: ResetRequest, session: str = Depends(session_id)) -> dict:
    old_key, _ = run_key(session, run), run_key(session, body.next_run)
    jobs = [terminate_run(old_key)]
    if body.worker_slots is not None:
        if not 1 <= body.worker_slots <= config.MAX_WORKER_SLOTS:
            raise HTTPException(400, f"worker slots must be 1 to {config.MAX_WORKER_SLOTS}")
        jobs.append(set_worker_slots(body.worker_slots))
    results = await asyncio.gather(*jobs)
    return {"run": body.next_run, "worker_slots": results[1] if len(results) > 1 else None}


app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")


def main() -> None:
    uvicorn.run("fairness_demo.api:app", host=config.API_HOST, port=config.API_PORT, log_level="warning")


if __name__ == "__main__":
    main()
