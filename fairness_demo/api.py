"""Dashboard API and static UI.

Every presenter action is one request that makes one Temporal call (or one concurrent group).
The dashboard's live view is one Visibility listing per run, shared by every open tab of that
run for about a second. It never Queries a Workflow, so it costs no Actions and keeps working
while the Workers are down.

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


class State:
    client: Client
    views: dict[str, tuple[float, asyncio.Task]] = {}


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
    return ScenarioInput(
        run_key=key,
        fifo_queue=config.FIFO_QUEUE,
        fair_queue=config.FAIR_QUEUE,
        acme_bulk_docs=config.ACME_BULK_DOCS,
        trickle_docs=config.TRICKLE_DOCS,
        trickle_interval_s=config.TRICKLE_INTERVAL_S,
        tiered_docs_per_tenant=config.TIERED_DOCS_PER_TENANT,
    )


async def signal_scenario(key: str, signal: str, args: list) -> None:
    """Signal-With-Start: the run's uploads Workflow starts on the first action."""
    await State.client.start_workflow(
        UploadScenario.run,
        scenario_input(key),
        id=f"{key}-uploads",
        task_queue=config.CONTROL_QUEUE,
        start_signal=signal,
        start_signal_args=args,
        search_attributes=TypedSearchAttributes([SearchAttributePair(DOC_SESSION, key)]),
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


async def _list_run(key: str) -> dict:
    executions = [
        wf
        async for wf in State.client.list_workflows(
            f'DocSession = "{key}" AND WorkflowType = "DocumentAgent"', page_size=1000
        )
    ]
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
    return build_view(key, executions, datetime.now(timezone.utc), started_now)


@app.get("/api/runs/{run}/view")
async def view(run: str, session: str = Depends(session_id)) -> dict:
    key = run_key(session, run)
    cached = State.views.get(key)
    if cached is None or time.monotonic() - cached[0] > 0.8:
        cached = (time.monotonic(), asyncio.create_task(_list_run(key)))
        State.views[key] = cached
        for stale in [k for k, (at, _) in State.views.items() if time.monotonic() - at > 60]:
            State.views.pop(stale, None)
    result = await cached[1]
    return {**result, "run": run, "temporal_query": f'DocSession="{key}"'}


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
