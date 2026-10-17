"""Turn one Visibility listing of a run's documents into everything the dashboard shows.

Each document contributes three times: its upload (the Workflow's start time), its
processing start (the DocStartedAt Search Attribute), and its finish (the close time).
Every number and chart on the screen is derived from those, so the API keeps no state.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime

from temporalio.client import WorkflowExecution, WorkflowExecutionStatus

from .documents import file_name, parse_document_workflow_id
from .search_attributes import DOC_STARTED_AT
from .tenants import SMALL_TENANTS, TENANTS

BUCKET_S = 10
LINE_HEAD = 60
SIDES = ("off", "on")


@dataclass
class Doc:
    side: str
    tenant: str
    n: int
    uploaded: float
    started: float | None
    finished: float | None
    status: WorkflowExecutionStatus

    @property
    def done(self) -> bool:
        return self.status == WorkflowExecutionStatus.COMPLETED

    @property
    def waiting(self) -> bool:
        return self.started is None and self.finished is None

    @property
    def running(self) -> bool:
        return self.started is not None and self.finished is None

    def wait_s(self, now: float) -> float:
        return (self.started if self.started is not None else now) - self.uploaded


def to_doc(run_key: str, wf: WorkflowExecution, started_now: dict[str, datetime]) -> Doc:
    side, tenant, n = parse_document_workflow_id(run_key, wf.id)
    started = wf.typed_search_attributes.get(DOC_STARTED_AT) or started_now.get(wf.id)
    return Doc(
        side=side,
        tenant=tenant,
        n=n,
        uploaded=wf.start_time.timestamp(),
        started=started.timestamp() if started else None,
        finished=wf.close_time.timestamp() if wf.close_time else None,
        status=wf.status,
    )


def p95(values: list[float]) -> float | None:
    """Nearest-rank 95th percentile."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


def side_view(docs: list[Doc], t0: float, now: float) -> dict:
    tenants = {}
    for t in TENANTS:
        mine = [d for d in docs if d.tenant == t.id]
        tenants[t.id] = {
            "total": len(mine),
            "waiting": sum(d.waiting for d in mine),
            "running": sum(d.running for d in mine),
            "done": sum(d.done for d in mine),
            "p95_wait_s": p95([d.wait_s(now) for d in mine]),
            "last_finish_s": max((d.finished - t0 for d in mine if d.done), default=None),
        }
    small = [d for d in docs if d.tenant in SMALL_TENANTS]

    buckets = max(1, math.ceil((now - t0) / BUCKET_S))
    completions = [{t.id: 0 for t in TENANTS} for _ in range(buckets)]
    for d in docs:
        if d.done and d.finished is not None:
            completions[min(buckets - 1, int((d.finished - t0) // BUCKET_S))][d.tenant] += 1
    backlog = []
    for i in range(buckets + 1):
        at = min(now, t0 + i * BUCKET_S)
        counts = {t.id: 0 for t in TENANTS}
        for d in docs:
            if d.uploaded <= at and (d.started is None or d.started > at) and (d.finished is None or d.finished > at):
                counts[d.tenant] += 1
        backlog.append(counts)

    line = sorted((d for d in docs if d.waiting), key=lambda d: (d.uploaded, d.n))
    small_positions = [
        {"tenant": d.tenant, "position": i + 1} for i, d in enumerate(line) if d.tenant in SMALL_TENANTS
    ]
    return {
        "tenants": tenants,
        "small": {
            "total": len(small),
            "waiting": sum(d.waiting for d in small),
            "p95_wait_s": p95([d.wait_s(now) for d in small]),
        },
        "completions": completions,
        "backlog": backlog,
        "line": {
            "total": len(line),
            "head": [d.tenant for d in line[:LINE_HEAD]],
            "small_positions": small_positions[:40],
        },
        "cedar": [
            {
                "file": file_name("cedar", d.n),
                "uploaded_s": d.uploaded - t0,
                "state": "done" if d.done else "running" if d.running else "waiting" if d.waiting else "stopped",
                "wait_s": d.wait_s(now),
                "took_s": (d.finished - d.uploaded) if d.done and d.finished else None,
            }
            for d in sorted((d for d in docs if d.tenant == "cedar"), key=lambda d: d.n)
        ],
        # Fairness shares dispatches, so act 2 compares weights with documents started.
        "recent_starts": {
            t.id: sum(1 for d in docs if d.tenant == t.id and d.started and d.started > now - 60)
            for t in TENANTS
        },
    }


def build_view(
    run_key: str,
    executions: list[WorkflowExecution],
    now: datetime,
    started_now: dict[str, datetime] | None = None,
) -> dict:
    """``started_now`` holds start times found with Describe for documents whose start isn't
    in Visibility yet."""
    docs = [to_doc(run_key, wf, started_now or {}) for wf in executions]
    now_s = now.timestamp()
    t0 = min((d.uploaded for d in docs), default=now_s)
    return {
        "as_of": now.isoformat(),
        "elapsed_s": now_s - t0 if docs else 0,
        "bucket_s": BUCKET_S,
        "documents": len(docs),
        "sides": {side: side_view([d for d in docs if d.side == side], t0, now_s) for side in SIDES},
    }
