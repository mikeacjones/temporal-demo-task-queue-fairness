"""Tenants, their plans, and the fairness key and weight each document run starts with."""

from dataclasses import dataclass

from temporalio.common import Priority


@dataclass(frozen=True)
class Tenant:
    id: str
    name: str
    plan: str
    weight: float


TENANTS = [
    Tenant("acme", "Acme Corp", "Enterprise", 5),
    Tenant("birch", "Birchwood Legal", "Pro", 2),
    Tenant("cedar", "Cedar Health", "Starter", 1),
    Tenant("dune", "Dune Freight", "Starter", 1),
]
TENANTS_BY_ID = {t.id: t for t in TENANTS}
SMALL_TENANTS = ["birch", "cedar", "dune"]

PLAN_WEIGHTS = {"Enterprise": 5.0, "Pro": 2.0, "Starter": 1.0}


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
