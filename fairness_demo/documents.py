"""Document identity shared by the uploads and the dashboard.

A document's file name and page count follow from its tenant and number, so the dashboard
can label any document from its Workflow ID without reading it back.
"""

_KINDS = {
    "acme": ["purchase-order", "invoice", "vendor-contract", "shipping-manifest"],
    "birch": ["engagement-letter", "deposition", "case-brief"],
    "cedar": ["intake-form", "discharge-summary", "lab-results", "referral-letter", "insurance-claim"],
    "dune": ["bill-of-lading", "customs-form", "freight-invoice"],
}

SIDES = {"off": False, "on": True}


def file_name(tenant_id: str, n: int) -> str:
    kinds = _KINDS[tenant_id]
    return f"{kinds[n % len(kinds)]}-{(n * 7919) % 9000 + 100}.pdf"


def pages(n: int) -> int:
    return 1 + (n * 37) % 12


def document_workflow_id(run_key: str, side: str, tenant_id: str, n: int) -> str:
    return f"{run_key}-{side}-{tenant_id}-{n}"


def parse_document_workflow_id(run_key: str, workflow_id: str) -> tuple[str, str, int]:
    side, tenant_id, n = workflow_id[len(run_key) + 1 :].split("-")
    return side, tenant_id, int(n)
