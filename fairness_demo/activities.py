"""The document agent's model call and its tools, all of them Temporal Activities.

SIMULATED: no model or document service is called. Each Activity sleeps for a fixed time
(SIM_MODEL_SECONDS, SIM_TOOL_SECONDS) and returns canned output, so every run of the demo
takes the same time and needs no API keys. Swap these bodies for real calls and nothing
else in the demo changes.

No ``from __future__ import annotations``: these signatures cross Temporal's data converter.
"""

import asyncio
import hashlib
from datetime import datetime, timedelta

from pydantic import BaseModel
from temporalio import activity
from temporalio.workflow import ActivityConfig

from temporal_agent_harness.harness import agent

from .config import SIM_MODEL_SECONDS, SIM_TOOL_SECONDS


class ToolCall(BaseModel):
    id: str
    name: str


class ModelRequest(BaseModel):
    document_id: str
    file_name: str
    pages: int


class ModelResponse(BaseModel):
    started_at: datetime
    tool_calls: list[ToolCall]


@activity.defn
async def call_model(request: ModelRequest) -> ModelResponse:
    """SIMULATED model call: reads the document and asks for the extract, classify and
    summarize tools. Returns when the model call started, which is when processing began."""
    started_at = activity.info().started_time
    await asyncio.sleep(SIM_MODEL_SECONDS)
    activity.logger.info("model planned tools document=%s", request.document_id)
    return ModelResponse(
        started_at=started_at,
        tool_calls=[
            ToolCall(id=f"{request.document_id}-{name}", name=name)
            for name in ("extract_fields", "classify_document", "summarize_document")
        ],
    )


# Quick simulated calls; 30 s is far above their fixed duration, so a timeout means a
# stuck Worker and Temporal retries the call on another one.
_TOOL = ActivityConfig(start_to_close_timeout=timedelta(seconds=30))

_DOC_TYPES = ["invoice", "contract", "lab report", "referral letter", "bill of lading"]


def _pick(document_id: str, options: list[str]) -> str:
    return options[int(hashlib.sha256(document_id.encode()).hexdigest(), 16) % len(options)]


@agent.activity_tool_defn(activity_config=_TOOL)
async def extract_fields(document_id: str, pages: int) -> dict[str, str]:
    """Extract the key fields from a document: parties, dates, and totals.

    Args:
        document_id: The uploaded document.
        pages: Its page count.
    """
    await asyncio.sleep(SIM_TOOL_SECONDS)
    return {"pages": str(pages), "party": _pick(document_id, ["Northwind", "Contoso", "Fabrikam"])}


@agent.activity_tool_defn(activity_config=_TOOL)
async def classify_document(document_id: str, pages: int) -> str:
    """Classify a document by type.

    Args:
        document_id: The uploaded document.
        pages: Its page count.
    """
    await asyncio.sleep(SIM_TOOL_SECONDS)
    return _pick(document_id, _DOC_TYPES)


@agent.activity_tool_defn(activity_config=_TOOL)
async def summarize_document(document_id: str, pages: int) -> str:
    """Write a two-sentence summary of a document.

    Args:
        document_id: The uploaded document.
        pages: Its page count.
    """
    await asyncio.sleep(SIM_TOOL_SECONDS)
    return f"A {pages}-page {_pick(document_id, _DOC_TYPES)}. Simulated summary."


DOCUMENT_TOOLS = [extract_fields, classify_document, summarize_document]
