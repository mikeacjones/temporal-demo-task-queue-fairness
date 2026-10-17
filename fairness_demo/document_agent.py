"""The document agent: one temporal-agent-harness agent run per uploaded document.

The agent knows nothing about tenants, fairness keys, or weights. The upload starts its run
with the tenant's fairness key and plan weight, and Temporal applies them to every Workflow
Task and Activity the run schedules.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.contrib.workflow_streams import WorkflowStream

with workflow.unsafe.imports_passed_through():
    from pydantic import BaseModel

    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent_protocol import AgentConfig, ToolApprovalPolicy
    from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner

    from .activities import DOCUMENT_TOOLS, ModelRequest, call_model
    from .search_attributes import DOC_STARTED_AT

TOOLS_BY_NAME = {tool.__name__: tool for tool in DOCUMENT_TOOLS}


class Document(BaseModel):
    """An uploaded document, as the agent sees it."""

    document_id: str
    file_name: str
    pages: int


class ProcessedDocument(BaseModel):
    """What the agent found in the document."""

    document_id: str
    doc_type: str
    fields: dict[str, str]
    summary: str


@agent.defn(name="DocumentAgent")
class DocumentAgent:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            # The tools only read the document the tenant uploaded, so none needs a human.
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
        )

    @agent.accepts
    async def process(self, document: Document) -> ProcessedDocument:
        """Read an uploaded document, then extract its fields, classify it, and summarize it."""
        plan = await workflow.execute_activity(
            call_model,
            ModelRequest(
                document_id=document.document_id,
                file_name=document.file_name,
                pages=document.pages,
            ),
            # The simulated call takes a fixed 1-2 s; a real model call would need longer.
            start_to_close_timeout=timedelta(seconds=60),
        )
        # The dashboard measures each document's wait as upload time to this moment.
        workflow.upsert_search_attributes([DOC_STARTED_AT.value_set(plan.started_at)])

        # The model asked for independent tools, so they run concurrently.
        outputs = await asyncio.gather(
            *(
                self._runner.run_tool(
                    call.id,
                    TOOLS_BY_NAME[call.name],
                    document_id=document.document_id,
                    pages=document.pages,
                )
                for call in plan.tool_calls
            )
        )
        results = dict(zip((call.name for call in plan.tool_calls), outputs))

        # One agent run per document, so the run ends once its only message is handled. A
        # Workflow can't Signal itself, and harness 0.7.0 exposes closing only as the
        # "close" Signal, so this calls the runner's handler for that Signal directly.
        self._runner._handle_close()
        return ProcessedDocument(
            document_id=document.document_id,
            doc_type=results["classify_document"],
            fields=results["extract_fields"],
            summary=results["summarize_document"],
        )
