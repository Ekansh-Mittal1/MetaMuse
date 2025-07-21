from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from agents import Agent, RunContextWrapper
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX
from pydantic import Field
from typing import Optional
from src.agents.handoff_base import BaseHandoff

from src.agents.tool_utils import get_session_tools
from src.utils.prompts import load_prompt

# Import Pydantic models for structured data
from src.models import IngestionOutput


class IngestionHandoff(BaseHandoff):
    """Input to the IngestionAgent."""

    geo_ids: list[str] = Field(
        ...,
        description="List of GEO IDs (GSM, GSE) or PubMed IDs (PMID) to extract metadata from.",
    )
    extraction_type: str = Field(
        default="auto",
        description="Type of extraction: 'gsm', 'gse', 'series_matrix', 'paper', or 'auto' to detect automatically.",
    )
    
    # Optional structured output from previous agent (for workflow continuity)
    upstream_data: Optional[dict] = Field(
        default=None,
        description="Optional structured data from upstream workflow step",
    )


def on_handoff_callback(ctx: RunContextWrapper[None], input_data: BaseHandoff):
    """A no-op callback to satisfy the handoff function's requirement."""
    pass


def create_ingestion_agent(
    session_id: str, sandbox_dir: str = None, handoffs: list = None
) -> Agent:
    """
    Factory method to create a metadata ingestion agent.

    This agent is responsible for extracting metadata from Gene Expression
    Omnibus (GEO) and PubMed databases given GSM/GSE/PMID identifiers.
    
    The agent is configured with structured output capabilities to produce
    validated IngestionOutput objects directly.

    Parameters
    ----------
    session_id : str
        The unique session identifier.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    handoffs : list, optional
        List of handoff objects to configure for this agent

    Returns
    -------
    Agent
        An instance of an Agent configured for metadata extraction tasks with structured outputs.
    """
    if session_id is None:
        session_id = f"ing_{str(uuid4())}"

    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    session_dir = (Path(sandbox_dir) / session_id).absolute()
    session_dir.mkdir(parents=True, exist_ok=True)

    tools = get_session_tools(session_dir)
    print(f"✅ IngestionAgent: Initialized with {len(tools)} tools")

    instructions = (
        RECOMMENDED_PROMPT_PREFIX
        + "\n\n"
        + load_prompt("ingestion_agent.md", session_dir=str(session_dir))
        + "\n\n"
        + "IMPORTANT: At the end of your work, provide a structured summary using the IngestionOutput format. "
        + "Include all extracted metadata, file paths, processing statistics, and sample IDs ready for the next agent."
    )

    return Agent(
        name="IngestionAgent",
        instructions=instructions,
        tools=tools,
        handoffs=handoffs or [],
        # Following DendroForge pattern: no structured outputs, natural language responses
    )
