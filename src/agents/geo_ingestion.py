from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

from agents import Agent, handoff, RunContextWrapper
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX
from pydantic import Field
from src.agents.handoff_base import BaseHandoff

from src.agents.tool_utils import get_session_tools
from src.utils.prompts import load_prompt


class GeoIngestionHandoff(BaseHandoff):
    """Input to the GeoIngestionAgent."""

    geo_ids: list[str] = Field(
        ..., description="List of GEO IDs (GSM, GSE) or PubMed IDs (PMID) to extract metadata from."
    )
    extraction_type: str = Field(
        default="auto", 
        description="Type of extraction: 'gsm', 'gse', 'series_matrix', 'paper', or 'auto' to detect automatically."
    )


def on_handoff_callback(ctx: RunContextWrapper[None], input_data: BaseHandoff):
    """A no-op callback to satisfy the handoff function's requirement."""
    pass


def create_geo_ingestion_agent(
    session_id: str, sandbox_dir: str = None, handoffs: list = None
) -> Agent:
    """
    Factory method to create a GEO metadata ingestion agent.

    This agent is responsible for extracting metadata from Gene Expression 
    Omnibus (GEO) and PubMed databases given GSM/GSE/PMID identifiers.

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
        An instance of an Agent configured for GEO metadata extraction tasks.
    """
    if session_id is None:
        session_id = str(uuid4())

    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    session_dir = (Path(sandbox_dir) / session_id).absolute()
    session_dir.mkdir(parents=True, exist_ok=True)

    tools = get_session_tools(session_dir)
    print(f"🔧 Created {len(tools)} tools for session {session_id}")
    print(f"🔧 Tool names: {[t.name for t in tools]}")

    instructions = RECOMMENDED_PROMPT_PREFIX + "\n\n" + load_prompt(
        "geo_ingestion_agent.md", 
        session_dir=str(session_dir)
    )
    print(f"📝 Loaded instructions: {len(instructions)} characters")

    return Agent(
        name="GeoIngestionAgent",
        instructions=instructions,
        tools=tools,
        handoffs=handoffs or [],
    ) 