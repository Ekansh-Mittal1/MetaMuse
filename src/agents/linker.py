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


class LinkerHandoff(BaseHandoff):
    """Input to the LinkerAgent."""

    sample_id: str = Field(
        ..., description="The sample ID to process and link metadata for (e.g., GSM1000981)."
    )
    fields_to_remove: list[str] = Field(
        default=None,
        description="List of fields to remove from metadata files during cleaning. If not provided, uses default fields."
    )
    session_directory: str = Field(
        ..., description="Path to the session directory containing IngestionAgent output files."
    )


def on_handoff_callback(ctx: RunContextWrapper[None], input_data: BaseHandoff):
    """A no-op callback to satisfy the handoff function's requirement."""
    pass


def create_linker_agent(
    session_id: str = None, sandbox_dir: str = None, handoffs: list = None, existing_session_dir: str = None
) -> Agent:
    """
    Factory method to create a metadata linking agent.

    This agent is responsible for processing and linking metadata files
    created by the IngestionAgent, including cleaning files, downloading
    series matrix data, and extracting sample-specific information.

    Parameters
    ----------
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    handoffs : list, optional
        List of handoff objects to configure for this agent
    existing_session_dir : str, optional
        Path to an existing session directory to use instead of creating a new one

    Returns
    -------
    Agent
        An instance of an Agent configured for metadata linking tasks.
    """
    if existing_session_dir:
        # Use existing session directory
        session_dir = Path(existing_session_dir).absolute()
        if not session_dir.exists():
            raise ValueError(f"Existing session directory does not exist: {existing_session_dir}")
        session_id = session_dir.name
    else:
        # Create new session directory
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
        "linker_agent.md", 
        session_dir=str(session_dir)
    )
    print(f"📝 Loaded instructions: {len(instructions)} characters")

    return Agent(
        name="LinkerAgent",
        instructions=instructions,
        tools=tools,
        handoffs=handoffs or [],
    ) 