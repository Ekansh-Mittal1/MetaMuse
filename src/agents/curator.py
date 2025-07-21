"""
Curator agent for metadata curation tasks.

This module provides the CuratorAgent that performs metadata curation
on GEO samples, extracting candidate values for specific metadata fields
and reconciling conflicts across multiple data sources.
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from agents import Agent, RunContextWrapper
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX
from pydantic import Field
from src.agents.handoff_base import BaseHandoff

from src.agents.tool_utils import get_session_tools
from src.utils.prompts import load_prompt


class CuratorHandoff(BaseHandoff):
    """Input to the CuratorAgent."""

    sample_ids: list[str] = Field(
        ...,
        description="List of sample IDs (GSM) to perform metadata curation on.",
    )
    target_field: str = Field(
        default="Disease",
        description="The target metadata field to extract candidates for (e.g., 'Disease', 'Tissue', 'Age').",
    )
    session_directory: str = Field(
        ...,
        description="Path to the session directory containing LinkerAgent output files.",
    )


def on_handoff_callback(ctx: RunContextWrapper[None], input_data: BaseHandoff):
    """A no-op callback to satisfy the handoff function's requirement."""
    pass


def create_curator_agent(
    session_id: str = None,
    sandbox_dir: str = None,
    handoffs: list = None,
    existing_session_dir: str = None,
    input_data: str = None,
) -> Agent:
    """
    Factory method to create a metadata curation agent.

    This agent is responsible for performing metadata curation tasks on
    GEO samples, including extracting candidate values for specific metadata
    fields and reconciling conflicts across multiple data sources.

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
    input_data : str, optional
        Input data string that may contain sample IDs and target field information

    Returns
    -------
    Agent
        An instance of an Agent configured for metadata curation tasks.
    """
    try:
        # Check if "testing" is in the input data
        is_testing = input_data and "testing" in input_data.lower()
        if is_testing:
            print("🧪 CuratorAgent: Testing mode detected")
            # Force testing session directory
            session_dir = Path("sandbox/test-session").absolute()
            session_dir.mkdir(parents=True, exist_ok=True)
            session_id = "test-session"
        elif existing_session_dir:
            # Use existing session directory
            session_dir = Path(existing_session_dir).absolute()
            if not session_dir.exists():
                error_msg = (
                    f"Existing session directory does not exist: {existing_session_dir}"
                )
                print(f"❌ CuratorAgent: {error_msg}")
                raise ValueError(error_msg)
            session_id = session_dir.name
        else:
            # Create new session directory
            if session_id is None:
                session_id = f"curator_{str(uuid4())}"

            if sandbox_dir is None:
                sandbox_dir = "sandbox"

            session_dir = (Path(sandbox_dir) / session_id).absolute()
            session_dir.mkdir(parents=True, exist_ok=True)

        tools = get_session_tools(session_dir)
        print(f"✅ CuratorAgent: Initialized with {len(tools)} tools")

        instructions = (
            RECOMMENDED_PROMPT_PREFIX
            + "\n\n"
            + load_prompt("curator_agent.md", session_dir=str(session_dir))
        )

        agent = Agent(
            name="CuratorAgent",
            instructions=instructions,
            tools=tools,
            handoffs=handoffs or [],
        )

        return agent

    except Exception as e:
        print(f"❌ CuratorAgent creation error: {str(e)}")
        import traceback

        print("🔍 CuratorAgent creation traceback:")
        traceback.print_exc()
        raise 