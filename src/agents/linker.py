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
from src.models import (
    IngestionOutput, 
    LinkerOutput,
    CurationDataPackage,
    CleanedSeriesMetadata,
    CleanedSampleMetadata, 
    CleanedAbstractMetadata
)


class LinkerHandoff(BaseHandoff):
    """Input to the LinkerAgent."""

    sample_id: str = Field(
        ...,
        description="The sample ID to process and link metadata for (e.g., GSM1000981).",
    )
    fields_to_remove: list[str] = Field(
        default=[],
        description="List of fields to remove from metadata files during cleaning. If not provided, uses default fields.",
    )
    session_directory: str = Field(
        ...,
        description="Path to the session directory containing IngestionAgent output files.",
    )
    all_sample_ids: list[str] = Field(
        default=[],
        description="List of all sample IDs that were processed by the IngestionAgent.",
    )
    target_field: str = Field(
        default="Disease",
        description="Target metadata field for curation (e.g., 'Disease', 'Tissue', 'Age').",
    )
    
    # Following DendroForge pattern: no complex nested structures in handoffs
    # ingestion_output: Optional[IngestionOutput] = Field(
    #     default=None,
    #     description="Complete structured output from the IngestionAgent, containing extracted metadata and mapping information",
    # )


def on_handoff_callback(ctx: RunContextWrapper[None], input_data: BaseHandoff):
    """A no-op callback to satisfy the handoff function's requirement."""
    pass


def create_linker_agent(
    session_id: str = None,
    sandbox_dir: str = None,
    handoffs: list = None,
    existing_session_dir: str = None,
    input_data: str = None,
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
    try:
        # Check if "testing" is in the input data
        is_testing = input_data and "testing" in input_data.lower()
        if is_testing:
            print("🧪 LinkerAgent: Testing mode detected")
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
                print(f"❌ LinkerAgent: {error_msg}")
                raise ValueError(error_msg)
            session_id = session_dir.name
        else:
            # Create new session directory
            if session_id is None:
                session_id = f"link_{str(uuid4())}"

            if sandbox_dir is None:
                sandbox_dir = "sandbox"

            session_dir = (Path(sandbox_dir) / session_id).absolute()
            session_dir.mkdir(parents=True, exist_ok=True)

        tools = get_session_tools(session_dir)
        print(f"✅ LinkerAgent: Initialized with {len(tools)} tools")

        instructions = (
            RECOMMENDED_PROMPT_PREFIX
            + "\n\n"
            + load_prompt("linker_agent.md", session_dir=str(session_dir))
            + "\n\n"
            + "IMPORTANT: At the end of your work, provide a structured summary using the LinkerOutput format. "
            + "Include all linked data objects, cleaned files, processing statistics, and sample IDs ready for curation. "
            + "Use the serialize_agent_output tool to persist your results as JSON files for inspection."
        )

        agent = Agent(
            name="LinkerAgent",
            instructions=instructions,
            tools=tools,
            handoffs=handoffs or [],
            # Following DendroForge pattern: no structured outputs, natural language responses
        )

        return agent

    except Exception as e:
        print(f"❌ LinkerAgent creation error: {str(e)}")
        import traceback

        print("🔍 LinkerAgent creation traceback:")
        traceback.print_exc()
        raise
