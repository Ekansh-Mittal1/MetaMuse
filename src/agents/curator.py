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
from typing import Optional
from src.agents.handoff_base import BaseHandoff

from src.agents.tool_utils import get_session_tools
from src.utils.prompts import load_prompt

# Import Pydantic models for structured data  
from src.models import (
    LinkerOutput, 
    CuratorOutput,
    CurationDataPackage,
    CurationResult,
    ExtractedCandidate
)


class CuratorHandoff(BaseHandoff):
    """Input to the CuratorAgent."""

    curation_packages: list[CurationDataPackage] = Field(
        ...,
        description="List of CurationDataPackage objects containing cleaned metadata from all sources.",
    )
    target_field: str = Field(
        default="Disease",
        description="The target metadata field to extract candidates for (e.g., 'Disease', 'Tissue', 'Age').",
    )
    session_directory: str = Field(
        ...,
        description="Path to the session directory for saving output files.",
    )
    
    # Following DendroForge pattern: no complex nested structures in handoffs
    # linker_output: Optional[LinkerOutput] = Field(
    #     default=None,
    #     description="Complete structured output from the LinkerAgent, containing linked and cleaned metadata",
    # )


class SimpleCuratorHandoff(BaseHandoff):
    """Simplified input to the CuratorAgent that avoids Dict[str, Any] content fields."""

    sample_ids: list[str] = Field(
        ...,
        description="List of sample IDs to curate (e.g., ['GSM1000981', 'GSM1021412']).",
    )
    target_field: str = Field(
        default="Disease",
        description="The target metadata field to extract candidates for (e.g., 'Disease', 'Tissue', 'Age').",
    )
    session_directory: str = Field(
        ...,
        description="Path to the session directory for saving output files.",
    )
    
    # Note: We don't include the full CurationDataPackage objects here
    # The CuratorAgent will load the data it needs from the session directory
    # This avoids the Dict[str, Any] content fields that cause schema validation issues


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
    
    The agent is configured with structured output capabilities to produce
    validated CuratorOutput objects directly using the output_type parameter.

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
        An instance of an Agent configured for metadata curation tasks with structured outputs.
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

        # Load extraction template based on target field
        # Extract target field from input_data if available, otherwise default to "Disease"
        target_field = "Disease"  # Default
        if input_data:
            # Try to extract target field from input_data string
            if "target_field=" in input_data.lower():
                # Simple parsing for development
                parts = input_data.split("target_field=")
                if len(parts) > 1:
                    target_field = parts[1].split()[0].strip('"\'')
            elif "target_field:" in input_data.lower():
                # Support colon format as well
                parts = input_data.split("target_field:")
                if len(parts) > 1:
                    target_field = parts[1].split()[0].strip('"\'')
            elif "disease" in input_data.lower():
                target_field = "Disease"
            elif "tissue" in input_data.lower():
                target_field = "Tissue"
            elif "age" in input_data.lower():
                target_field = "Age"
            elif "organ" in input_data.lower():
                target_field = "Organ"
        
        try:
            template_file = Path(__file__).parent.parent / "prompts" / "extraction_templates" / f"{target_field.lower()}.md"
            if template_file.exists():
                with open(template_file, 'r', encoding='utf-8') as f:
                    extraction_template = f.read()
            else:
                extraction_template = "# Generic Extraction Template\nExtract relevant candidates for the target field."
        except Exception as e:
            print(f"⚠️  Could not load extraction template for {target_field}: {e}")
            extraction_template = "# Generic Extraction Template\nExtract relevant candidates for the target field."

        # Load the base instructions and inject the template
        base_instructions = load_prompt("curator_agent_v2.md", session_dir=str(session_dir))
        instructions = (
            RECOMMENDED_PROMPT_PREFIX
            + "\n\n"
            + base_instructions.replace("{EXTRACTION_TEMPLATE}", extraction_template)
        )

        agent = Agent(
            name="CuratorAgent",
            instructions=instructions,
            tools=tools,
            handoffs=handoffs or [],
            output_type=CuratorOutput
        )

        return agent

    except Exception as e:
        print(f"❌ CuratorAgent creation error: {str(e)}")
        import traceback

        print("🔍 CuratorAgent creation traceback:")
        traceback.print_exc()
        raise 