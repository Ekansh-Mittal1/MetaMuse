"""
Normalizer agent for metadata normalization tasks.

This module provides the NormalizerAgent that performs ontology normalization
on candidate values extracted by the CuratorAgent, mapping them to standard
biomedical ontology terms using semantic similarity search.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional
from uuid import uuid4

from agents import Agent, RunContextWrapper
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX
from pydantic import Field
from src.agents.handoff_base import BaseHandoff

from src.agents.tool_utils import get_normalizer_tools
from src.utils.prompts import load_prompt

# Import Pydantic models for structured data
from src.models import NormalizationResult, BatchNormalizationResult
from src.models.agent_outputs import CuratorOutput


class NormalizerHandoff(BaseHandoff):
    """Input to the NormalizerAgent."""

    sample_ids: list[str] = Field(
        ...,
        description="List of sample IDs to normalize (e.g., ['GSM1000981', 'GSM1021412']).",
    )
    target_field: str = Field(
        default="Disease",
        description="The target metadata field that was curated (e.g., 'Disease', 'Tissue', 'Age').",
    )
    session_directory: str = Field(
        ...,
        description="Path to the session directory containing candidates files.",
    )
    ontologies: Optional[list[str]] = Field(
        default=None,
        description="Specific ontologies to search (if None, uses defaults for target field).",
    )
    min_score: float = Field(
        default=0.5,
        description="Minimum similarity score threshold for ontology matches.",
    )


class SimpleNormalizerHandoff(BaseHandoff):
    """Simplified input to the NormalizerAgent that avoids complex nested structures."""

    sample_ids: list[str] = Field(
        ...,
        description="List of sample IDs to normalize (e.g., ['GSM1000981', 'GSM1021412']).",
    )
    target_field: str = Field(
        default="Disease",
        description="The target metadata field that was curated (e.g., 'Disease', 'Tissue', 'Age').",
    )
    session_directory: str = Field(
        ...,
        description="Path to the session directory containing candidates files.",
    )


def on_handoff_callback(ctx: RunContextWrapper[None], input_data: BaseHandoff):
    """A no-op callback to satisfy the handoff function's requirement."""
    pass


def create_normalizer_agent(
    session_id: str = None,
    sandbox_dir: str = None,
    handoffs: list = None,
    existing_session_dir: str = None,
    input_data: str = None,
    curator_output: Optional[CuratorOutput] = None,
) -> Agent:
    """
    Factory method to create a metadata normalization agent.

    This agent is responsible for performing ontology normalization tasks on
    candidate values extracted by the CuratorAgent, mapping them to standard
    biomedical ontology terms using semantic similarity search.

    The agent is configured with structured output capabilities to produce
    validated normalization results using the output_type parameter.

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
    curator_output : CuratorOutput, optional
        Complete structured output from the CuratorAgent. Used in pipeline mode.

    Returns
    -------
    Agent
        An instance of an Agent configured for metadata normalization tasks with structured outputs.
    """
    try:
        # Check if "testing" is in the input data
        is_testing = input_data and "testing" in input_data.lower()
        if is_testing:
            print("🧪 NormalizerAgent: Testing mode detected")
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
                print(f"❌ NormalizerAgent: {error_msg}")
                raise ValueError(error_msg)
            session_id = session_dir.name
        else:
            # Create new session directory
            if session_id is None:
                session_id = f"normalizer_{str(uuid4())}"

            if sandbox_dir is None:
                sandbox_dir = "sandbox"

            session_dir = (Path(sandbox_dir) / session_id).absolute()
            session_dir.mkdir(parents=True, exist_ok=True)

        # Store curator_output for the tool to access if provided
        if curator_output:
            # Store in a module-level variable that the tool can access
            import src.agents.normalizer as normalizer_module
            normalizer_module._curator_output = curator_output

        tools = get_normalizer_tools(session_dir)

        # Extract target field from input_data if available, otherwise default to "Disease"
        target_field = "Disease"  # Default
        ontologies = None  # Will use defaults based on target field
        min_score = 0.5  # Default threshold

        if input_data:
            # Try to extract target field from input_data string
            if "target_field=" in input_data.lower():
                # Simple parsing for development
                parts = input_data.split("target_field=")
                if len(parts) > 1:
                    target_field = parts[1].split()[0].strip("\"'")
            elif "target_field:" in input_data.lower():
                # Support colon format as well
                parts = input_data.split("target_field:")
                if len(parts) > 1:
                    target_field = parts[1].split()[0].strip("\"'")

            # Extract ontologies if specified
            if "ontologies=" in input_data.lower():
                parts = input_data.split("ontologies=")
                if len(parts) > 1:
                    ontology_str = parts[1].split()[0].strip("\"'")
                    ontologies = [o.strip() for o in ontology_str.split(",")]

            # Extract min_score if specified
            if "min_score=" in input_data.lower():
                parts = input_data.split("min_score=")
                if len(parts) > 1:
                    try:
                        min_score = float(parts[1].split()[0].strip("\"'"))
                    except ValueError:
                        pass

        # Load the base instructions for normalization
        base_instructions = load_prompt(
            "normalizer_agent.md", session_dir=str(session_dir)
        )

        # Create instructions with session-specific information
        instructions = (
            RECOMMENDED_PROMPT_PREFIX
            + "\n\n"
            + base_instructions
            + "\n\n## Session Information\n"
            + f"Session Directory: {session_dir}\n"
            + f"Session ID: {session_id}\n"
            + f"Target Field: {target_field}\n"
            + f"Ontologies: {ontologies or 'auto-detected based on target field'}\n"
            + f"Minimum Score Threshold: {min_score}\n"
        )

        agent = Agent(
            name="NormalizerAgent",
            instructions=instructions,
            tools=tools,
            handoffs=handoffs or [],
            output_type=BatchNormalizationResult,  # Structured output for batch normalization
        )

        return agent

    except Exception as e:
        import traceback

        print("🔍 NormalizerAgent creation traceback:")
        traceback.print_exc()
        raise


# Module-level variable to store curator_output for tool access
_curator_output: Optional[CuratorOutput] = None 