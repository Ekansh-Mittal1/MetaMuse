"""
Normalizer agent for metadata normalization tasks.

This module provides the NormalizerAgent that performs ontology normalization
on candidate values extracted by the CuratorAgent, mapping them to standard
biomedical ontology terms using semantic similarity search.
"""

from __future__ import annotations

from pathlib import Path
import json
from typing import Optional
from uuid import uuid4

from agents import Agent, RunContextWrapper, Runner, RunConfig, ModelSettings
from openai.types.shared import Reasoning
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX
from agents.agent_output import AgentOutputSchema
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

        # No longer need to store curator_output in module - data passed in message

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
            output_type=BatchNormalizationResult,
        )

        return agent

    except Exception as e:
        import traceback

        print("🔍 NormalizerAgent creation traceback:")
        traceback.print_exc()
        raise


async def run_normalizer_agent(
    curator_output: CuratorOutput,
    target_field: str = "Disease",
    session_id: str = None,
    sandbox_dir: str = None,
    ontologies: Optional[list[str]] = None,
    min_score: float = 0.5,
    model_provider = None,
    max_tokens: int = 4096,
    max_turns: int = 100,
) -> BatchNormalizationResult:
    """
    Run the normalizer agent and return its structured output.
    
    This function creates a normalizer agent, runs it using Runner.run_streamed,
    and returns the final BatchNormalizationResult Pydantic model. This is part of the
    new deterministic workflow architecture where agents are decoupled.
    
    Parameters
    ----------
    curator_output : CuratorOutput
        The output from the curator agent containing extracted candidates
    target_field : str, optional
        The target metadata field that was curated (e.g., 'Disease', 'Tissue', 'Age')
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    ontologies : Optional[list[str]], optional
        Specific ontologies to search (if None, uses defaults for target field)
    min_score : float, optional
        Minimum similarity score threshold for ontology matches
        
    Returns
    -------
    BatchNormalizationResult
        The structured output from the normalizer agent containing ontology mappings
    """
    try:
        # Use the session directory from curator_output
        existing_session_dir = curator_output.session_directory
        
        # Extract sample IDs from curator output
        sample_ids = list(curator_output.sample_ids_requested)
        
        # Prepare input data for the normalizer
        input_data = f"target_field:{target_field} {' '.join(sample_ids)}"
        if ontologies:
            input_data += f" ontologies={','.join(ontologies)}"
        input_data += f" min_score={min_score}"
        
        # Create the normalizer agent without handoffs (decoupled)
        agent = create_normalizer_agent(
            session_id=session_id,
            sandbox_dir=sandbox_dir,
            handoffs=[],  # No handoffs in deterministic workflow
            existing_session_dir=existing_session_dir,
            input_data=input_data,
            curator_output=None,  # No longer needed - data passed in message
        )
        
        # Prepare run config if model provider is specified
        run_config = None
        if model_provider:
            extra_body = {"provider": {"order": ["google-vertex/us"]}}
            if max_tokens is not None:
                extra_body["max_tokens"] = max_tokens
                
            run_config = RunConfig(
                model_provider=model_provider,
                model_settings=ModelSettings(
                    max_tokens=max_tokens,
                    reasoning=Reasoning(effort="high"),
                    extra_body=extra_body,
                ),
            )
        
        # Save curator results to a file to be passed to the next agent
        curator_results_file = (
            Path(existing_session_dir) / "curator_results_for_normalization.json"
        )
        curation_results_data = []
        if curator_output.curation_results:
            curation_results_data = [
                result.model_dump() for result in curator_output.curation_results
            ]

        with open(curator_results_file, "w") as f:
            json.dump(curation_results_data, f, indent=2)

        # Create a simple message that includes the curation_results
        normalizer_message = (
            f"Please run normalization on the file: '{curator_results_file}' "
            f"for the target field '{target_field}'."
        )

        result = Runner.run_streamed(
            agent, normalizer_message, run_config=run_config, max_turns=max_turns
        )
        
        # Extract the final result with strict output
        final_result = None
        
        try:
            async for event in result.stream_events():
                # Handle raw response events (token-by-token streaming)
                if event.type == "raw_response_event":
                    # Check if this is a text delta event with actual content
                    if (
                        hasattr(event, "data")
                        and hasattr(event.data, "delta")
                        and event.data.delta is not None
                        and event.data.delta.strip()
                    ):
                        # Stream tokens naturally like ChatGPT with debug prefix
                        print(f"[NORMALIZER_AGENT_OUTPUT]: {event.data.delta}", end="", flush=True)
                    continue
                
                # Handle agent response events (final result)
                elif event.type == "agent_response_event":
                    print(f"\n✅ Found agent response event: {type(event.result)}")
                    final_result = event.result
                    break
                    
        except Exception as stream_error:
            print(f"\n❌ Stream error: {stream_error}")
            
        print("\n🔍 Streaming completed")
        
        
        try:
            final_result = result.final_output
            print(f"✅ Got final_output directly: {type(final_result)}")
        except Exception as e:
            print(f"❌ Could not get final_output: {e}")
            raise RuntimeError("No result received from curator agent")
            
        # Validate that we got a BatchNormalizationResult
        if not isinstance(final_result, BatchNormalizationResult):
            raise RuntimeError(f"Expected BatchNormalizationResult, got {type(final_result)}")
            
        print("✅ Normalizer agent completed with structured output")
        return final_result
        
    except Exception as e:
        print(f"❌ run_normalizer_agent error: {str(e)}")
        import traceback
        print("🔍 run_normalizer_agent traceback:")
        traceback.print_exc()
        raise


