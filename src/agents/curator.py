"""
Curator agent for metadata curation tasks.

This module provides the CuratorAgent that performs metadata curation
on GEO samples, extracting candidate values for specific metadata fields
and reconciling conflicts across multiple data sources.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Union
from uuid import uuid4

from agents import Agent, RunContextWrapper, Runner, RunConfig, ModelSettings
from openai.types.shared import Reasoning
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX
from pydantic import Field
from src.agents.handoff_base import BaseHandoff

from src.utils.prompts import load_prompt

# Import Pydantic models for structured data
from src.models import CuratorOutput, CurationDataPackage
from src.models.agent_outputs import LinkerOutput, SampleTypeCuratorOutput, DiseaseCuratorOutput, AssayTypeCuratorOutput, SexCuratorOutput, TreatmentCuratorOutput

# Module-level variable to store data_intake_output for tool access
_data_intake_output: Optional[LinkerOutput] = None


def get_curator_output_type_for_field(target_field: str):
    """
    Get the appropriate curator output type based on target field.

    Parameters
    ----------
    target_field : str
        The target metadata field

    Returns
    -------
    type
        The appropriate CuratorOutput model class
    """
    if target_field.lower() in ["sampletype", "sample_type"]:
        from src.models.agent_outputs import SampleTypeCuratorOutput
        return SampleTypeCuratorOutput
    elif target_field.lower() in ["assaytype", "assay_type"]:
        from src.models.agent_outputs import AssayTypeCuratorOutput
        return AssayTypeCuratorOutput
    elif target_field.lower() == "disease":
        from src.models.agent_outputs import DiseaseCuratorOutput
        return DiseaseCuratorOutput
    elif target_field.lower() == "sex":
        from src.models.agent_outputs import SexCuratorOutput
        return SexCuratorOutput
    elif target_field.lower() == "treatment":
        from src.models.agent_outputs import TreatmentCuratorOutput
        return TreatmentCuratorOutput
    from src.models.agent_outputs import CuratorOutput
    return CuratorOutput


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
    data_intake_output: Optional[LinkerOutput] = None,
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
    data_intake_output : LinkerOutput, optional
        Complete structured output from the data_intake workflow. Only provided when
        the hybrid pipeline is being invoked. Use get_data_intake_context() tool to access this data.

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

        # NO TOOLS NEEDED - Data passed directly in input
        tools = []

        # Load extraction template based on target field
        # Extract target field from input_data if available, otherwise default to "Disease"
        target_field = "Disease"  # Default
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
            else:
                # Check for specific field keywords in input_data
                input_lower = input_data.lower()
                if "disease" in input_lower:
                    target_field = "Disease"
                elif "tissue" in input_lower:
                    target_field = "Tissue"
                elif "age" in input_lower or "developmental" in input_lower:
                    target_field = "Age"
                elif "organ" in input_lower:
                    target_field = "Organ"
                elif (
                    "drug" in input_lower
                    or "medication" in input_lower
                    or "pharmaceutical" in input_lower
                ):
                    target_field = "Drug"
                elif (
                    "treatment" in input_lower
                    or "therapy" in input_lower
                    or "intervention" in input_lower
                ):
                    target_field = "Treatment"
                elif (
                    "organism" in input_lower
                    or "species" in input_lower
                    or "human" in input_lower
                    or "mouse" in input_lower
                ):
                    target_field = "Organism"
                elif (
                    "ethnicity" in input_lower
                    or "ethnic" in input_lower
                    or "race" in input_lower
                ):
                    target_field = "Ethnicity"
                elif (
                    "gender" in input_lower
                    or "sex" in input_lower
                    or "male" in input_lower
                    or "female" in input_lower
                ):
                    target_field = "Sex"
                elif (
                    "cell line" in input_lower
                    or "cellline" in input_lower
                    or "hela" in input_lower
                    or "hek" in input_lower
                ):
                    target_field = "Cell_Line"

        try:
            # Map target field to template filename
            template_mapping = {
                "Disease": "disease.md",
                "Tissue": "tissue.md",
                "Age": "age.md",
                "Organ": "organ.md",
                "Drug": "drug.md",
                "Treatment": "treatment.md",
                "Organism": "organism.md",
                "Ethnicity": "ethnicity.md",
                "Sex": "sex.md",
                "Cell_Line": "cell_line.md",
                "CellLine": "cell_line.md",
                "SampleType": "sample_type.md",
                "AssayType": "assay_type.md",
                "assay_type": "assay_type.md",
            }

            template_filename = template_mapping.get(
                target_field, f"{target_field.lower()}.md"
            )
            template_file = (
                Path(__file__).parent.parent
                / "prompts"
                / "extraction_templates"
                / template_filename
            )

            if template_file.exists():
                with open(template_file, "r", encoding="utf-8") as f:
                    extraction_template = f.read()
                # Loaded extraction template for target field
            else:
                print(
                    f"⚠️  No extraction template found for {target_field}, using generic template"
                )
                extraction_template = "# Generic Extraction Template\nExtract relevant candidates for the target field."
        except Exception as e:
            print(f"⚠️  Could not load extraction template for {target_field}: {e}")
            extraction_template = "# Generic Extraction Template\nExtract relevant candidates for the target field."

        # Use optimized prompt - more context-aware and sample-specific
        base_instructions = load_prompt(
            "curator_agent_optimized.md", session_dir=str(session_dir)
        )

        instructions = (
            RECOMMENDED_PROMPT_PREFIX
            + "\n\n"
            + base_instructions.replace("{EXTRACTION_TEMPLATE}", extraction_template)
            + "\n\n## Session Information\n"
            + f"Session Directory: {session_dir}\n"
            + f"Session ID: {session_id}\n"
        )

        # Get appropriate output type based on target field
        output_type = get_curator_output_type_for_field(target_field)

        agent = Agent(
            name="CuratorAgent",
            instructions=instructions,
            tools=tools,
            handoffs=handoffs or [],
            output_type=output_type,
        )
        agent.strict_output = True

        return agent

    except Exception as e:
        print(f"❌ CuratorAgent creation error: {str(e)}")
        import traceback

        print("🔍 CuratorAgent creation traceback:")
        traceback.print_exc()
        raise


async def run_curator_agent(
    data_intake_output: LinkerOutput,
    target_field: str = "Disease",
    session_id: str = None,
    sandbox_dir: str = None,
    model_provider=None,
    max_tokens: int = None,
    max_turns: int = 100,
    verbose_output: bool = False,
    guidance: dict | None = None,
) -> Union[CuratorOutput, SampleTypeCuratorOutput, DiseaseCuratorOutput, AssayTypeCuratorOutput, SexCuratorOutput, TreatmentCuratorOutput]:
    """
    Run the curator agent and return its structured output.

    This function creates a curator agent, runs it using Runner.run_streamed,
    and returns the final CuratorOutput Pydantic model. This is part of the
    new deterministic workflow architecture where agents are decoupled.

    Parameters
    ----------
    data_intake_output : LinkerOutput
        The output from the data intake workflow containing linked metadata
    target_field : str, optional
        The target metadata field to extract candidates for (e.g., 'Disease', 'Tissue', 'Age')
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    verbose_output : bool, optional
        Whether to print streaming output during agent execution. Defaults to False.

    Returns
    -------
    CuratorOutput
        The structured output from the curator agent containing extraction candidates
    """
    try:
        # Use the session directory from data_intake_output
        existing_session_dir = data_intake_output.session_directory

        # Prepare input data for the curator
        sample_ids = data_intake_output.sample_ids_for_curation
        if not sample_ids:
            error_msg = "No sample IDs available for curation in data_intake_output"
            print(f"❌ {error_msg}")
            print(f"🔍 DEBUG: data_intake_output attributes: {dir(data_intake_output)}")
            raise ValueError(error_msg)
            
        input_data = f"target_field:{target_field} {' '.join(sample_ids)}"

        # Create the curator agent without handoffs (decoupled)
        agent = create_curator_agent(
            session_id=session_id,
            sandbox_dir=sandbox_dir,
            handoffs=[],  # No handoffs in deterministic workflow
            existing_session_dir=existing_session_dir,
            input_data=input_data,
            data_intake_output=data_intake_output,
        )

        # Agent is already configured with strict CuratorOutput

        # Prepare the input message with only the curation packages (not the entire data intake output)
        curation_packages_json = ""
        if data_intake_output.curation_packages:
            curation_packages_data = [
                pkg.model_dump() for pkg in data_intake_output.curation_packages
            ]
            curation_packages_json = json.dumps(curation_packages_data, indent=2)
        else:
            print(f"⚠️  WARNING: No curation packages found in data_intake_output")

        curator_message = (
            f"Please curate metadata for the target field '{target_field}' using the following curation packages:\n\n"
            f"{curation_packages_json}\n\n"
            f"Extract candidates from the provided metadata for samples: {', '.join(sample_ids)}. "
            f"Process all metadata internally and return a CuratorOutput object."
        )
        if guidance:
            try:
                import json as _json
                curator_message += ("\n\nAdditional guidance for specific samples (if present):\n" +
                    _json.dumps(guidance, indent=2))
            except Exception:
                pass

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

        # Run the agent using Runner.run_streamed
        result = Runner.run_streamed(
            agent, curator_message, run_config=run_config, max_turns=max_turns
        )

        # Extract the final result with strict output
        final_result = None
        stream_events_processed = 0
        raw_response_content = ""

        try:
            async for event in result.stream_events():
                stream_events_processed += 1
                
                # Handle raw response events (token-by-token streaming)
                if event.type == "raw_response_event":
                    # Check if this is a text delta event with actual content
                    if (
                        hasattr(event, "data")
                        and hasattr(event.data, "delta")
                        and event.data.delta is not None
                        and event.data.delta.strip()
                    ):
                        # Stream tokens naturally like ChatGPT
                        if verbose_output:
                            print(event.data.delta, end="", flush=True)
                        raw_response_content += event.data.delta
                    continue

                # Handle agent response events (final result)
                elif event.type == "agent_response_event":
                    final_result = event.result
                    break

        except Exception as stream_error:
            print(f"\n❌ Stream error: {stream_error}")
            print(f"🔍 DEBUG: Stream error type: {type(stream_error)}")
            print(f"🔍 DEBUG: Stream error traceback:")
            import traceback
            traceback.print_exc()
            print(f"🔍 DEBUG: Raw response content collected: {raw_response_content[:500]}...")

        try:
            if final_result is None:
                final_result = result.final_output
        except Exception as e:
            print(f"❌ Could not get final_output: {e}")
            print(f"🔍 DEBUG: final_output error type: {type(e)}")
            print(f"🔍 DEBUG: final_output error traceback:")
            import traceback
            traceback.print_exc()
            print(f"🔍 DEBUG: Stream events processed: {stream_events_processed}")
            print(f"🔍 DEBUG: Raw response content: {raw_response_content[:1000]}...")
            raise RuntimeError("No result received from curator agent")

        # Validate that we got a CuratorOutput, SampleTypeCuratorOutput, DiseaseCuratorOutput, or AssayTypeCuratorOutput
        from src.models.agent_outputs import CuratorOutput, SampleTypeCuratorOutput, DiseaseCuratorOutput, AssayTypeCuratorOutput, SexCuratorOutput, TreatmentCuratorOutput
        
        if not isinstance(final_result, (CuratorOutput, SampleTypeCuratorOutput, DiseaseCuratorOutput, AssayTypeCuratorOutput, SexCuratorOutput, TreatmentCuratorOutput)):
            error_msg = f"Expected CuratorOutput, SampleTypeCuratorOutput, DiseaseCuratorOutput, AssayTypeCuratorOutput, SexCuratorOutput, or TreatmentCuratorOutput, got {type(final_result)}"
            print(f"❌ {error_msg}")
            print(f"🔍 DEBUG: Final result content: {final_result}")
            print(f"🔍 DEBUG: Raw response content: {raw_response_content[:1000]}...")
            raise RuntimeError(error_msg)

        # Check if the result has the expected structure
        if not hasattr(final_result, "curation_results"):
            error_msg = f"CuratorOutput missing 'curation_results' attribute"
            print(f"❌ {error_msg}")
            print(f"🔍 DEBUG: Available attributes: {dir(final_result)}")
            print(f"🔍 DEBUG: Final result content: {final_result}")
            raise RuntimeError(error_msg)
            
        if not final_result.curation_results:
            error_msg = f"CuratorOutput has empty 'curation_results'"
            print(f"❌ {error_msg}")
            print(f"🔍 DEBUG: curation_results: {final_result.curation_results}")
            print(f"🔍 DEBUG: Final result content: {final_result}")
            print(f"🔍 DEBUG: Raw response content: {raw_response_content[:1000]}...")
            raise RuntimeError(error_msg)

        # FIX: Ensure session_directory is correct (LLM sometimes mixes session ID with GSM sample ID)
        correct_session_dir = str(Path(existing_session_dir).absolute())
        if final_result.session_directory != correct_session_dir:
            final_result.session_directory = correct_session_dir

        return final_result

    except Exception as e:
        print(f"❌ run_curator_agent error: {str(e)}")
        print(f"🔍 DEBUG: Error type: {type(e)}")
        print(f"🔍 DEBUG: Error occurred in run_curator_agent for target_field='{target_field}'")
        import traceback

        print("🔍 run_curator_agent traceback:")
        traceback.print_exc()

        raise
