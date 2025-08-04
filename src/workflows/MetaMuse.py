from __future__ import annotations

from uuid import uuid4

from agents import Agent, handoff

from src.agents.ingestion import create_ingestion_agent
from src.agents.linker import create_linker_agent, LinkerHandoff
from src.agents.curator import (
    create_curator_agent,
    CuratorHandoff,
)
from src.agents.normalizer import (
    create_normalizer_agent,
    NormalizerHandoff,
)
from src.workflows.data_intake import run_data_intake_workflow
from src.workflows.deterministic_workflow import (
    run_deterministic_workflow_sync,
)


# New deterministic workflow function - recommended approach
def create_deterministic_pipeline(
    target_field: str = "Disease",
    session_id: str = None,
    sandbox_dir: str = None,
    ontologies: list[str] = None,
    min_score: float = 0.5,
):
    """
    Create a deterministic metadata processing pipeline.

    This is the new recommended approach that uses the deterministic workflow
    architecture where agents are completely decoupled and run independently
    using Runner.run_streamed. Data is serialized between each step.

    The workflow is: data_intake -> curator_agent -> normalizer_agent

    Parameters
    ----------
    target_field : str, optional
        The target metadata field to extract and normalize (e.g., 'Disease', 'Tissue', 'Age')
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    ontologies : list[str], optional
        Specific ontologies to search during normalization
    min_score : float, optional
        Minimum similarity score threshold for ontology matches

    Returns
    -------
    function
        A function that accepts input_text and returns the complete workflow results
    """

    def workflow_function(input_text: str):
        """
        Execute the deterministic workflow with the given input.

        Parameters
        ----------
        input_text : str
            Input text containing GEO IDs for processing

        Returns
        -------
        Dict[str, Any]
            Complete workflow results containing outputs from all stages
        """
        return run_deterministic_workflow_sync(
            input_text=input_text,
            target_field=target_field,
            session_id=session_id,
            sandbox_dir=sandbox_dir,
            ontologies=ontologies,
            min_score=min_score,
        )

    return workflow_function


# LEGACY FUNCTIONS - Use create_deterministic_pipeline() for new projects


def create_extraction_pipeline(
    session_id: str = None, sandbox_dir: str = None
) -> Agent:
    """
    Create a metadata extraction pipeline.

    This pipeline creates a single-agent workflow for extracting metadata
    from GEO and PubMed databases. The agent can handle multiple types of
    identifiers and extraction requests.

    Parameters
    ----------
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"

    Returns
    -------
    Agent
        The configured ingestion agent ready for metadata extraction
    """
    if session_id is None:
        session_id = str(uuid4())

    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    # Create the ingestion agent
    agent = create_ingestion_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[],  # Single agent pipeline, no handoffs needed
    )

    return agent


def create_linking_pipeline(
    session_id: str = None,
    sandbox_dir: str = None,
    existing_session_dir: str = None,
    input_data: str = None,
) -> Agent:
    """
    Create a metadata linking pipeline.

    This pipeline creates a single LinkerAgent workflow for processing
    and linking metadata files created by the IngestionAgent.

    Parameters
    ----------
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    existing_session_dir : str, optional
        Path to an existing session directory to use instead of creating a new one

    Returns
    -------
    Agent
        The configured linker agent ready for metadata linking
    """
    if existing_session_dir:
        # Use existing session directory
        agent = create_linker_agent(
            existing_session_dir=existing_session_dir,
            handoffs=[],  # Single agent pipeline, no handoffs needed
            input_data=input_data,
        )
    else:
        # Create new session directory
        if session_id is None:
            session_id = f"mm_link_{str(uuid4())}"

        if sandbox_dir is None:
            sandbox_dir = "sandbox"

        # Create the linker agent
        agent = create_linker_agent(
            session_id=session_id,
            sandbox_dir=sandbox_dir,
            handoffs=[],  # Single agent pipeline, no handoffs needed
            input_data=input_data,
        )

    return agent


def create_multi_agent_pipeline(
    session_id: str = None, sandbox_dir: str = None, input_data: str = None
) -> Agent:
    """
    Create a multi-agent metadata extraction and linking pipeline.

    This pipeline chains together the IngestionAgent and LinkerAgent to provide
    a complete workflow from metadata extraction to linking and processing.

    Parameters
    ----------
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"

    Returns
    -------
    Agent
        The initial agent in the pipeline (IngestionAgent)
    """
    if session_id is None:
        session_id = f"mm_ext_{str(uuid4())}"
    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    # Parse input_data for multiple sample IDs (comma or whitespace separated)
    sample_ids = []
    if input_data:
        # Accept comma or whitespace separated
        for part in input_data.replace(",", " ").split():
            if part.strip():
                sample_ids.append(part.strip())

    # Create a single LinkerAgent that can handle multiple samples
    linker_agent = create_linker_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[],
        input_data=input_data,  # Pass the full input_data so it knows about all samples
    )

    # Create the IngestionAgent with handoff to the single LinkerAgent
    ingestion_agent = create_ingestion_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=linker_agent,
                input_type=LinkerHandoff,
                on_handoff=on_handoff_callback,
            )
        ],
    )

    return ingestion_agent  # Return entry point


def on_handoff_callback(ctx, input_data):
    """Print the original request and any additional handoff fields for debugging."""
    print(f"[Handoff] original_request: {input_data.original_request}")
    print(f"[Handoff] handoff_type: {type(input_data).__name__}")
    print(f"[Handoff] handoff_data: {input_data.model_dump()}")
    for field_name, value in input_data.model_dump(
        exclude={"original_request"}
    ).items():
        if hasattr(value, "model_dump"):  # Pydantic object
            print(f"[Handoff] {field_name}: <Pydantic {type(value).__name__}>")
        else:
            print(f"[Handoff] {field_name}: {value}")
    print("[Handoff] Handoff callback executed successfully")


def create_curation_pipeline(
    session_id: str = None,
    sandbox_dir: str = None,
    existing_session_dir: str = None,
    input_data: str = None,
) -> Agent:
    """
    Create a metadata curation pipeline.

    This pipeline creates a single CuratorAgent workflow for performing
    metadata curation tasks on processed samples from the LinkerAgent.

    Parameters
    ----------
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    existing_session_dir : str, optional
        Path to an existing session directory to use instead of creating a new one
    input_data : str, optional
        Input data string that may contain sample IDs and target field information

    Returns
    -------
    Agent
        The configured curator agent ready for metadata curation
    """
    if existing_session_dir:
        # Use existing session directory
        agent = create_curator_agent(
            existing_session_dir=existing_session_dir,
            handoffs=[],  # Single agent pipeline, no handoffs needed
            input_data=input_data,
        )
    else:
        # Create new session directory
        if session_id is None:
            session_id = f"mm_curator_{str(uuid4())}"

        if sandbox_dir is None:
            sandbox_dir = "sandbox"

        # Create the curator agent
        agent = create_curator_agent(
            session_id=session_id,
            sandbox_dir=sandbox_dir,
            handoffs=[],  # Single agent pipeline, no handoffs needed
            input_data=input_data,
        )

    return agent


def create_complete_pipeline(
    session_id: str = None, sandbox_dir: str = None, input_data: str = None
) -> Agent:
    """
    Create a complete metadata extraction, linking, and curation pipeline.

    This pipeline chains together the IngestionAgent, LinkerAgent, and CuratorAgent
    to provide a complete workflow from metadata extraction to final curation.

    Parameters
    ----------
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    input_data : str, optional
        Input data string that may contain sample IDs and target field information

    Returns
    -------
    Agent
        The initial agent in the pipeline (IngestionAgent)
    """
    if session_id is None:
        session_id = f"mm_complete_{str(uuid4())}"
    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    # Parse input_data for multiple sample IDs and target field
    sample_ids = []
    target_field = "Disease"  # Default target field

    if input_data:
        # Look for target field specification (support both : and = formats)
        if "target_field:" in input_data.lower():
            parts = input_data.split("target_field:")
            if len(parts) > 1:
                target_field = parts[1].split()[0].strip()
                input_data = parts[0].strip()
        elif "target_field=" in input_data.lower():
            parts = input_data.split("target_field=")
            if len(parts) > 1:
                target_field = parts[1].split()[0].strip()
                input_data = parts[0].strip()

        # Accept comma or whitespace separated sample IDs
        for part in input_data.replace(",", " ").split():
            if part.strip() and part.strip().startswith("GSM"):
                sample_ids.append(part.strip())

    # Create CuratorAgent
    curator_agent = create_curator_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[],
        input_data=f"target_field:{target_field} {' '.join(sample_ids)}",
    )

    # Create LinkerAgent with handoff to CuratorAgent
    linker_agent = create_linker_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=curator_agent,
                input_type=CuratorHandoff,
                on_handoff=on_handoff_callback,
            )
        ],
        input_data=input_data,
    )

    # Create IngestionAgent with handoff to LinkerAgent
    ingestion_agent = create_ingestion_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=linker_agent,
                input_type=LinkerHandoff,
                on_handoff=on_handoff_callback,
            )
        ],
    )

    return ingestion_agent  # Return entry point


def create_structured_pipeline(
    session_id: str = None, sandbox_dir: str = None, input_data: str = None
) -> Agent:
    """
    Create a fully structured multi-agent pipeline with Pydantic objects.

    This pipeline demonstrates the new Pydantic-based approach where agents
    produce validated structured outputs that are passed seamlessly between
    agents, with JSON serialization only at the end for human inspection.

    Parameters
    ----------
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    input_data : str, optional
        Input data containing sample IDs and parameters

    Returns
    -------
    Agent
        The initial agent in the structured pipeline (IngestionAgent)
    """
    if session_id is None:
        session_id = f"mm_struct_{str(uuid4())}"
    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    # Parse sample IDs from input_data
    sample_ids = []
    if input_data:
        for part in input_data.replace(",", " ").split():
            if part.strip():
                sample_ids.append(part.strip())

    # Create CuratorAgent with structured output
    curator_agent = create_curator_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[],
        input_data=input_data,
    )

    # Create LinkerAgent with handoff to CuratorAgent
    linker_agent = create_linker_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=curator_agent,
                input_type=CuratorHandoff,
                on_handoff=on_structured_handoff_callback,
            )
        ],
        input_data=input_data,
    )

    # Create IngestionAgent with handoff to LinkerAgent
    ingestion_agent = create_ingestion_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=linker_agent,
                input_type=LinkerHandoff,
                on_handoff=on_structured_handoff_callback,
            )
        ],
    )

    return ingestion_agent


def on_structured_handoff_callback(ctx, input_data):
    """Enhanced handoff callback for structured Pydantic workflows."""
    # Log structured data presence for debugging if needed
    pass


def create_hybrid_pipeline(
    session_id: str = None, sandbox_dir: str = None, input_data: str = None
) -> Agent:
    """
    Create a hybrid pipeline that uses deterministic data_intake.py followed by CuratorAgent.

    This pipeline combines the reliability of deterministic processing with the intelligence
    of the curator agent for metadata curation.

    Parameters
    ----------
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    input_data : str, optional
        Input data string that may contain sample IDs and target field information

    Returns
    -------
    Agent
        The curator agent that will process the deterministic output
    """
    if session_id is None:
        session_id = f"mm_hybrid_{str(uuid4())}"
    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    # Parse input_data for multiple sample IDs and target field
    sample_ids = []
    target_field = "Disease"  # Default target field
    geo_input = input_data

    if input_data:
        # Look for target field specification
        if "target_field:" in input_data.lower():
            parts = input_data.split("target_field:")
            if len(parts) > 1:
                target_field = parts[1].split()[0].strip()
                geo_input = parts[0].strip()

        # Extract sample IDs for curator
        for part in geo_input.replace(",", " ").split():
            if part.strip() and part.strip().startswith("GSM"):
                sample_ids.append(part.strip())

    # Running deterministic data_intake workflow

    # Run the deterministic data_intake workflow first
    data_intake_result = run_data_intake_workflow(
        input_text=geo_input,
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        workflow_type="complete",
    )

    if not data_intake_result.success:
        raise RuntimeError(f"Data intake workflow failed: {data_intake_result.message}")

    # Create CuratorAgent that will process the deterministic output
    # Note: The CuratorAgent will only have access to curation-specific tools,
    # not the data intake tools, since the data is already available from the data_intake workflow
    existing_session_dir = data_intake_result.session_directory
    sample_ids_for_curation = data_intake_result.sample_ids_for_curation

    # Pass the LinkerOutput data directly to the CuratorAgent via data_intake_output parameter

    # Create clean input_data without the LinkerOutput JSON
    curator_input_data = (
        f"target_field:{target_field} {' '.join(sample_ids_for_curation)}"
    )

    # Create CuratorAgent with the LinkerOutput data via data_intake_output parameter
    curator_agent = create_curator_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[],
        existing_session_dir=existing_session_dir,
        input_data=curator_input_data,
        data_intake_output=data_intake_result,
    )

    return curator_agent


def create_enhanced_hybrid_pipeline(
    session_id: str = None, sandbox_dir: str = None, input_data: str = None
) -> Agent:
    """
    Create an enhanced hybrid pipeline that includes normalization after curation.

    This pipeline combines deterministic data_intake.py followed by CuratorAgent
    and then NormalizerAgent for complete metadata processing workflow.

    Parameters
    ----------
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    input_data : str, optional
        Input data string that may contain sample IDs and target field information

    Returns
    -------
    Agent
        The curator agent that will process the deterministic output and hand off to normalizer
    """
    if session_id is None:
        session_id = f"mm_enhanced_{str(uuid4())}"
    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    # Parse input_data for multiple sample IDs and target field
    sample_ids = []
    target_field = "Disease"  # Default target field
    geo_input = input_data

    if input_data:
        # Look for target field specification
        if "target_field:" in input_data.lower():
            parts = input_data.split("target_field:")
            if len(parts) > 1:
                target_field = parts[1].split()[0].strip()
                geo_input = parts[0].strip()

        # Extract sample IDs for curator
        for part in geo_input.replace(",", " ").split():
            if part.strip() and part.strip().startswith("GSM"):
                sample_ids.append(part.strip())

    # Running deterministic data_intake workflow

    # Run the deterministic data_intake workflow first
    data_intake_result = run_data_intake_workflow(
        input_text=geo_input,
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        workflow_type="complete",
    )

    if not data_intake_result.success:
        raise RuntimeError(f"Data intake workflow failed: {data_intake_result.message}")

    # Create session variables
    existing_session_dir = data_intake_result.session_directory
    sample_ids_for_curation = data_intake_result.sample_ids_for_curation

    # Create clean input_data without the LinkerOutput JSON
    curator_input_data = (
        f"target_field:{target_field} {' '.join(sample_ids_for_curation)}"
    )

    # Create NormalizerAgent
    normalizer_agent = create_normalizer_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[],
        existing_session_dir=existing_session_dir,
        input_data=curator_input_data,
    )

    # Create CuratorAgent with handoff to NormalizerAgent
    curator_agent = create_curator_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=normalizer_agent,
                input_type=NormalizerHandoff,
                on_handoff=on_handoff_callback,
            )
        ],
        existing_session_dir=existing_session_dir,
        input_data=curator_input_data,
        data_intake_output=data_intake_result,
    )

    return curator_agent


def create_full_pipeline(
    session_id: str = None, sandbox_dir: str = None, input_data: str = None
) -> Agent:
    """
    Create a complete metadata extraction, linking, and curation pipeline.

    This pipeline chains together the IngestionAgent, LinkerAgent, and CuratorAgent
    to provide a complete workflow from metadata extraction to final curation.

    Parameters
    ----------
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    input_data : str, optional
        Input data string that may contain sample IDs and target field information

    Returns
    -------
    Agent
        The initial agent in the pipeline (IngestionAgent)
    """
    if session_id is None:
        session_id = f"mm_fp_{str(uuid4())}"
    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    # Parse input_data for multiple sample IDs and target field
    sample_ids = []
    target_field = "Disease"  # Default target field

    if input_data:
        # Look for target field specification (support both : and = formats)
        if "target_field:" in input_data.lower():
            parts = input_data.split("target_field:")
            if len(parts) > 1:
                target_field = parts[1].split()[0].strip()
                input_data = parts[0].strip()
        elif "target_field=" in input_data.lower():
            parts = input_data.split("target_field=")
            if len(parts) > 1:
                target_field = parts[1].split()[0].strip()
                input_data = parts[0].strip()

        # Accept comma or whitespace separated sample IDs
        for part in input_data.replace(",", " ").split():
            if part.strip() and part.strip().startswith("GSM"):
                sample_ids.append(part.strip())

    # Create CuratorAgent
    curator_agent = create_curator_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[],
        input_data=f"target_field:{target_field} {' '.join(sample_ids)}",
    )

    # Create LinkerAgent with handoff to CuratorAgent using CuratorHandoff
    # Pass the target field information to the LinkerAgent
    linker_input_data = (
        f"target_field:{target_field} {input_data}"
        if input_data
        else f"target_field:{target_field}"
    )
    linker_agent = create_linker_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=curator_agent,
                input_type=CuratorHandoff,
                on_handoff=on_handoff_callback,
            )
        ],
        input_data=linker_input_data,
    )

    print("🔗 LinkerAgent configured with handoff to CuratorAgent")
    print("📋 Handoff type: CuratorHandoff")
    print(f"🎯 Target field: {target_field}")
    print(f"📁 Sample IDs: {sample_ids}")

    # Create IngestionAgent with handoff to LinkerAgent
    # Pass the target field information to the IngestionAgent
    ingestion_input_data = (
        f"target_field:{target_field} {input_data}"
        if input_data
        else f"target_field:{target_field}"
    )
    ingestion_agent = create_ingestion_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=linker_agent,
                input_type=LinkerHandoff,
                on_handoff=on_handoff_callback,
            )
        ],
        input_data=ingestion_input_data,
    )

    return ingestion_agent  # Return entry point


def create_enhanced_full_pipeline(
    session_id: str = None, sandbox_dir: str = None, input_data: str = None
) -> Agent:
    """
    Create a complete metadata extraction, linking, curation, and normalization pipeline.

    This pipeline chains together the IngestionAgent, LinkerAgent, CuratorAgent, and NormalizerAgent
    to provide a complete workflow from metadata extraction to final ontology normalization.

    Parameters
    ----------
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    input_data : str, optional
        Input data string that may contain sample IDs and target field information

    Returns
    -------
    Agent
        The initial agent in the pipeline (IngestionAgent)
    """
    if session_id is None:
        session_id = f"mm_efp_{str(uuid4())}"
    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    # Parse input_data for multiple sample IDs and target field
    sample_ids = []
    target_field = "Disease"  # Default target field

    if input_data:
        # Look for target field specification (support both : and = formats)
        if "target_field:" in input_data.lower():
            parts = input_data.split("target_field:")
            if len(parts) > 1:
                target_field = parts[1].split()[0].strip()
                input_data = parts[0].strip()
        elif "target_field=" in input_data.lower():
            parts = input_data.split("target_field=")
            if len(parts) > 1:
                target_field = parts[1].split()[0].strip()
                input_data = parts[0].strip()

        # Accept comma or whitespace separated sample IDs
        for part in input_data.replace(",", " ").split():
            if part.strip() and part.strip().startswith("GSM"):
                sample_ids.append(part.strip())

    # Create NormalizerAgent (end of pipeline)
    normalizer_agent = create_normalizer_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[],
        input_data=f"target_field:{target_field} {' '.join(sample_ids)}",
    )

    # Create CuratorAgent with handoff to NormalizerAgent
    curator_agent = create_curator_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=normalizer_agent,
                input_type=NormalizerHandoff,
                on_handoff=on_handoff_callback,
            )
        ],
        input_data=f"target_field:{target_field} {' '.join(sample_ids)}",
    )

    # Create LinkerAgent with handoff to CuratorAgent using CuratorHandoff
    # Pass the target field information to the LinkerAgent
    linker_input_data = (
        f"target_field:{target_field} {input_data}"
        if input_data
        else f"target_field:{target_field}"
    )
    linker_agent = create_linker_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=curator_agent,
                input_type=CuratorHandoff,
                on_handoff=on_handoff_callback,
            )
        ],
        input_data=linker_input_data,
    )

    # LinkerAgent configured with handoff to CuratorAgent → NormalizerAgent

    # Create IngestionAgent with handoff to LinkerAgent
    # Pass the target field information to the IngestionAgent
    ingestion_input_data = (
        f"target_field:{target_field} {input_data}"
        if input_data
        else f"target_field:{target_field}"
    )
    ingestion_agent = create_ingestion_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=linker_agent,
                input_type=LinkerHandoff,
                on_handoff=on_handoff_callback,
            )
        ],
        input_data=ingestion_input_data,
    )

    return ingestion_agent  # Return entry point
