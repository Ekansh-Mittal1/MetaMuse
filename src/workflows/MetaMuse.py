from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from agents import Agent, handoff
from pydantic import Field

from src.agents.ingestion import create_ingestion_agent, IngestionHandoff
from src.agents.linker import create_linker_agent, LinkerHandoff


def create_extraction_pipeline(
    session_id: str = None, 
    sandbox_dir: str = None
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
        handoffs=[]  # Single agent pipeline, no handoffs needed
    )
    
    return agent


def create_linking_pipeline(
    session_id: str = None,
    sandbox_dir: str = None,
    existing_session_dir: str = None
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
            handoffs=[]  # Single agent pipeline, no handoffs needed
        )
    else:
        # Create new session directory
        if session_id is None:
            session_id = str(uuid4())
        
        if sandbox_dir is None:
            sandbox_dir = "sandbox"
        
        # Create the linker agent
        agent = create_linker_agent(
            session_id=session_id,
            sandbox_dir=sandbox_dir,
            handoffs=[]  # Single agent pipeline, no handoffs needed
        )
    
    return agent


def create_multi_agent_pipeline(
    session_id: str = None,
    sandbox_dir: str = None
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
        session_id = str(uuid4())
    
    if sandbox_dir is None:
        sandbox_dir = "sandbox"
    
    # Create the LinkerAgent first (it will be the handoff target)
    linker_agent = create_linker_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[]  # End of pipeline, no further handoffs
    )
    
    # Create the IngestionAgent with handoff to LinkerAgent
    ingestion_agent = create_ingestion_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=linker_agent,
                input_type=LinkerHandoff,
                on_handoff=on_handoff_callback,
            )
        ]
    )
    
    return ingestion_agent  # Return entry point


def on_handoff_callback(ctx, input_data):
    """Print the original request and any additional handoff fields for debugging."""
    print(f"[Handoff] original_request: {input_data.original_request}")
    for field_name, value in input_data.dict(exclude={"original_request"}).items():
        print(f"[Handoff] {field_name}: {value}")


def create_full_pipeline(
    session_id: str = None,
    sandbox_dir: str = None
) -> Agent:
    """
    Create a complete metadata extraction and linking pipeline.
    
    This is an alias for create_multi_agent_pipeline that provides a complete
    workflow from metadata extraction to linking and processing.
    
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
    return create_multi_agent_pipeline(session_id, sandbox_dir) 