from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from agents import Agent, handoff
from pydantic import Field

from src.agents.geo_ingestion import create_geo_ingestion_agent, GeoIngestionHandoff


def create_geo_extraction_pipeline(
    session_id: str = None, 
    sandbox_dir: str = None
) -> Agent:
    """
    Create a GEO metadata extraction pipeline.
    
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
        The configured GEO ingestion agent ready for metadata extraction
    """
    if session_id is None:
        session_id = str(uuid4())
    
    if sandbox_dir is None:
        sandbox_dir = "sandbox"
    
    # Create the GEO ingestion agent
    geo_agent = create_geo_ingestion_agent(
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        handoffs=[]  # Single agent pipeline, no handoffs needed
    )
    
    return geo_agent


def create_multi_agent_geo_pipeline(
    session_id: str = None,
    sandbox_dir: str = None
) -> Agent:
    """
    Create a multi-agent GEO metadata extraction pipeline.
    
    This pipeline can be extended to include multiple agents for different
    types of processing (e.g., validation, extraction, analysis, reporting).
    Currently returns the same single agent but provides a foundation for
    future multi-agent workflows.
    
    Parameters
    ----------
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
        
    Returns
    -------
    Agent
        The initial agent in the pipeline
    """
    if session_id is None:
        session_id = str(uuid4())
    
    if sandbox_dir is None:
        sandbox_dir = "sandbox"
    
    # For now, return the single GEO ingestion agent
    # In the future, this could be extended to include:
    # - Validation agent
    # - Extraction agent  
    # - Analysis agent
    # - Report generation agent
    
    return create_geo_extraction_pipeline(session_id, sandbox_dir) 