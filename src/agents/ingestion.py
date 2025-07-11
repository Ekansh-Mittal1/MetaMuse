from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel

from agents import Agent, handoff, RunContextWrapper
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX

from src.utils.prompts import load_prompt


def on_handoff_callback(ctx: RunContextWrapper[None], input_data: BaseModel):
    """A no-op callback to satisfy the handoff function's requirement."""
    pass


def create_ingestion_agent(session_id: str, sandbox_dir: str = None, handoffs: list = None) -> Agent:
    """
    
    Create an ingestion agent for extracting GEO metadata.

    Creates an agent that can extract metadata from Gene Expression Omnibus (GEO) 
    given GSM and GSE IDs. The agent extracts:
    - Sample-level metadata from GSM records
    - Series-level metadata from GSE records  
    - Series matrix table download links
    - Associated paper abstracts

    Args:
        session_id (str): Unique identifier for this agent session
        sandbox_dir (str, optional): Directory for temporary files. Defaults to None.
        handoffs (list, optional): List of handoff functions. Defaults to None.

    Returns:
        Agent: Configured ingestion agent ready to process GEO metadata requests

    Note:
        The agent requires NCBI email and API key to be set in environment variables
        NCBI_EMAIL and NCBI_API_KEY respectively.
    """
    
    if session_id is None:
        session_id = str(uuid4())

    instructions = RECOMMENDED_PROMPT_PREFIX + "\n\n" + load_prompt("planning_agent.md")

    return Agent(
        name="IngestionAgent",
        instructions=instructions,
        handoffs=handoffs or [],
        tools = None #TODO
    )