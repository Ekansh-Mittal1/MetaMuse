from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import os
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel
from openai import AsyncOpenAI

from agents import Agent, handoff, RunContextWrapper
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX

from src.utils.prompts import load_prompt
from .tool_utils import get_geo_tools


def on_handoff_callback(ctx: RunContextWrapper[None], input_data: BaseModel):
    """A no-op callback to satisfy the handoff function's requirement."""
    pass


def create_ingestion_agent(
    session_id: str, 
    client: AsyncOpenAI = None,
    sandbox_dir: str = None, 
    handoffs: list = None
) -> Agent:
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
        client (AsyncOpenAI, optional): OpenRouter client to use. If not provided, 
                                       will create a default OpenRouter client.
        sandbox_dir (str, optional): Directory for temporary files. Defaults to None.
        handoffs (list, optional): List of handoff functions. Defaults to None.

    Returns:
        Agent: Configured ingestion agent ready to process GEO metadata requests

    Note:
        The agent requires NCBI email and API key to be set in environment variables
        NCBI_EMAIL and NCBI_API_KEY respectively.
        For OpenRouter, requires OPENROUTER_API_KEY environment variable.
    """
    
    if session_id is None:
        session_id = str(uuid4())

    # Create OpenRouter client if not provided
    if client is None:
        
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise ValueError("OPENROUTER_API_KEY environment variable is required")
        
        client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key,
            default_headers={
                "HTTP-Referer": "localhost",
                "X-Title": "MetaMuse Ingestion Agent"
            }
        )

    instructions = RECOMMENDED_PROMPT_PREFIX + "\n\n" + load_prompt("planning_agent.md")

    # Get the GEO tools for this session
    geo_tools = get_geo_tools(session_id)

    return Agent(
        name="IngestionAgent",
        instructions=instructions,
        handoffs=handoffs or [],
        tools=geo_tools
    )