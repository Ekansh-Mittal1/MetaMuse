from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

from agents import Agent, handoff, RunContextWrapper
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX
from pydantic import Field
from src.agents.handoff_base import BaseHandoff

from src.agents.tool_utils import get_session_tools
from src.utils.prompts import load_prompt


class DataDiscoveryHandoff(BaseHandoff):
    """Input to the DataDiscoveryAgent."""

    file_path: str = Field(
        ..., description="The absolute path to the data file to be analyzed."
    )


def on_handoff_callback(ctx: RunContextWrapper[None], input_data: BaseHandoff):
    """A no-op callback to satisfy the handoff function's requirement."""
    pass


def create_data_discovery_agent(
    session_id: str, sandbox_dir: str = None, handoffs: list = None
) -> Agent:
    """
    Factory method to create a data discovery agent.

    This agent is responsible for exploring a dataset, identifying its
    file type, and extracting key metadata such as column names and dataset size.

    Parameters
    ----------
    session_id : str
        The unique session identifier.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    handoffs : list, optional
        List of handoff objects to configure for this agent

    Returns
    -------
    Agent
        An instance of an Agent configured for data discovery tasks.
    """
    if session_id is None:
        session_id = str(uuid4())

    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    session_dir = (Path(sandbox_dir) / session_id).absolute()
    session_dir.mkdir(parents=True, exist_ok=True)

    tools = get_session_tools(session_dir)

    instructions = RECOMMENDED_PROMPT_PREFIX + "\n\n" + load_prompt(
        "data_discovery_agent.md", 
        session_dir=str(session_dir)
    )

    return Agent(
        name="DataDiscoveryAgent",
        instructions=instructions,
        tools=tools,
        handoffs=handoffs or [],
    )
