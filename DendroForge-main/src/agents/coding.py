from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

from agents import Agent

from src.agents.tool_utils import get_session_tools
from src.utils.prompts import load_prompt


def create_coding_agent(session_id: str | None = None, sandbox_dir: str = None) -> Agent:
    """
    Factory method to create a coding agent.

    This agent is designed to write and execute Python code in a sandboxed
    environment to fulfill user requests.

    Parameters
    ----------
    session_id : str, optional
        A unique identifier for the session. If not provided, a new one
        will be generated. This is used to create a dedicated directory
        for the session's files.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"

    Returns
    -------
    Agent
        An instance of an Agent configured for coding tasks.
    """
    if session_id is None:
        session_id = str(uuid4())

    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    session_dir = (Path(sandbox_dir) / session_id).absolute()
    session_dir.mkdir(parents=True, exist_ok=True)

    tools = get_session_tools(session_dir)

    instructions = load_prompt("coding_agent.md", session_dir=str(session_dir))

    return Agent(
        name="CodingAgent",
        instructions=instructions,
        tools=tools,
    )
