from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

from agents import Agent
from pydantic import Field

from src.agents.handoff_base import BaseHandoff
from src.agents.tool_utils import get_session_tools
from src.utils.prompts import load_prompt


class QAHandoff(BaseHandoff):
    """Input to the QAAgent."""

    file_structure: str = Field(
        ...,
        description="A list of the files in the sandbox directory and their functionalities.",
    )
    key_findings: str = Field(
        ...,
        description="A list of key findings from the output of the code.",
    )


def create_qa_agent(session_id: str | None = None, sandbox_dir: str = None) -> Agent:
    """
    Factory method for the qa agent.

    This agent receives a summary of the coding work and the original request,
    reads all the generated files, and creates a final markdown report.

    Parameters
    ----------
    session_id : str, optional
        The unique session identifier.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"

    Returns
    -------
    Agent
        An instance of the QAAgent.
    """
    if session_id is None:
        session_id = str(uuid4())

    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    session_dir = (Path(sandbox_dir) / session_id).absolute()
    session_dir.mkdir(parents=True, exist_ok=True)

    tools = get_session_tools(session_dir)

    instructions = load_prompt("qa_agent.md", session_dir=str(session_dir))

    return Agent(
        name="QAAgent",
        instructions=instructions,
        tools=tools,
    ) 