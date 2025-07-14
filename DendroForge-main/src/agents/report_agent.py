from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

from agents import Agent
from pydantic import Field

from src.agents.handoff_base import BaseHandoff
from src.agents.tool_utils import get_session_tools
from src.utils.prompts import load_prompt


class ReportHandoff(BaseHandoff):
    """Input to the ReportAgent."""

    code_summary: str = Field(
        ...,
        description="A text summary of all the code created and its functionalities.",
    )


def create_report_agent(session_id: str | None = None, sandbox_dir: str = None) -> Agent:
    """
    Factory method for the report agent.

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
        An instance of the ReportAgent.
    """
    if session_id is None:
        session_id = str(uuid4())

    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    session_dir = (Path(sandbox_dir) / session_id).absolute()
    session_dir.mkdir(parents=True, exist_ok=True)

    tools = get_session_tools(session_dir)

    instructions = load_prompt("report_agent.md", session_dir=str(session_dir))

    return Agent(
        name="ReportAgent",
        instructions=instructions,
        tools=tools,
    ) 