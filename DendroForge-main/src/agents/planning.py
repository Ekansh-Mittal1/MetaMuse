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


def create_planning_agent(session_id: str, sandbox_dir: str = None, handoffs: list = None) -> Agent:
    """
    Factory method to create the initial planning agent.

    This agent understands the user's request and kicks off the workflow
    by handing off to the DataDiscoveryAgent.

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
        An instance of the initial PlanningAgent.
    """
    if session_id is None:
        session_id = str(uuid4())

    instructions = RECOMMENDED_PROMPT_PREFIX + "\n\n" + load_prompt("planning_agent.md")

    return Agent(
        name="PlanningAgent",
        instructions=instructions,
        handoffs=handoffs or [],
    )
