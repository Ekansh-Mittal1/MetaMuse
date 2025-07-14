from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4
import json

from agents import (
    Agent,
    ModelProvider,
    RunConfig,
    Runner,
    function_tool,
    ModelSettings,
    handoff,
    RunContextWrapper,
    ItemHelpers,
    FunctionTool,
)
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX  
from openai.types.shared import Reasoning
from pydantic import BaseModel, Field

from src.agents.coding import create_coding_agent
from src.agents.tool_utils import get_session_tools
from src.utils.prompts import load_prompt
from src.agents.handoff_base import BaseHandoff


class CodingPlanningHandoff(BaseHandoff):
    """Input to the CodingPlanningAgent."""

    data_summary: str = Field(
        ...,
        description="A summary of the data analysis from the DataDiscoveryAgent, including file type, dimensions, and schema.",
    )


def on_handoff_callback(ctx: RunContextWrapper[None], input_data: BaseHandoff):
    """Log the handoff's original request and any additional fields."""
    print(f"[Handoff] original_request: {input_data.original_request}")
    for field_name, value in input_data.dict(exclude={"original_request"}).items():
        print(f"[Handoff] {field_name}: {value}")


def create_coding_planning_agent(
    session_id: str, model_provider: ModelProvider, sandbox_dir: str = None, handoffs: list = None
) -> Agent:
    """
    Factory method for the Coding Planning Agent.

    This agent receives a data summary and user request, then creates and
    executes a coding plan by calling the CodingAgent as a tool.

    Parameters
    ----------
    session_id : str
        The unique session identifier.
    model_provider : ModelProvider
        The custom model provider to use for the coding agent runs.
    sandbox_dir : str, optional
        Base sandbox directory. If not provided, defaults to "sandbox"
    handoffs : list, optional
        List of handoff objects to configure for this agent

    Returns
    -------
    Agent
        An instance of the CodingPlanningAgent.
    """
    if sandbox_dir is None:
        sandbox_dir = "sandbox"

    session_dir = (Path(sandbox_dir) / session_id).absolute()
    session_dir.mkdir(parents=True, exist_ok=True)

    @function_tool
    async def execute_coding_task(plan: str) -> str:
        """
        Executes a single, focused coding task. It automatically reads all
        *.py files in the session directory to provide context.

        Parameters
        ----------
        plan : str
            A detailed, step-by-step plan for this specific task.

        Returns
        -------
        str
            The output of the code execution.
        """
        coding_agent = create_coding_agent(session_id, sandbox_dir=sandbox_dir)

        code_so_far_parts = []
        for file_path in session_dir.glob("*.py"):
            try:
                with open(file_path, "r") as f:
                    content = f.read()
                code_so_far_parts.append(
                    f"--- File: {file_path.name} ---\n{content}"
                )
            except Exception:
                pass  # Ignore files that cannot be read
        code_so_far = "\n\n".join(code_so_far_parts)

        tool_input = f"Execution Plan:\n{plan}\n\nCurrent Codebase:\n{code_so_far}"

        result = Runner.run_streamed(
            coding_agent,
            input=tool_input,
            run_config=RunConfig(
                model_provider=model_provider,
                model_settings=ModelSettings(
                    max_tokens=40_000,
                    reasoning=Reasoning(
                        effort="high",
                    ),
                    extra_body={
                        "provider": {
                            "order": [
                                "google-vertex/us"
                            ]
                        }
                    }
                ),
            ),
            max_turns=512,
        )
        
        print("=== Coding ExecutionRun starting ===")
        async for event in result.stream_events():
            # We'll ignore the raw responses event deltas
            if event.type == "raw_response_event":
                continue
            elif event.type == "agent_updated_stream_event":
                print(f"Agent updated: {event.new_agent.name}")

                # if event.new_agent.tools:
                #     tool_names = [tool.name for tool in event.new_agent.tools if isinstance(tool, FunctionTool)]
                #     print(f"Available tools: {', '.join(tool_names)}")
                continue
            elif event.type == "run_item_stream_event":
                if event.item.type == "tool_call_item":
                    print("-- Tool was called")
                elif event.item.type == "tool_call_output_item":
                    print(f"-- Tool output: {event.item.output}")
                elif event.item.type == "message_output_item":
                    print(f"-- Message output:\n {ItemHelpers.text_message_output(event.item)}")
                else:
                    pass  # Ignore other event types

        print("=== Coding Execution Run complete ===")
        
        
        return str(result.final_output)

    # The CodingPlanningAgent gets the full suite of session tools,
    # plus the specialized tool for running the CodingAgent.
    tools = get_session_tools(session_dir)
    tools.append(execute_coding_task)

    instructions = RECOMMENDED_PROMPT_PREFIX + "\n\n" + load_prompt("coding_planning_agent.md")

    return Agent(
        name="CodingPlanningAgent",
        instructions=instructions,
        tools=tools,
        handoffs=handoffs or [],
    ) 