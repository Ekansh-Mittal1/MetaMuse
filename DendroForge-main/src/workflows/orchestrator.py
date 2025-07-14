from typing import Callable
from agents import RunResultStreaming, Runner, RunConfig, ModelSettings, FunctionTool
from openai.types.shared import Reasoning
from pathlib import Path
import os
from dotenv import load_dotenv
from agents import ItemHelpers
import json

# Load environment variables
load_dotenv(override=True)


class SimpleOrchestrator:
    """
    Simple orchestrator for running agent workflows.
    
    This minimal orchestrator manages session directories and executes
    workflow functions that build agent chains.
    """
    
    def __init__(self, session_id: str, model_provider=None, provider_max_tokens=None, sandbox_dir: str = None):
        """
        Initialize the orchestrator.
        
        Parameters
        ----------
        session_id : str
            The unique session identifier
        model_provider : optional
            Custom model provider to use for agent runs
        provider_max_tokens : optional
            Max tokens for the model provider
        sandbox_dir : str, optional
            Base sandbox directory. If not provided, uses SANDBOX_DIR environment variable or defaults to "sandbox"
        """
        self.session_id = session_id
        self.model_provider = model_provider
        self.provider_max_tokens = provider_max_tokens
        
        # Determine sandbox directory
        if sandbox_dir is None:
            sandbox_dir = os.getenv("SANDBOX_DIR", "sandbox")
        
        self.sandbox_dir = sandbox_dir
        self.session_dir = Path(sandbox_dir) / session_id
        self.session_dir.mkdir(parents=True, exist_ok=True)
    
    async def run_workflow(
        self, 
        workflow_func: Callable, 
        input_data: str, 
        **kwargs
    ) -> RunResultStreaming:
        """
        Run a workflow function.
        
        Parameters
        ----------
        workflow_func : Callable
            A function that builds and returns the entry point agent
        input_data : str
            The input data for the workflow
        **kwargs : dict
            Additional arguments passed to the workflow function
            
        Returns
        -------
        RunResultStreaming
            The result from Runner.run_streamed
        """
        # Add sandbox_dir to kwargs so workflow functions can pass it to agent factories
        kwargs['sandbox_dir'] = self.sandbox_dir
        
        # Build the agent chain using the workflow function
        entry_agent = workflow_func(self.session_id, **kwargs)
        
        # Prepare run config if model provider is specified
        run_config = None
        if self.model_provider:
            run_config = RunConfig(
                model_provider=self.model_provider,
                model_settings=ModelSettings(
                    max_tokens=self.provider_max_tokens,
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
            )
        
        # Run the workflow
        result = Runner.run_streamed(
            entry_agent,
            input=input_data,
            run_config=run_config,
            max_turns=kwargs.get('max_turns', 2048)
        )
        
        print("=== Run starting ===")
        print(f"Session directory: {self.session_dir}")
        print(f"Max turns: {kwargs.get('max_turns', 2048)}")
        
        event_count = 0
        tool_call_count = 0
        
        try:
            async for event in result.stream_events():
                event_count += 1
                
                # We'll ignore the raw responses event deltas
                if event.type == "raw_response_event":
                    continue
                elif event.type == "agent_updated_stream_event":
                    print(f"Agent updated: {event.new_agent.name}")

                    if event.new_agent.tools:
                        tool_names = [tool.name for tool in event.new_agent.tools if isinstance(tool, FunctionTool)]
                        print(f"Available tools: {', '.join(tool_names)}")
                    continue
                
                elif event.type == "run_item_stream_event":
                    if event.item.type == "tool_call_item":
                        tool_call_count += 1
                        print(f"-- Tool {event.item.raw_item.name} was called with arguments: {event.item.raw_item.arguments}")
                    elif event.item.type == "tool_call_output_item":
                        print(f"-- Tool output: {event.item.output}")
                        # Check for error indicators in tool output
                        if "error:" in str(event.item.output).lower() or "failed" in str(event.item.output).lower():
                            print(f"⚠ Tool execution may have failed: {event.item.output}")
                    elif event.item.type == "message_output_item":
                        print(f"-- Message output:\n {ItemHelpers.text_message_output(event.item)}")
                    else:
                        print(f"-- Other event: {event.item.type}")
                else:
                    print(f"-- Unhandled event type: {event.type}")
                    
        except Exception as e:
            print(f"Error during workflow execution: {e}")
            print(f"Exception type: {type(e).__name__}")
            import traceback
            traceback.print_exc()
            
        print("=== Run complete ===")
        print(f"Total events processed: {event_count}")
        print(f"Total tool calls: {tool_call_count}")
        
        # Check if result has final output
        if hasattr(result, 'final_output') and result.final_output:
            print(f"Final output length: {len(str(result.final_output))}")
        else:
            print("⚠ No final output generated")
        
        return result 