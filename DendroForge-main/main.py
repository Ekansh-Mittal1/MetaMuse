from __future__ import annotations

import asyncio
import argparse
import os
import sys
from pathlib import Path
from datetime import datetime
from uuid import uuid4

from openai import AsyncOpenAI
from openai.types.shared import Reasoning

from dotenv import load_dotenv

from agents import (
    RunConfig,
    Runner,
    set_tracing_disabled,
    ItemHelpers,
    Model,
    ModelProvider,
    OpenAIChatCompletionsModel,
    ModelSettings,
)

from src.workflows.orchestrator import SimpleOrchestrator
from src.workflows.standard_pipeline import create_report_pipeline, create_qa_pipeline

load_dotenv(override=True)

BASE_URL = os.getenv("OPENROUTER_BASE_URL")
API_KEY = os.getenv("OPENROUTER_API_KEY")

if not BASE_URL or not API_KEY:
    raise ValueError("Required environment variables are not set.")

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
# Only the following models are supported. Each entry also defines its context
# window so that the orchestrator can automatically respect model limits.
MODEL_CHOICES = (
    "google/gemini-2.5-pro",
    "anthropic/claude-sonnet-4",
)
MODEL_TOKEN_LIMITS: dict[str, int] = {
    "anthropic/claude-sonnet-4": 64_000,
    "google/gemini-2.5-pro": 64_000,
}
DEFAULT_MODEL_NAME: str = MODEL_CHOICES[0]

client = AsyncOpenAI(base_url=BASE_URL, api_key=API_KEY)
set_tracing_disabled(disabled=True)

# Workflow registry
WORKFLOWS = {
    'report': create_report_pipeline,
    'qa': create_qa_pipeline,
}


class CustomModelProvider(ModelProvider):
    """Return chat-completion model instances, defaulting to *default_model*."""

    def __init__(self, default_model: str):
        self._default_model = default_model

    def get_model(self, model_name: str | None) -> Model:
        return OpenAIChatCompletionsModel(
            model=model_name or self._default_model,
            openai_client=client,
        )


async def run_workflow(workflow_name: str, input_data: str, model_name: str, **kwargs):
    """Run a specific workflow"""
    try:
        session_id = kwargs.get('session_id', str(uuid4()))
        session_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{session_id}_{model_name.split('/')[-1]}"
        
        print(f"Starting workflow '{workflow_name}' with session ID: {session_id} and model: {model_name}")
        print(f"Input data length: {len(input_data)} characters")
        
        model_provider = CustomModelProvider(model_name)
        orchestrator = SimpleOrchestrator(
            session_id,
            model_provider,
            provider_max_tokens=MODEL_TOKEN_LIMITS[model_name],
        )
        
        print(f"Session directory: {orchestrator.session_dir}")
        
        workflow_func = WORKFLOWS[workflow_name]
        result = await orchestrator.run_workflow(
            workflow_func,
            input_data,
            model_provider=model_provider,
            **kwargs
        )
        
        print(f"\n{'='*60}")
        print(f"WORKFLOW COMPLETED: {workflow_name}")
        print(f"{'='*60}")
        print(f"Final Output Length: {len(str(result.final_output))} characters")
        print(f"Final Output:\n{result.final_output}")
        
        # Log completion status
        if result.final_output:
            print("✓ Workflow completed successfully with output")
        else:
            print("⚠ Workflow completed but with empty output")
        
        return result
        
    except Exception as e:
        print(f"Error running workflow '{workflow_name}': {e}")
        print(f"Exception type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def list_workflows():
    """List available workflows"""
    print("Available Workflows:")
    for name, func in WORKFLOWS.items():
        print(f"  - {name}: {str(func.__doc__).strip().split('.')[0]}")


async def main():
    parser = argparse.ArgumentParser(description="DendroForge - Modular Agent System")
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List available workflows')
    
    # Run command
    run_parser = subparsers.add_parser('run', help='Run a workflow')
    run_parser.add_argument('workflow', choices=WORKFLOWS.keys(), help='Workflow to run')
    run_parser.add_argument('input', nargs='?', help='Input data or file path')
    run_parser.add_argument('--file', help='Read input from file')
    run_parser.add_argument('--session-id', help='Use specific session ID')
    run_parser.add_argument('--max-turns', type=int, default=2048, help='Maximum turns')
    run_parser.add_argument('--model', choices=MODEL_CHOICES, default=DEFAULT_MODEL_NAME, help='Model to use')
    
    args = parser.parse_args()
    
    if args.command == 'list':
        list_workflows()
        
    elif args.command == 'run':
        input_data = args.input
        
        if args.file:
            file_path = Path(args.file)
            if file_path.exists():
                input_data = file_path.read_text()
            else:
                print(f"Error: File {args.file} not found")
                sys.exit(1)
        elif not input_data:
            # For backward compatibility with old usage pattern
            if len(sys.argv) > 2:
                # Try to read as file path
                file_path = Path(sys.argv[2])
                if file_path.exists():
                    input_data = file_path.read_text()
                else:
                    input_data = sys.argv[2]
            else:
                print("Error: No input provided")
                sys.exit(1)
        
        # Prepare kwargs
        kwargs = {
            'max_turns': args.max_turns
        }
        if args.session_id:
            kwargs['session_id'] = args.session_id
        
        await run_workflow(args.workflow, input_data, args.model, **kwargs)
        
    else:
        # Default behavior for backward compatibility
        if len(sys.argv) >= 2:
            prompt_file = sys.argv[1]
            try:
                with open(prompt_file, "r") as f:
                    input_data = f.read()
                await run_workflow('report', input_data, DEFAULT_MODEL_NAME)
            except FileNotFoundError:
                print(f"Error: Prompt file not found at '{prompt_file}'")
                sys.exit(1)
        else:
            parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())