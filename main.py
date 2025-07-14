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
from src.workflows.geo_pipeline import create_geo_extraction_pipeline, create_multi_agent_geo_pipeline

load_dotenv(override=True)

BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
API_KEY = os.getenv("OPENROUTER_API_KEY")

if not API_KEY:
    raise ValueError("OPENROUTER_API_KEY environment variable is required.")

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
# Only the following models are supported. Each entry also defines its context
# window so that the orchestrator can automatically respect model limits.
MODEL_CHOICES = (
    "openai/gpt-4o",
    "anthropic/claude-3.5-sonnet",
    "google/gemini-2.0-flash-exp",
)
MODEL_TOKEN_LIMITS: dict[str, int] = {
    "openai/gpt-4o": 4_096,
    "anthropic/claude-3.5-sonnet": 4_096,
    "google/gemini-2.0-flash-exp": 4_096,
}

# Disable tracing for OpenRouter
set_tracing_disabled(disabled=True)

# ---------------------------------------------------------------------------
# Model provider setup
# ---------------------------------------------------------------------------
class CustomModelProvider(ModelProvider):
    """Custom model provider for OpenRouter."""
    
    def __init__(self, default_model: str):
        self.default_model = default_model
    
    def get_model(self, model_name: str | None) -> Model:
        model = model_name or self.default_model
        client = AsyncOpenAI(
            base_url=BASE_URL,
            api_key=API_KEY,
            default_headers={
                "HTTP-Referer": "localhost",
                "X-Title": "MetaMuse GEO Extraction"
            }
        )
        return OpenAIChatCompletionsModel(model=model, openai_client=client)


async def run_workflow(workflow_name: str, input_data: str, model_name: str, **kwargs):
    """
    Run a specific workflow with the given input data.
    
    Parameters
    ----------
    workflow_name : str
        Name of the workflow to run
    input_data : str
        The input data/request to process
    model_name : str
        The model to use for processing
    **kwargs
        Additional arguments for the workflow
    """
    session_id = str(uuid4())
    
    # Create model provider
    model_provider = CustomModelProvider(model_name)
    max_tokens = MODEL_TOKEN_LIMITS.get(model_name, 128_000)
    
    # Create orchestrator
    orchestrator = SimpleOrchestrator(
        session_id=session_id,
        model_provider=model_provider,
        provider_max_tokens=max_tokens
    )
    
    # Select workflow function
    workflow_funcs = {
        "geo_extraction": create_geo_extraction_pipeline,
        "multi_agent_geo": create_multi_agent_geo_pipeline,
    }
    
    if workflow_name not in workflow_funcs:
        raise ValueError(f"Unknown workflow: {workflow_name}. Available: {list(workflow_funcs.keys())}")
    
    workflow_func = workflow_funcs[workflow_name]
    
    print(f"🚀 Starting {workflow_name} workflow...")
    print(f"📋 Session ID: {session_id}")
    print(f"🤖 Model: {model_name}")
    print(f"📝 Input: {input_data}")
    print("=" * 60)
    
    try:
        # Run the workflow
        result = await orchestrator.run_workflow(workflow_func, input_data, **kwargs)
        
        print("\n" + "=" * 60)
        print("✅ Workflow completed successfully!")
        
        # Print session metadata
        session_metadata = orchestrator.get_session_metadata()
        print(f"📁 Session directory: {session_metadata['session_dir']}")
        print(f"📄 Files created: {session_metadata['files_created']}")
        
        if session_metadata['file_list']:
            print("📋 Generated files:")
            for file_path in session_metadata['file_list']:
                print(f"   - {file_path}")
        
        # Print final output
        print("\n🎯 Final Output:")
        print("-" * 40)
        print(result.final_output)
        
        return result
        
    except Exception as e:
        print(f"\n❌ Workflow failed: {str(e)}")
        raise


def list_workflows():
    """List available workflows."""
    workflows = {
        "geo_extraction": "Single-agent GEO metadata extraction pipeline",
        "multi_agent_geo": "Multi-agent GEO metadata extraction pipeline (extensible)",
    }
    
    print("Available workflows:")
    for name, description in workflows.items():
        print(f"  {name}: {description}")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="MetaMuse GEO Metadata Extraction System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py geo_extraction "Extract metadata for GSM1019742" --model openai/gpt-4o
  python main.py geo_extraction "Get series info for GSE41588 and paper abstract for PMID 23902433"
  python main.py --list-workflows
        """
    )
    
    parser.add_argument(
        "workflow",
        nargs="?",
        choices=["geo_extraction", "multi_agent_geo"],
        help="Workflow to run"
    )
    
    parser.add_argument(
        "input",
        nargs="?",
        help="Input data/request for the workflow"
    )
    
    parser.add_argument(
        "--model",
        choices=MODEL_CHOICES,
        default="openai/gpt-4o",
        help="Model to use for processing"
    )
    
    parser.add_argument(
        "--list-workflows",
        action="store_true",
        help="List available workflows"
    )
    
    args = parser.parse_args()
    
    if args.list_workflows:
        list_workflows()
        return
    
    if not args.workflow or not args.input:
        parser.error("Both workflow and input are required unless using --list-workflows")
    
    # Validate required environment variables
    required_env_vars = ["OPENROUTER_API_KEY", "NCBI_EMAIL"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set them in your .env file")
        sys.exit(1)
    
    # Run the workflow
    await run_workflow(args.workflow, args.input, args.model)


if __name__ == "__main__":
    asyncio.run(main()) 