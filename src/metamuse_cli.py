from __future__ import annotations

import asyncio
import argparse
import logging
import os
import sys
import traceback
from pathlib import Path
from uuid import uuid4

from openai import AsyncOpenAI

from dotenv import load_dotenv

from src.sample_paths import DEFAULT_GSM_IDS_FILE

# Suppress HTTP request logging from httpx/openai
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

from agents import (
    set_tracing_disabled,
    Model,
    ModelProvider,
    OpenAIChatCompletionsModel,
)

load_dotenv(override=True)


# Set up exception hook to catch any unhandled exceptions
def exception_hook(exctype, value, tb):
    print(f"\n❌ UNHANDLED EXCEPTION: {exctype.__name__}: {value}")
    print("🔍 FULL TRACEBACK:")
    print("-" * 60)
    traceback.print_exception(exctype, value, tb)
    sys.exit(1)


sys.excepthook = exception_hook

BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")


def _openrouter_api_key() -> str:
    """Resolve API key when a workflow actually needs OpenRouter (lazy)."""
    k = os.getenv("OPENROUTER_API_KEY")
    if not k:
        raise ValueError("OPENROUTER_API_KEY environment variable is required.")
    return k


# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
# Only the following models are supported. Each entry also defines its context
# window so that the orchestrator can automatically respect model limits.
MODEL_CHOICES = ("google/gemini-2.5-pro", "google/gemini-2.5-flash", "openai/gpt-4o", "openai/gpt-4o-mini")

# Context window limits for each model
MODEL_CONTEXT_LIMITS: dict[str, int] = {
    "google/gemini-2.5-pro": 2_097_152,  # 2M tokens context window for Pro
    "google/gemini-2.5-flash": 1_048_576,  # 1M tokens context window for Flash
    "openai/gpt-4o": 128_000,
    "openai/gpt-4o-mini": 128_000,
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
            api_key=_openrouter_api_key(),
            default_headers={
                "HTTP-Referer": "localhost",
                "X-Title": "MetaMuse GEO Extraction",
                "X-App-Name": "MetaMuse",
            },
        )
        return OpenAIChatCompletionsModel(model=model, openai_client=client)


async def run_workflow(
    workflow_name: str, input_data: str, model_name: str, max_turns: int = 100, max_tokens: int = None, **kwargs
):
    """
    Run a specific workflow with the given input data.

    Parameters
    ----------
    workflow_name : str
        Name of the workflow to run (batch_samples_efficient or deterministic_sql)
    input_data : str
        The input data/request to process
    model_name : str
        The model to use for processing
    max_turns : int
        Maximum conversation turns per agent (default: 100)
    max_tokens : int
        Maximum tokens for LLM responses
    **kwargs
        Additional arguments for the workflow
    """
    # Create model provider
    model_provider = CustomModelProvider(model_name)

    # Handle batch_samples_efficient workflow (lazy-import heavy dependency chain)
    if workflow_name == "batch_samples_efficient":
        from src.workflows.batch_samples_efficient import run_efficient_batch_samples_workflow

        print("🎯 Starting efficient batch samples workflow")

        # Parse parameters from input_data (same format as batch_samples)
        sample_count = 100
        batch_size = 5
        output_dir = "batch"
        samples_file = DEFAULT_GSM_IDS_FILE
        target_fields = None
        sample_type_filter = None
        batch_name = None
        output_format = "csv"
        max_workers = None
        enable_profiling = False

        if input_data:
            # Parse key=value pairs
            pairs = input_data.split()
            for pair in pairs:
                if "=" in pair:
                    key, value = pair.split("=", 1)
                    if key == "sample_count":
                        sample_count = int(value)
                    elif key == "batch_size":
                        batch_size = int(value)
                    elif key == "output_dir":
                        output_dir = value
                    elif key == "samples_file":
                        samples_file = value
                    elif key == "target_fields":
                        target_fields = [f.strip() for f in value.split(",")]
                    elif key == "sample_type_filter":
                        sample_type_filter = value
                    elif key == "batch_name":
                        batch_name = value
                    elif key == "output_format":
                        output_format = value
                    elif key == "max_workers":
                        try:
                            max_workers = int(value)
                        except Exception:
                            raise ValueError(f"Invalid value for max_workers: {value}")
                    elif key == "enable_profiling":
                        enable_profiling = str(value).lower() in ("true", "1", "yes", "on")
                    elif key == "conditional_mode":
                        conditional_mode = value.strip().lower()
                        if conditional_mode not in ("classic", "eval"):
                            raise ValueError(f"Invalid conditional_mode: {value}. Use 'classic' or 'eval'.")
                    elif key == "arbitrator_test_mode":
                        arbitrator_test_mode = str(value).lower() in ("true", "1", "yes", "on")
                    elif key == "max_iterations":
                        try:
                            max_iterations = int(value)
                            if max_iterations < 1:
                                raise ValueError("max_iterations must be >= 1")
                        except Exception:
                            raise ValueError(f"Invalid value for max_iterations: {value}")
                    elif key == "max_arbitrator_iterations":  # Alternative name for clarity
                        try:
                            max_iterations = int(value)
                            if max_iterations < 1:
                                raise ValueError("max_arbitrator_iterations must be >= 1")
                        except Exception:
                            raise ValueError(f"Invalid value for max_arbitrator_iterations: {value}")

        try:
            # Provide default if not set
            if 'conditional_mode' not in locals():
                conditional_mode = "eval"
            if 'arbitrator_test_mode' not in locals():
                arbitrator_test_mode = False
            if 'max_iterations' not in locals():
                max_iterations = 2

            result = await run_efficient_batch_samples_workflow(
                sample_count=sample_count,
                batch_size=batch_size,
                output_dir=output_dir,
                samples_file=samples_file,
                model_provider=model_provider,
                max_tokens=max_tokens,
                target_fields=target_fields,
                sample_type_filter=sample_type_filter,
                batch_name=batch_name,
                output_format=output_format,
                max_workers=max_workers,
                enable_profiling=enable_profiling,
                conditional_mode=conditional_mode,
                arbitrator_test_mode=arbitrator_test_mode,
                max_iterations=max_iterations,
            )

            if result["success"]:
                print("✅ Efficient batch samples workflow completed successfully")
                print(f"📁 Results saved to: {result['batch_directory']}")
                print(f"⏱️ Total execution time: {result['total_execution_time_seconds']:.2f} seconds")
                print(f"📊 Stage results: {result['stage_results']}")
            else:
                print(f"❌ Efficient batch samples workflow failed: {result['message']}")
                if 'error' in result:
                    print(f"🔍 Error details: {result['error']}")

        except Exception as e:
            print(f"❌ Efficient batch samples workflow failed with exception: {str(e)}")
            import traceback
            traceback.print_exc()
            result = {"success": False, "error": str(e)}

        return result

    # Handle deterministic_sql workflow
    if workflow_name == "deterministic_sql":
        from src.workflows.deterministic_sql import run_deterministic_sql_workflow_sync

        # Create session ID for deterministic SQL workflow
        session_id = f"det_sql_{str(uuid4())}"

        # Extract target field from input if specified
        target_field = "Disease"  # Default
        input_lower = input_data.lower()

        # Support multiple formats: "target_field:" and "target_field ="
        if "target_field:" in input_lower:
            # Find the position in the original string (case insensitive)
            pos = input_lower.find("target_field:")
            before = input_data[:pos].strip()
            after = input_data[pos + len("target_field:"):].strip()
            if after:
                target_field = after.split()[0].strip()
                input_data = before
        elif "target_field =" in input_lower:
            # Find the position in the original string (case insensitive)
            pos = input_lower.find("target_field =")
            before = input_data[:pos].strip()
            after = input_data[pos + len("target_field =") :].strip()
            if after:
                target_field = after.split()[0].strip()
                input_data = before
        elif "target_field=" in input_lower:
            # Find the position in the original string (case insensitive)
            pos = input_lower.find("target_field=")
            before = input_data[:pos].strip()
            after = input_data[pos + len("target_field=") :].strip()
            if after:
                target_field = after.split()[0].strip()
                input_data = before

        # Normalize target field to snake_case format for consistency
        target_field = target_field.lower().replace(" ", "_")

        print(f"🎯 Parsed target field: {target_field}")
        print(f"📝 Cleaned input: {input_data}")

        result = run_deterministic_sql_workflow_sync(
            input_text=input_data,
            target_field=target_field,
            session_id=session_id,
            sandbox_dir="sandbox",
            model_provider=model_provider,
            max_turns=max_turns,
        )

        # Print results
        if result.get("success"):
            print("✅ Deterministic SQL workflow completed successfully!")
            print(f"📁 Session directory: {result['session_directory']}")
            print(f"🎯 Target field: {result['target_field']}")
            print(f"📊 Summary: {result['summary']}")
            print(f"📄 Files created: {len(result.get('files_created', []))}")
            for file_path in result.get("files_created", []):
                print(f"   📄 {Path(file_path).name}")
        else:
            print(
                f"❌ Deterministic SQL workflow failed: {result.get('error', 'Unknown error')}"
            )

        return result

    # Unknown workflow
    raise ValueError(
        f"Unknown workflow: {workflow_name}. Available workflows: batch_samples_efficient, deterministic_sql"
    )


def list_workflows():
    """List available workflows."""
    workflows = {
        "batch_samples_efficient": "Efficient batch samples workflow using three-stage architecture (Data Intake → Preprocessing → Conditional Processing with Arbitrator-driven quality control)",
        "deterministic_sql": "Deterministic SQL workflow: data_intake_sql → curator → normalizer (single-field processing)",
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
  # Batch processing with quality control (production workflow)
  uv run metamuse batch_samples_efficient "sample_count=100 batch_size=5"
  uv run metamuse batch_samples_efficient "sample_count=50 batch_name=test sample_type_filter=primary_sample"
  uv run metamuse batch_samples_efficient "sample_count=25 target_fields=disease,tissue,organ"
  uv run metamuse batch_samples_efficient "sample_count=100 conditional_mode=eval max_iterations=3"

  # Single-field deterministic workflow (simple workflow for testing)
  uv run metamuse deterministic_sql "GSM1000981 target_field:disease"
  uv run metamuse deterministic_sql "GSM1000981,GSM1000984 target_field:tissue"

  # List available workflows
  uv run metamuse --list-workflows
        """,
    )

    parser.add_argument(
        "workflow",
        nargs="?",
        choices=[
            "batch_samples_efficient",
            "deterministic_sql",
        ],
        help="Workflow to run",
    )

    parser.add_argument("input", nargs="?", help="Input data/request for the workflow")

    parser.add_argument(
        "--model",
        choices=MODEL_CHOICES,
        default="google/gemini-2.5-pro",
        help="Model to use for processing (default: google/gemini-2.5-pro)",
    )

    parser.add_argument(
        "--max-tokens",
        type=int,
        help="Maximum tokens for LLM responses",
    )

    parser.add_argument(
        "--list-workflows", action="store_true", help="List available workflows"
    )

    args = parser.parse_args()

    if args.list_workflows:
        list_workflows()
        return

    if not args.workflow or not args.input:
        parser.error(
            "Both workflow and input are required unless using --list-workflows"
        )

    # Validate required environment variables
    required_env_vars = {
        "OPENROUTER_API_KEY": "Required for OpenRouter API access (LLM provider)",
        "NCBI_EMAIL": "Required for NCBI E-Utilities API access (PubMed/GEO data)",
    }

    missing_vars = []
    for var, description in required_env_vars.items():
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        print("❌ MISSING REQUIRED ENVIRONMENT VARIABLES")
        print("=" * 60)
        for var in missing_vars:
            print(f"❌ {var}: {required_env_vars[var]}")
        print("\nPlease set these variables in your .env file:")
        for var in missing_vars:
            print(f"   {var}=your_value_here")
        print("\nExample .env file:")
        print("   OPENROUTER_API_KEY=your_openrouter_api_key")
        print("   NCBI_EMAIL=your_email@example.com")
        print("   NCBI_API_KEY=your_ncbi_api_key  # Optional but recommended")
        print("=" * 60)
        sys.exit(1)

    # Validate optional but recommended environment variables
    recommended_vars = {
        "NCBI_API_KEY": "Recommended for higher NCBI API rate limits",
    }

    missing_recommended = []
    for var, description in recommended_vars.items():
        if not os.getenv(var):
            missing_recommended.append(var)

    if missing_recommended:
        print("⚠️  WARNING: Missing recommended environment variables:")
        for var in missing_recommended:
            print(f"   ⚠️  {var}: {recommended_vars[var]}")
        print("   The workflow will continue but may be rate-limited.")
        print()

    # Run the workflow
    await run_workflow(args.workflow, args.input, args.model, max_tokens=args.max_tokens)


def main_sync() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    main_sync()
