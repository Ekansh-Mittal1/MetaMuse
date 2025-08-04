from __future__ import annotations

import asyncio
import argparse
import os
import sys
import traceback
from pathlib import Path
from uuid import uuid4

from openai import AsyncOpenAI

from dotenv import load_dotenv

from agents import (
    set_tracing_disabled,
    Model,
    ModelProvider,
    OpenAIChatCompletionsModel,
)

from src.workflows.orchestrator import SimpleOrchestrator
from src.workflows.MetaMuse import (
    create_extraction_pipeline,
    create_multi_agent_pipeline,
    create_linking_pipeline,
    create_full_pipeline,
    create_hybrid_pipeline,
    create_enhanced_hybrid_pipeline,
    create_enhanced_full_pipeline,
    create_curation_pipeline,
    create_structured_pipeline,
    create_deterministic_pipeline,  # New deterministic workflow
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
API_KEY = os.getenv("OPENROUTER_API_KEY")

if not API_KEY:
    raise ValueError("OPENROUTER_API_KEY environment variable is required.")

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
# Only the following models are supported. Each entry also defines its context
# window so that the orchestrator can automatically respect model limits.
MODEL_CHOICES = ("google/gemini-2.5-flash", "openai/gpt-4o", "openai/gpt-4o-mini")

# Context window limits for each model
MODEL_CONTEXT_LIMITS: dict[str, int] = {
    "google/gemini-2.5-flash": 4_096,
    "openai/gpt-4o": 128_000,
    "openai/gpt-4o-mini": 128_000,
}

# Maximum response tokens for each model (increased for complex JSON outputs)
MODEL_RESPONSE_LIMITS: dict[str, int] = {
    "google/gemini-2.5-flash": 32_768,  # Increased for curator JSON output
    "openai/gpt-4o": 32_768,  # Increased for normalizer verbose output
    "openai/gpt-4o-mini": 32_768,  # Increased for normalizer verbose output
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
                "X-Title": "MetaMuse GEO Extraction",
                "X-App-Name": "MetaMuse",
            },
        )
        return OpenAIChatCompletionsModel(model=model, openai_client=client)


async def run_workflow(
    workflow_name: str, input_data: str, model_name: str, max_turns: int = 100, **kwargs
):
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
    # Generate session ID with pipeline prefix
    pipeline_prefixes = {
        "geo_extraction": "ge",
        "multi_agent_geo": "mag",
        "linking": "link",
        "full_pipeline": "fp",
        "hybrid_pipeline": "hybrid",
        "curation": "c",
    }

    # For other workflows, use the standard prefix system
    prefix = pipeline_prefixes.get(workflow_name, "unknown")
    session_id = f"{prefix}_{str(uuid4())}"
    existing_session_dir = None

    # Extract session directory from input for linking and curation workflows
    if workflow_name in ["linking", "curation"]:
        import re

        session_match = re.search(
            r"session directory\s+([^\s,]+)", input_data, re.IGNORECASE
        )
        if session_match:
            existing_session_dir = session_match.group(1)
            # Extract session ID from the directory path
            session_id = Path(existing_session_dir).name
            print(f"📁 Using existing session directory: {existing_session_dir}")

    # Create model provider
    model_provider = CustomModelProvider(model_name)
    max_response_tokens = MODEL_RESPONSE_LIMITS.get(
        model_name, 16_384
    )  # High default for complex JSON output

    # Handle deterministic workflow specially (bypasses orchestrator)
    if workflow_name == "deterministic":
        from src.workflows.deterministic_workflow import run_deterministic_workflow_sync

        # Create session ID for deterministic workflow
        session_id = f"det_{str(uuid4())}"

        # Extract target field from input if specified
        target_field = "Disease"  # Default
        input_lower = input_data.lower()

        # Support multiple formats: "target_field:" and "target_field ="
        if "target_field:" in input_lower:
            # Find the position in the original string (case insensitive)
            pos = input_lower.find("target_field:")
            before = input_data[:pos].strip()
            after = input_data[pos + len("target_field:") :].strip()
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

        result = run_deterministic_workflow_sync(
            input_text=input_data,
            target_field=target_field,
            session_id=session_id,
            sandbox_dir="sandbox",
            model_provider=model_provider,
            max_tokens=max_response_tokens,
            max_turns=max_turns,
        )

        # Print results
        if result.get("success"):
            print("✅ Deterministic workflow completed successfully!")
            print(f"📁 Session directory: {result['session_directory']}")
            print(f"🎯 Target field: {result['target_field']}")
            print(f"📊 Summary: {result['summary']}")
            print(f"📄 Files created: {len(result.get('files_created', []))}")
            for file_path in result.get("files_created", []):
                print(f"   📄 {Path(file_path).name}")
        else:
            print(
                f"❌ Deterministic workflow failed: {result.get('error', 'Unknown error')}"
            )

        return result

    # Handle test normalizer workflow specially (bypasses orchestrator)
    if workflow_name == "test_normalizer":
        from src.workflows.deterministic_workflow import test_normalizer_agent

        # Hardcode the test session directory
        test_session_dir = "/teamspace/studios/this_studio/sandbox/test_session"

        result = await test_normalizer_agent(
            test_session_dir=test_session_dir,
            model_provider=model_provider,
            max_tokens=max_response_tokens,
            max_turns=max_turns,
        )

        # Print results
        if result.get("status") == "success":
            print("✅ Normalizer agent test completed successfully!")
            print(f"📁 Test session: {result['test_session_dir']}")
            print(f"🎯 Target field: {result['normalizer_output']['target_field']}")
            print(f"📊 Input candidates: {result['curator_input']['total_candidates']}")
            print(
                f"🔬 Normalized results: {result['normalizer_output']['sample_results_count']}"
            )
            print(
                f"✨ Successful normalizations: {result['normalizer_output']['successful_normalizations']}"
            )
            print(f"📄 Files created: {len(result.get('files_created', []))}")
            for file_path in result.get("files_created", []):
                print(f"   📄 {Path(file_path).name}")
        else:
            print(
                f"❌ Normalizer agent test failed: {result.get('error', 'Unknown error')}"
            )

        return result

    # Create orchestrator
    orchestrator = SimpleOrchestrator(
        session_id=session_id,
        model_provider=model_provider,
        provider_max_tokens=max_response_tokens,
    )

    # Select workflow function
    workflow_funcs = {
        "geo_extraction": create_extraction_pipeline,
        "multi_agent_geo": create_multi_agent_pipeline,
        "linking": create_linking_pipeline,
        "full_pipeline": create_full_pipeline,
        "hybrid_pipeline": create_hybrid_pipeline,
        "enhanced_hybrid_pipeline": create_enhanced_hybrid_pipeline,
        "enhanced_full_pipeline": create_enhanced_full_pipeline,
        "curation": create_curation_pipeline,
        "structured_pipeline": create_structured_pipeline,
        "deterministic": create_deterministic_pipeline,  # New recommended approach
        "test_normalizer": None,  # Special handling for normalizer agent testing
    }

    if workflow_name not in workflow_funcs:
        raise ValueError(
            f"Unknown workflow: {workflow_name}. Available: {list(workflow_funcs.keys())}"
        )

    workflow_func = workflow_funcs[workflow_name]

    # Skip workflow function requirement for special workflows
    if workflow_name in ["test_normalizer"] and workflow_func is None:
        # This is handled specially above, should not reach this point
        raise ValueError(
            f"Special workflow {workflow_name} should be handled before this point"
        )

    # Starting workflow

    try:
        # Handle deterministic workflow specially (bypasses orchestrator)
        if workflow_name == "deterministic":
            # This is already handled above, so we shouldn't reach here
            raise ValueError(
                "Deterministic workflow should be handled before orchestrator creation"
            )

        # Run the workflow with existing session directory if specified
        elif existing_session_dir and workflow_name in ["linking", "curation"]:
            result = await orchestrator.run_workflow(
                lambda **kwargs: workflow_func(
                    existing_session_dir=existing_session_dir, input_data=input_data
                ),
                input_data,
                session_id=session_id,  # Pass session_id explicitly to avoid orchestrator adding it
                max_turns=max_turns,
                **kwargs,
            )
        elif workflow_name in [
            "linking",
            "full_pipeline",
            "hybrid_pipeline",
            "multi_agent_geo",
            "curation",
            "structured_pipeline",
        ]:
            # These workflows need input_data for testing detection
            result = await orchestrator.run_workflow(
                lambda **kwargs: workflow_func(
                    session_id=session_id,
                    sandbox_dir=orchestrator.sandbox_dir,
                    input_data=input_data,
                ),
                input_data,
                max_turns=max_turns,
                **kwargs,
            )
        else:
            # Extraction pipeline doesn't need input_data
            result = await orchestrator.run_workflow(
                lambda **kwargs: workflow_func(
                    session_id=session_id, sandbox_dir=orchestrator.sandbox_dir
                ),
                input_data,
                max_turns=max_turns,
                **kwargs,
            )

        # Print session metadata
        session_metadata = orchestrator.get_session_metadata()
        print(f"Session directory: {session_metadata['session_dir']}")
        print(f"Total files created: {session_metadata['files_created']}")

        # Display series directories
        if session_metadata["series_directories"]:
            print(f"Series directories: {session_metadata['series_count']}")
            for series_dir in session_metadata["series_directories"]:
                print(
                    f"   {series_dir['series_id']}/ ({series_dir['file_count']} files)"
                )
                for file_name in series_dir["files"]:
                    print(f"      📄 {file_name}")
        else:
            print("📂 No series directories created")

        # Display root files
        if session_metadata["root_files"]:
            print(f"📄 Root files: {session_metadata['root_file_count']}")
            for file_name in session_metadata["root_files"]:
                print(f"   📄 {file_name}")

        # Print final output
        print("\n🎯 Final Output:")
        print("-" * 40)
        print(result.final_output)

        return result

    except Exception as e:
        print(f"\n❌ Workflow failed: {str(e)}")
        print("\n🔍 Full traceback:")
        print("-" * 40)
        traceback.print_exc()

        # Also print to stderr for better visibility
        print("\n❌ ERROR DETAILS (stderr):", file=sys.stderr)
        print(f"Error type: {type(e).__name__}", file=sys.stderr)
        print(f"Error message: {str(e)}", file=sys.stderr)
        print("Full traceback:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

        # Re-raise the exception to ensure it's not silently caught
        raise


def list_workflows():
    """List available workflows."""
    workflows = {
        "geo_extraction": "Single-agent GEO metadata extraction pipeline",
        "multi_agent_geo": "Multi-agent GEO metadata extraction pipeline (extensible)",
        "linking": "Single-agent metadata linking and processing pipeline",
        "full_pipeline": "Complete pipeline: IngestionAgent → LinkerAgent → CuratorAgent",
        "hybrid_pipeline": "Hybrid pipeline: Deterministic data_intake + CuratorAgent",
        "enhanced_hybrid_pipeline": "Enhanced hybrid pipeline: Deterministic data_intake + CuratorAgent + NormalizerAgent",
        "enhanced_full_pipeline": "Enhanced full pipeline: IngestionAgent → LinkerAgent → CuratorAgent → NormalizerAgent",
        "curation": "Single-agent metadata curation pipeline for extracting specific fields",
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
  python main.py geo_extraction "Extract metadata for GSM1019742"
  python main.py geo_extraction "Get series info for GSE41588 and paper abstract for PMID 23902433"
  python main.py linking "session directory sandbox/di_c414a6ee-346e-469b-bae5-2c5316872314"
  python main.py full_pipeline "GSM1000981 target_field Disease"
  python main.py hybrid_pipeline "GSM1000981 target_field Disease"
  python main.py enhanced_hybrid_pipeline "GSM1000981 target_field Disease"
  python main.py enhanced_full_pipeline "GSM1000981 target_field Disease"
  python main.py curation "session directory sandbox/test-session target_field Disease samples GSM1000981,GSM1000984"
  python main.py deterministic "GSM1000981 target_field:disease"
  python main.py test_normalizer "any_input"
  python main.py --list-workflows
        """,
    )

    parser.add_argument(
        "workflow",
        nargs="?",
        choices=[
            "geo_extraction",
            "multi_agent_geo",
            "linking",
            "full_pipeline",
            "hybrid_pipeline",
            "enhanced_hybrid_pipeline",
            "enhanced_full_pipeline",
            "curation",
            "structured_pipeline",
            "deterministic",
            "test_normalizer",
        ],
        help="Workflow to run",
    )

    parser.add_argument("input", nargs="?", help="Input data/request for the workflow")

    parser.add_argument(
        "--model",
        choices=MODEL_CHOICES,
        default="google/gemini-2.5-flash",
        help="Model to use for processing (default: google/gemini-2.5-flash)",
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
    await run_workflow(args.workflow, args.input, args.model)


if __name__ == "__main__":
    asyncio.run(main())
