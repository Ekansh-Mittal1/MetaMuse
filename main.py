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
from src.workflows.batch_targets import run_batch_targets_workflow_async
from src.workflows.batch_samples import run_batch_samples_workflow
from src.workflows.batch_samples_efficient import run_efficient_batch_samples_workflow

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
            api_key=API_KEY,
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

    # Handle batch_targets workflow specially (bypasses orchestrator)
    if workflow_name == "batch_targets":
        # Create session ID for batch targets workflow
        session_id = f"batch_{str(uuid4())}"

        # Parse target_fields from input_data if specified
        # Format: "target_fields=disease,tissue,organ GSM1234567 GSM1234568"
        target_fields = None
        cleaned_input = input_data

        if "target_fields=" in input_data.lower():
            # Find the position in the original string (case insensitive)
            pos = input_data.lower().find("target_fields=")
            before = input_data[:pos].strip()
            after = input_data[pos + len("target_fields=") :].strip()

            # Extract target_fields value
            if after:
                target_fields_str = after.split()[0].strip()
                target_fields = [
                    field.strip() for field in target_fields_str.split(",")
                ]
                cleaned_input = (
                    before + " " + " ".join(after.split()[1:])
                    if len(after.split()) > 1
                    else before
                )

        print("🎯 Starting batch targets workflow")

        result = await run_batch_targets_workflow_async(
            input_text=cleaned_input,
            session_id=session_id,
            sandbox_dir="sandbox",
            model_provider=model_provider,
            max_turns=max_turns,
            target_fields=target_fields,
        )

        # Print results
        if result.get("success"):
            print("✅ Batch targets workflow completed successfully!")
        else:
            print(
                f"❌ Batch targets workflow failed: {result.get('error', 'Unknown error')}"
            )

        return result

    # Handle batch_samples workflow specially (bypasses orchestrator)
    if workflow_name == "batch_samples":
        print("🎯 Starting batch samples workflow")

        # Parse parameters from input_data
        # Format: "sample_count=100 batch_size=5 output_dir=batch target_fields=disease,tissue sample_type_filter=primary_sample batch_name=my_batch output_format=csv"
        # batch_name: Custom name for the batch directory (optional, creates 'batch_[name]_[timestamp]', otherwise 'batch_[timestamp]')
        # output_format: Output format for batch results (optional, 'parquet' or 'csv', default: 'parquet')
        sample_count = 100  # Default
        batch_size = 5  # Default
        output_dir = "batch"  # Default
        samples_file = "archs4_samples/archs4_gsm_ids.txt"  # Default
        target_fields = None  # Default
        sample_type_filter = None  # Default
        batch_name = None  # Default
        output_format = "parquet"  # Default

        # Parse input parameters
        if input_data:
            parts = input_data.split()
            for part in parts:
                if "=" in part:
                    key, value = part.split("=", 1)
                    if key == "sample_count":
                        sample_count = int(value)
                    elif key == "batch_size":
                        batch_size = int(value)
                    elif key == "output_dir":
                        output_dir = value
                    elif key == "samples_file":
                        samples_file = value
                    elif key == "target_fields":
                        target_fields = [field.strip() for field in value.split(",")]
                    elif key == "sample_type_filter":
                        sample_type_filter = value
                    elif key == "batch_name":
                        batch_name = value
                    elif key == "output_format":
                        output_format = value

        try:
            output_path = await run_batch_samples_workflow(
                sample_count=sample_count,
                batch_size=batch_size,
                output_dir=output_dir,
                age_file=samples_file,  # Original workflow uses age_file parameter name
                model_provider=model_provider,
                target_fields=target_fields,
                sample_type_filter=sample_type_filter,
                batch_name=batch_name,
                output_format=output_format,
            )

            print("✅ Batch samples workflow completed successfully!")

            return {"success": True, "output_path": output_path}

        except Exception as e:
            print(f"❌ Batch samples workflow failed: {e}")
            return {"success": False, "error": str(e)}

    # Handle batch_samples_efficient workflow specially (bypasses orchestrator)
    if workflow_name == "batch_samples_efficient":
        print("🎯 Starting efficient batch samples workflow")

        # Parse parameters from input_data (same format as batch_samples)
        sample_count = 100
        batch_size = 5
        output_dir = "batch"
        samples_file = "archs4_samples/archs4_gsm_ids.txt"
        target_fields = None
        sample_type_filter = None
        batch_name = None
        output_format = "parquet"
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

        try:
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

    # Handle deterministic SQL workflow specially (bypasses orchestrator)
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

    # Handle test normalizer workflow specially (bypasses orchestrator)
    if workflow_name == "test_normalizer":
        from src.workflows.deterministic_workflow import test_normalizer_agent

        # Hardcode the test session directory
        test_session_dir = "/teamspace/studios/this_studio/sandbox/test_session"

        result = await test_normalizer_agent(
            test_session_dir=test_session_dir,
            model_provider=model_provider,
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
        "deterministic": "Deterministic workflow: data_intake → curator → normalizer",
        "deterministic_sql": "Deterministic SQL workflow: data_intake_sql → curator → normalizer",
        "batch_targets": "Batch processing pipeline for all metadata fields (Disease, Tissue, Organ, Cell Line, Ethnicity, Developmental Stage, Gender/Sex, Organism, PubMed ID, Instrument)",
        "batch_samples_efficient": "Efficient batch samples workflow using three-stage architecture (Data Intake → Preprocessing → Conditional Processing)",
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
  python main.py deterministic_sql "GSM1000981 target_field:disease"
  python main.py batch_targets "GSM1000981"
  python main.py batch_targets "GSM1000981,GSM1000984"
  python main.py batch_samples "sample_count=100 batch_size=5"
  python main.py batch_samples "sample_count=50 batch_size=3 output_dir=my_batch"
  python main.py batch_samples "sample_count=50 batch_size=5 sample_type_filter=primary_sample"
  python main.py batch_samples "sample_count=30 batch_size=3 sample_type_filter=cell_line"
  python main.py batch_samples_efficient "sample_count=100 batch_size=5"
  python main.py batch_samples_efficient "sample_count=50 batch_name=test sample_type_filter=primary_sample"
  python main.py batch_samples_efficient "sample_count=25 target_fields=disease,tissue,organ"
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
            "deterministic_sql",
            "test_normalizer",
            "batch_targets",
            "batch_samples",
            "batch_samples_efficient",
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


if __name__ == "__main__":
    asyncio.run(main())
