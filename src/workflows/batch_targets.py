"""
Batch targets workflow for processing multiple metadata fields simultaneously.

This workflow processes all target metadata fields for samples in a single execution:
- Disease, Tissue, Organ, Cell Line, Ethnicity, Developmental Stage, Gender/Sex, Organism, PubMed ID, Instrument

The workflow uses different processing stages based on field requirements:
1. All fields: Data Intake (runs once for all fields)
2. Initial: Sample Type Curation (determines processing path)
3. Conditional: Field-specific Curation based on sample type (Disease, Organ, Tissue, Cell Line, Developmental Stage, Ethnicity, Gender/Sex)
4. Unified: Normalization (Disease, Organ, Tissue)

Final output is a comprehensive JSON file with all extracted metadata.

RETRY LOGIC:
- All curation and normalization operations now include automatic retry logic
- Retries occur when operations return NoneType (indicating failure)
- Uses exponential backoff with configurable delays
- Maximum of 3 retry attempts per operation
- Comprehensive logging of retry attempts and failures
- Error tracking integration for monitoring and debugging

THREE-MODEL OPTIMIZATION:
- Sample Type Curation: Gemini 2.5 Flash for faster, simple sample type determination
- Conditional Curation: Gemini 2.5 Pro for higher quality, complex field-specific reasoning
- Normalization: Gemini 2.5 Flash for faster, cost-effective standardization
- Automatic model selection based on operation type and complexity
- Maintains retry logic with operation-specific models
"""

import asyncio
import json
import time
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from uuid import uuid4

from src.workflows.data_intake_sql import run_data_intake_sql_workflow as run_data_intake_workflow
from src.models import LinkerOutput
from src.agents.curator import run_curator_agent
from src.agents.normalizer import run_normalizer_agent
from src.tools.batch_processing_tools import (
    extract_direct_fields_from_data_intake,
    extract_curation_candidates,
    extract_normalization_results,
    combine_target_field_results,
    save_batch_results,
    create_target_field_subdirectories,
)
from pydantic import BaseModel
from typing import Optional

# Add retry configuration constants
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
RETRY_BACKOFF_MULTIPLIER = 2

# Dual-model configuration for optimal performance
SAMPLE_TYPE_CURATION_MODEL = "google/gemini-2.5-flash"  # Faster for simple sample type determination
CONDITIONAL_CURATION_MODEL = "openai/gpt-5"    # Higher quality for complex field-specific curation
NORMALIZATION_MODEL = "google/gemini-2.5-flash"         # Faster and more cost-effective for straightforward tasks

# Configuration loaded silently - models will be selected automatically based on operation type


def create_model_provider_for_operation(operation_type: str, base_model_provider=None):
    """
    Create a model provider optimized for the specific operation type.
    
    Parameters
    ----------
    operation_type : str
        Type of operation ('sample_type_curation', 'conditional_curation', or 'normalization')
    base_model_provider : ModelProvider, optional
        Base model provider to use as template
        
    Returns
    -------
    ModelProvider
        Model provider configured for the specific operation
    """
    from agents import ModelProvider
    
    if operation_type.lower() == "sample_type_curation":
        model_name = SAMPLE_TYPE_CURATION_MODEL
    elif operation_type.lower() == "conditional_curation":
        model_name = CONDITIONAL_CURATION_MODEL
    elif operation_type.lower() == "normalization":
        model_name = NORMALIZATION_MODEL
    else:
        # Default to conditional curation model for unknown operations
        model_name = CONDITIONAL_CURATION_MODEL
    
    # If we have a base model provider, create a new one with the specific model
    if base_model_provider and hasattr(base_model_provider, 'default_model'):
        # Create a new provider with the operation-specific model
        # Use the base provider's class to create a new instance
        return type(base_model_provider)(default_model=model_name)
    else:
        # Create a new provider from scratch using the base provider's class
        if base_model_provider:
            return type(base_model_provider)(default_model=model_name)
        else:
            # Fallback: create OpenRouter-based model provider (like in evaluation code)
            import os
            from agents import ModelProvider, Model, OpenAIChatCompletionsModel
            from openai import AsyncOpenAI
            
            class OpenRouterModelProvider(ModelProvider):
                """OpenRouter-backed model provider for batch targets."""

                def __init__(self, default_model: str):
                    self.default_model = default_model

                def get_model(self, model_name: str | None) -> Model:
                    model = model_name or self.default_model
                    base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
                    api_key = os.getenv("OPENROUTER_API_KEY")
                    if not api_key:
                        raise ValueError("OPENROUTER_API_KEY environment variable is required.")
                    client = AsyncOpenAI(
                        base_url=base_url,
                        api_key=api_key,
                        default_headers={
                            "HTTP-Referer": "localhost",
                            "X-Title": "MetaMuse Batch Processing",
                            "X-App-Name": "MetaMuse",
                        },
                    )
                    return OpenAIChatCompletionsModel(model=model, openai_client=client)
            
            return OpenRouterModelProvider(default_model=model_name)


async def retry_operation_with_backoff(
    operation_func,
    operation_name: str,
    target_field: str,
    sample_ids: List[str],
    max_retries: int = MAX_RETRIES,
    base_delay: float = RETRY_DELAY,
    backoff_multiplier: float = RETRY_BACKOFF_MULTIPLIER,
    error_tracker=None,
    model_provider=None,
    **kwargs
) -> Tuple[str, Any]:
    """
    Retry an operation with exponential backoff when it returns None.
    
    Parameters
    ----------
    operation_func : callable
        The async function to retry
    operation_name : str
        Name of the operation for logging (e.g., "curation", "normalization")
    target_field : str
        The target field being processed
    sample_ids : List[str]
        List of sample IDs being processed
    max_retries : int
        Maximum number of retry attempts
    base_delay : float
        Base delay between retries in seconds
    backoff_multiplier : float
        Multiplier for exponential backoff
    error_tracker : object, optional
        Error tracker for logging failures
    **kwargs
        Additional arguments to pass to operation_func
        
    Returns
    -------
    Tuple[str, Any]
        Tuple of (target_field, result) where result is the operation output or None if all retries failed
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            result = await operation_func(**kwargs)
            
            if result is not None:
                # Only show retry success if it wasn't the first attempt
                if attempt > 0:
                    print(f"✅ {operation_name.capitalize()} succeeded for {target_field} on attempt {attempt + 1}")
                return target_field, result
            else:
                # Only show retry messages if we're actually retrying
                if attempt < max_retries:
                    delay = base_delay * (backoff_multiplier ** attempt)
                    print(f"⚠️  {operation_name.capitalize()} returned None for {target_field}, retrying in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                else:
                    print(f"❌ {operation_name.capitalize()} failed for {target_field} after {max_retries + 1} attempts")
                    
                    # Log the failure
                    if error_tracker and hasattr(error_tracker, 'track_target_field_error'):
                        error_tracker.track_target_field_error(
                            target_field=target_field,
                            error=f"{operation_name} returned None after {max_retries + 1} attempts",
                            samples=sample_ids,
                            stage=f"{operation_name}_retry_exhausted"
                        )
                    
                    return target_field, None
                    
        except Exception as e:
            last_exception = e
            # Only show retry messages if we're actually retrying
            if attempt < max_retries:
                delay = base_delay * (backoff_multiplier ** attempt)
                print(f"❌ {operation_name.capitalize()} failed for {target_field}: {str(e)}, retrying in {delay:.1f} seconds...")
                await asyncio.sleep(delay)
            else:
                print(f"❌ {operation_name.capitalize()} failed for {target_field} after {max_retries + 1} attempts")
                print(f"🔍 Final error: {str(e)}")
                
                # Log the failure
                if error_tracker and hasattr(error_tracker, 'track_target_field_error'):
                    error_tracker.track_target_field_error(
                        target_field=target_field,
                        error=str(e),
                        samples=sample_ids,
                        stage=f"{operation_name}_retry_exhausted"
                    )
                
                return target_field, None
    
    # This should never be reached, but just in case
    return target_field, None


# Define target field processing requirements
TARGET_FIELD_CONFIG = {
    # Initial phase - Only sample_type determination
    "initial_phase": {
        "curation": ["sample_type"],
        "normalization": []  # No normalization in initial phase
    },
    
    # Conditional processing based on sample_type
    "conditional_processing": {
        "primary_sample": {
            "curation": ["disease", "organ", "ethnicity", "gender", "age", "tissue", "cell_type", "developmental_stage", "assay_type", "treatment"],
            "normalization": ["disease", "organ", "tissue"],
            "not_applicable": ["cell_line"]
        },
        "cell_line": {
            "curation": ["disease", "organ", "cell_line", "cell_type", "assay_type", "treatment"], 
            "normalization": ["disease", "organ"],  # Disease, organ, and assay_type are normalized for cell lines
            "not_applicable": ["ethnicity", "gender", "age", "tissue", "developmental_stage"]
        },
        "unknown": {
            "curation": ["disease", "organ", "ethnicity", "gender", "age", "tissue", "cell_line", "cell_type", "assay_type", "treatment"],
            "normalization": ["disease", "organ", "tissue"],  # Cell line still not normalized for unknown
            "not_applicable": ["developmental_stage"]  # developmental_stage only for primary samples
        }
    },
    
    # Fields requiring only Data Intake (direct extraction) - unchanged
    "direct_only": {
        "Organism": "organism_ch1",
        "PubMed ID": "pubmed_id",
        "Platform ID": "platform_id",
        "Instrument": "instrument_model",
        "Series ID": "series_id",
    },
}


def extract_sample_type_results(curator_outputs: Dict[str, Any], sample_ids: List[str]) -> Dict[str, str]:
    """
    Extract sample_type classification results from sample_type curation output.
    
    Parameters
    ----------
    curator_outputs : Dict[str, Any]
        Dictionary of curator outputs by target field
    sample_ids : List[str]
        List of all sample IDs
        
    Returns
    -------
    Dict[str, str]
        Dictionary mapping sample_id -> sample_type value
    """
    sample_type_mapping = {}
    
    # Get sample_type curator output
    sample_type_output = curator_outputs.get("sample_type")
    if not sample_type_output or not hasattr(sample_type_output, "curation_results"):
        print("⚠️  No sample_type curation results found")
        return sample_type_mapping
    
    # Extract sample_type for each sample
    for curation_result in sample_type_output.curation_results:
        sample_id = curation_result.sample_id
        if hasattr(curation_result, "sample_type"):
            sample_type_mapping[sample_id] = curation_result.sample_type.value
        else:
            print(f"⚠️  No sample_type found for sample {sample_id}, marking as failed")
            sample_type_mapping[sample_id] = "failed"
    
    return sample_type_mapping


def group_samples_by_type(sample_type_mapping: Dict[str, str]) -> Dict[str, List[str]]:
    """
    Group samples by their sample_type classification.
    
    Parameters
    ----------
    sample_type_mapping : Dict[str, str]
        Dictionary mapping sample_id -> sample_type value
        
    Returns
    -------
    Dict[str, List[str]]
        Dictionary mapping sample_type -> list of sample_ids
    """
    grouped_samples = {
        "primary_sample": [],
        "cell_line": [],
        "unknown": [],
        "failed": []
    }
    
    for sample_id, sample_type in sample_type_mapping.items():
        if sample_type in grouped_samples:
            grouped_samples[sample_type].append(sample_id)
        else:
            print(f"⚠️  Unknown sample_type '{sample_type}' for sample {sample_id}, treating as failed")
            grouped_samples["failed"].append(sample_id)
    
    return grouped_samples


def create_not_applicable_results(sample_ids: List[str], fields: List[str], target_field: str) -> Dict[str, Dict[str, Any]]:
    """
    Create 'not applicable' results for excluded fields.
    
    Parameters
    ----------
    sample_ids : List[str]
        List of sample IDs
    fields : List[str]
        List of field names to mark as not applicable
    target_field : str
        The specific target field being marked
        
    Returns
    -------
    Dict[str, Dict[str, Any]]
        Dictionary mapping sample_id -> not_applicable_result
    """
    results = {}
    
    for sample_id in sample_ids:
        results[sample_id] = {
            "candidate_count": 1,
            "best_candidate": {
                "value": "not_applicable",
                "confidence": 1.0,
                "source": "system",
                "context": "Field excluded for sample_type classification",
                "rationale": f"Field {target_field} is not applicable for this sample type"
            },
            "all_candidates": [{
                "value": "not_applicable",
                "confidence": 1.0,
                "source": "system",
                "context": "Field excluded for sample_type classification",
                "rationale": f"Field {target_field} is not applicable for this sample type"
            }]
        }
    
    return results


async def run_batch_targets_workflow(
    input_text: str,
    session_id: str = None,
    sandbox_dir: str = "sandbox",
    model_provider=None,
    max_tokens: int = None,
    max_turns: int = 100,
    enable_parallel_execution: bool = True,
    target_fields: list = None,
    error_tracker=None,  # Callback function for error tracking
) -> Dict[str, Any]:
    """
    Run the reformed batch targets workflow with conditional processing based on sample_type.

    NEW WORKFLOW PHASES:
    1. Data Intake → Direct Extraction (unchanged)
    2. Initial Curation (sample_type only) - ALL SAMPLES PARALLEL
    3. Sample Type Grouping - Group samples by sample_type value
    4. Conditional Curation - Process each group with appropriate fields PARALLEL
    5. Unified Normalization - Normalize ALL fields together PARALLEL
    6. Result Assembly - Unified JSON with "not applicable" for excluded fields

    CONDITIONAL PROCESSING RULES:
    - primary_sample: curation[disease, organ, ethnicity, gender, age, tissue, cell_type, developmental_stage] + normalization[disease, organ, tissue]
    - cell_line: curation[disease, organ, cell_line, cell_type] + normalization[disease, organ]
    - unknown: curation[disease, organ, ethnicity, gender, age, tissue, cell_line, cell_type] + normalization[disease, organ, tissue]

    Parameters
    ----------
    input_text : str
        Input text containing GEO IDs for processing
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. Defaults to "sandbox"
    model_provider : ModelProvider, optional
        Model provider for LLM requests
    max_tokens : int, optional
        Maximum tokens for LLM responses
    max_turns : int, optional
        Maximum turns for agent interactions
    enable_parallel_execution : bool, optional
        Whether to enable parallel execution within each phase.
        Defaults to True. Set to False for debugging or rate-limited APIs.
    target_fields : list, optional
        IGNORED in new workflow - all processing is conditional based on sample_type
    error_tracker : callable, optional
        Callback function for error tracking

    Returns
    -------
    Dict[str, Any]
        Complete workflow results with conditional field processing and separate outputs per sample_type
    """

    start_time = time.time()

    try:
        # Generate session ID if not provided
        if session_id is None:
            session_id = f"batch_{str(uuid4())}"

        print(f"🚀 Starting REFORMED batch targets workflow with session: {session_id}")
        print("📋 Processing with conditional sample_type-based workflow")
        print(
            f"📝 Input: {input_text[:100]}..."
            if len(input_text) > 100
            else f"📝 Input: {input_text}"
        )

        # Create session directory
        session_path = Path(sandbox_dir) / session_id
        session_path.mkdir(parents=True, exist_ok=True)
        session_directory = str(session_path)

        # ====================================================================
        # PHASE 1: DATA INTAKE (runs once for all fields)
        # ====================================================================

        try:
            data_intake_output = run_data_intake_workflow(
                input_text=input_text,
                session_id=session_id,
                sandbox_dir=sandbox_dir,
                workflow_type="complete",
            )

            if not data_intake_output.success:
                error_msg = f"Data intake failed: {data_intake_output.message}"
                print(f"❌ {error_msg}")
                
                if error_tracker and hasattr(error_tracker, 'track_stage_error'):
                    error_tracker.track_stage_error(
                        stage="data_intake",
                        error=data_intake_output.message,
                        affected_items=["all_samples"]
                    )
                
                raise RuntimeError(error_msg)

            print("✅ Data intake completed")

        except Exception as e:
            error_msg = f"Data intake workflow failed: {str(e)}"
            print(f"❌ {error_msg}")
            
            if error_tracker and hasattr(error_tracker, 'track_stage_error'):
                error_tracker.track_stage_error(
                    stage="data_intake",
                    error=str(e),
                    affected_items=["all_samples"]
                )
            
            raise RuntimeError(error_msg)

        sample_ids = data_intake_output.sample_ids_for_curation
        if not sample_ids:
            raise RuntimeError("No samples available for processing")

        # Note: Data intake output saving is handled by run_initial_processing for unified discovery structure

        # ====================================================================
        # PHASE 2: DIRECT FIELD EXTRACTION (unchanged)
        # ====================================================================

        direct_fields = extract_direct_fields_from_data_intake(
            data_intake_output=data_intake_output, sample_ids=sample_ids
        )

        # ====================================================================
        # PHASE 3: INITIAL CURATION (sample_type only)
        # ====================================================================

        initial_curation_fields = TARGET_FIELD_CONFIG["initial_phase"]["curation"]
        
        # Create subdirectories for initial curation fields
        initial_field_subdirs = create_target_field_subdirectories(
            session_directory, initial_curation_fields
        )

        async def run_single_curation(target_field: str):
            """Run curation for a single target field with retry logic."""
            # Create sample type curation-specific model provider
            curation_model_provider = create_model_provider_for_operation("sample_type_curation", model_provider)
            
            async def _run_curation():
                curator_output = await run_curator_agent(
                    data_intake_output=data_intake_output,
                    target_field=target_field,
                    session_id=session_id,
                    sandbox_dir=sandbox_dir,
                    model_provider=curation_model_provider,
                    max_tokens=max_tokens,
                    max_turns=max_turns,
                    verbose_output=False,
                )
                return curator_output
            
            return await retry_operation_with_backoff(
                operation_func=_run_curation,
                operation_name="curation",
                target_field=target_field,
                sample_ids=sample_ids,
                error_tracker=error_tracker
            )

        # Run initial curation in parallel
        curation_start_time = time.time()
        initial_curation_tasks = [run_single_curation(field) for field in initial_curation_fields]

        if enable_parallel_execution:
            initial_curation_results = await asyncio.gather(
                *initial_curation_tasks, return_exceptions=True
            )
        else:
            initial_curation_results = []
            for task in initial_curation_tasks:
                result = await task
                initial_curation_results.append(result)
        
        curation_end_time = time.time()

        # Process initial curation results
        initial_curator_outputs = {}
        initial_curation_data = {}

        for result in initial_curation_results:
            if isinstance(result, Exception):
                print(f"❌ Initial curation task failed with exception: {result}")
                print(f"🔍 DEBUG: Exception type: {type(result)}")
                print(f"🔍 DEBUG: Exception traceback:")
                import traceback
                traceback.print_exc()
                continue

            target_field, curator_output = result
            if curator_output is None:
                print(f"⚠️  Initial curation failed for {target_field}: No output returned")
                print(f"🔍 DEBUG: This usually means run_curator_agent raised an exception")
                continue

            if not hasattr(curator_output, "curation_results") or not curator_output.curation_results:
                print(f"⚠️  Initial curation failed for {target_field}: No curation results returned")
                print(f"🔍 DEBUG: Curator output type: {type(curator_output)}")
                print(f"🔍 DEBUG: Curator output attributes: {dir(curator_output) if hasattr(curator_output, '__dict__') else 'No attributes'}")
                print(f"🔍 DEBUG: Curator output content: {curator_output}")
                if hasattr(curator_output, 'curation_results'):
                    print(f"🔍 DEBUG: curation_results attribute exists but is: {curator_output.curation_results}")
                continue

            # Store results
            initial_curator_outputs[target_field.lower()] = curator_output

            # Save curator output (original method for non-unified workflow)
            curator_output_path = Path(initial_field_subdirs[target_field]) / "curator_output.json"
            with open(curator_output_path, "w") as f:
                json.dump(curator_output.model_dump(), f, indent=2)

            # Extract curation candidates
            field_curation_results = extract_curation_candidates(
                curator_output=curator_output,
                target_field=target_field,
                sample_ids=sample_ids,
                error_tracker=error_tracker,
            )
            initial_curation_data[target_field] = field_curation_results


        # ====================================================================
        # PHASE 4: SAMPLE TYPE EVALUATION AND GROUPING
        # ====================================================================

        # Extract sample_type results
        sample_type_mapping = extract_sample_type_results(initial_curator_outputs, sample_ids)
        
        # Save sample_type mapping for reference
        try:
            with open(f"{session_directory}/sample_type_mapping.json", "w") as f:
                json.dump(sample_type_mapping, f, indent=2)
        except Exception as save_error:
            print(f"⚠️  Could not save sample_type mapping: {save_error}")

        # Group samples by sample_type
        grouped_samples = group_samples_by_type(sample_type_mapping)


        # ====================================================================
        # PHASE 5: CONDITIONAL PROCESSING BY SAMPLE TYPE
        # ====================================================================
        print("🔄 PHASE 5: CONDITIONAL PROCESSING BY SAMPLE TYPE")

        # Initialize results storage
        conditional_curation_data = {}
        conditional_normalization_data = {}
        all_sample_type_outputs = {}

        # Process each sample type group
        for sample_type, samples_list in grouped_samples.items():
            if not samples_list or sample_type == "failed":
                continue  # Skip empty groups and failed samples

            print(f"\n📋 Processing {sample_type} group ({len(samples_list)} samples)")
            
            # Get processing configuration for this sample type
            if sample_type not in TARGET_FIELD_CONFIG["conditional_processing"]:
                print(f"⚠️  No configuration found for sample_type: {sample_type}")
                continue
                
            config = TARGET_FIELD_CONFIG["conditional_processing"][sample_type]
            curation_fields = config["curation"]
            normalization_fields = config["normalization"]
            not_applicable_fields = config["not_applicable"]

            # Create subdirectories for this sample type
            sample_type_subdir = Path(session_directory) / f"conditional_{sample_type}"
            sample_type_subdir.mkdir(exist_ok=True)
            
            conditional_field_subdirs = create_target_field_subdirectories(
                str(sample_type_subdir), curation_fields
            )

            # ================================================================
            # CONDITIONAL CURATION
            # ================================================================
            if curation_fields:
                print(f"  🎯 Conditional curation for {sample_type}: {curation_fields}")

                async def run_conditional_curation(target_field: str, samples: List[str]):
                    """Run conditional curation for a single target field and sample type group with retry logic."""
                    # Create conditional curation-specific model provider
                    curation_model_provider = create_model_provider_for_operation("conditional_curation", model_provider)
                    
                    async def _run_conditional_curation():
                        # Filter curation packages for this sample type's samples
                        filtered_curation_packages = []
                        if data_intake_output.curation_packages:
                            for package in data_intake_output.curation_packages:
                                if package.sample_id in samples:
                                    filtered_curation_packages.append(package)
                        
                        # Create filtered data intake output for this sample type
                        filtered_data_intake = LinkerOutput(
                            session_directory=session_directory,
                            sample_ids_for_curation=samples,
                            success=True,
                            message=f"Filtered for {sample_type} samples",
                            execution_time_seconds=0.0,  # Required field
                            sample_ids_requested=samples,  # Required field
                            fields_removed_during_cleaning=[],  # Default value
                            files_created=[],  # Required field
                            curation_packages=filtered_curation_packages  # ✅ FIXED: Include filtered curation packages
                        )

                        curator_output = await run_curator_agent(
                            data_intake_output=filtered_data_intake,
                            target_field=target_field,
                            session_id=f"{session_id}_{sample_type}",
                            sandbox_dir=sandbox_dir,
                            model_provider=curation_model_provider,
                            max_tokens=max_tokens,
                            max_turns=max_turns,
                            verbose_output=False,
                        )
                        return curator_output
                    
                    return await retry_operation_with_backoff(
                        operation_func=_run_conditional_curation,
                        operation_name="curation",
                        target_field=target_field,
                        sample_ids=samples,
                        error_tracker=error_tracker
                    )

                # Run conditional curation in parallel
                conditional_curation_tasks = [
                    run_conditional_curation(field, samples_list) for field in curation_fields
                ]

                if enable_parallel_execution:
                    conditional_curation_results = await asyncio.gather(
                        *conditional_curation_tasks, return_exceptions=True
                    )
                else:
                    conditional_curation_results = []
                    for task in conditional_curation_tasks:
                        result = await task
                        conditional_curation_results.append(result)

                # Process conditional curation results
                for result in conditional_curation_results:
                    if isinstance(result, Exception):
                        print(f"❌ Conditional curation task failed with exception: {result}")
                        print(f"🔍 DEBUG: Exception type: {type(result)}")
                        print(f"🔍 DEBUG: Exception traceback:")
                        import traceback
                        traceback.print_exc()
                        continue
                    
                    target_field, curator_output = result
                    if curator_output is None:
                        print(f"⚠️  Conditional curation failed for {target_field} ({sample_type})")
                        print(f"🔍 DEBUG: This usually means run_curator_agent raised an exception")
                        continue

                    # Debug: Print curator output details
                    print(f"🔍 DEBUG: Processing curator output for {target_field} ({sample_type})")
                    print(f"   - Curator output type: {type(curator_output)}")
                    print(f"   - Curator output success: {getattr(curator_output, 'success', 'N/A')}")
                    print(f"   - Curator output message: {getattr(curator_output, 'message', 'N/A')}")
                    if hasattr(curator_output, 'curation_results'):
                        print(f"   - Curation results count: {len(curator_output.curation_results) if curator_output.curation_results else 0}")
                        if curator_output.curation_results:
                            for i, result in enumerate(curator_output.curation_results[:2]):  # Show first 2
                                print(f"     - Result {i}: sample_id={getattr(result, 'sample_id', 'N/A')}, target_field={getattr(result, 'target_field', 'N/A')}")
                    else:
                        print("   - No curation_results attribute found")

                    # Store results
                    if target_field not in conditional_curation_data:
                        conditional_curation_data[target_field] = {}
                    
                    # Save curator output
                    curator_output_path = Path(conditional_field_subdirs[target_field]) / "curator_output.json"
                    try:
                        curator_data = curator_output.model_dump()
                        # Write to temporary file first, then rename (atomic operation)
                        temp_path = curator_output_path.with_suffix('.json.tmp')
                        with open(temp_path, "w") as f:
                            json.dump(curator_data, f, indent=2)
                        temp_path.rename(curator_output_path)
                        print(f"✅ Successfully saved initial curator output for {target_field}")
                    except Exception as save_error:
                        error_msg = f"❌ Failed to save initial curator output for {target_field}: {str(save_error)}"
                        print(error_msg)
                        # Create empty placeholder to prevent parsing errors later
                        placeholder = {
                            "success": False,
                            "target_field": target_field,
                            "error": error_msg,
                            "curation_results": []
                        }
                        with open(curator_output_path, "w") as f:
                            json.dump(placeholder, f, indent=2)
                        continue

                    # Extract curation candidates
                    field_curation_results = extract_curation_candidates(
                        curator_output=curator_output,
                        target_field=target_field,
                        sample_ids=samples_list,
                    )
                    conditional_curation_data[target_field].update(field_curation_results)

                    # Store for normalization
                    if sample_type not in all_sample_type_outputs:
                        all_sample_type_outputs[sample_type] = {}
                    all_sample_type_outputs[sample_type][target_field.lower()] = curator_output
                    
                    # Debug: Print curator output details
                    print(f"🔍 DEBUG: Stored curator output for {target_field} ({sample_type})")
                    print(f"   - Curator output type: {type(curator_output)}")
                    print(f"   - Curator output success: {getattr(curator_output, 'success', 'N/A')}")
                    print(f"   - Curator output message: {getattr(curator_output, 'message', 'N/A')}")
                    if hasattr(curator_output, 'curation_results'):
                        print(f"   - Curation results count: {len(curator_output.curation_results) if curator_output.curation_results else 0}")
                    else:
                        print("   - No curation_results attribute found")



            # ================================================================
            # NOT APPLICABLE FIELDS
            # ================================================================
            if not_applicable_fields:
                print(f"  ⚪ Marking not applicable for {sample_type}: {not_applicable_fields}")
                
                for field in not_applicable_fields:
                    # Add not applicable curation results
                    not_applicable_curation = create_not_applicable_results(
                        samples_list, not_applicable_fields, field
                    )
                    if field not in conditional_curation_data:
                        conditional_curation_data[field] = {}
                    conditional_curation_data[field].update(not_applicable_curation)


        # ====================================================================
        # PHASE 6: UNIFIED NORMALIZATION (ALL FIELDS)
        # ====================================================================
        print("🔬 PHASE 6: UNIFIED NORMALIZATION (ALL FIELDS) - RUN_BATCH_TARGETS_WORKFLOW")

        # Collect all curator outputs for normalization
        all_curator_outputs_for_normalization = {}
        
        # Add initial curator outputs (sample_type only - not used for normalization)
        # Note: sample_type is not normalized, so we skip adding initial outputs
        
        # Add conditional curator outputs from all sample types
        for sample_type, outputs in all_sample_type_outputs.items():
            for field, curator_output in outputs.items():
                all_curator_outputs_for_normalization[field] = curator_output
                print(f"🔍 DEBUG: Added to normalization - field: {field}, sample_type: {sample_type}")
                print(f"   - Curator output type: {type(curator_output)}")
                print(f"   - Curator output success: {getattr(curator_output, 'success', 'N/A')}")

        # Determine which fields need normalization based on the configuration
        fields_to_normalize = set()
        
        # No initial fields to normalize anymore (sample_type is not normalized)
        
        # Add conditional normalization fields based on which samples we have
        for sample_type, samples_list in grouped_samples.items():
            if samples_list and sample_type in TARGET_FIELD_CONFIG["conditional_processing"]:
                config = TARGET_FIELD_CONFIG["conditional_processing"][sample_type]
                fields_to_normalize.update(config["normalization"])

        # Only normalize fields that have curator outputs
        available_fields_to_normalize = [
            field for field in fields_to_normalize 
            if field.lower() in all_curator_outputs_for_normalization
        ]

        print(f"📋 Fields to normalize: {available_fields_to_normalize}")
        print(f"🔍 DEBUG: all_curator_outputs_for_normalization keys: {list(all_curator_outputs_for_normalization.keys())}")
        print(f"🔍 DEBUG: fields_to_normalize: {list(fields_to_normalize)}")
        for field in fields_to_normalize:
            if field.lower() not in all_curator_outputs_for_normalization:
                print(f"🔍 DEBUG: Missing curator output for {field} (looking for {field.lower()})")
            else:
                print(f"🔍 DEBUG: Found curator output for {field}")

        async def run_unified_normalization(target_field: str):
            """Run normalization for a single target field using unified approach."""
            try:
                print(f"🔍 DEBUG: Starting normalization for {target_field}")
                print(f"   - Looking for key: {target_field.lower()}")
                print(f"   - Available keys: {list(all_curator_outputs_for_normalization.keys())}")
                
                curator_output = all_curator_outputs_for_normalization.get(target_field.lower())
                if not curator_output:
                    print(f"⚠️  No curator output available for normalization of {target_field}")
                    return target_field, None
                
                print(f"🔍 DEBUG: Found curator output for {target_field}")
                print(f"   - Curator output type: {type(curator_output)}")
                print(f"   - Curator output success: {getattr(curator_output, 'success', 'N/A')}")

                # Create normalization-specific model provider
                normalization_model_provider = create_model_provider_for_operation("normalization", model_provider)
                
                normalizer_output = await run_normalizer_agent(
                    curator_output=curator_output,
                    target_field=target_field,
                    session_id=session_id,
                    sandbox_dir=sandbox_dir,
                    model_provider=normalization_model_provider,
                    max_tokens=max_tokens,
                    max_turns=max_turns,
                    verbose_output=False,
                )
                return target_field, normalizer_output

            except Exception as e:
                error_msg = f"Unified normalization failed for {target_field}: {str(e)}"
                print(f"❌ {error_msg}")
                
                if error_tracker and hasattr(error_tracker, 'track_target_field_error'):
                    error_tracker.track_target_field_error(
                        target_field=target_field,
                        error=str(e),
                        samples=sample_ids,
                        stage="unified_normalization"
                    )
                
                return target_field, None

        # Run unified normalization in parallel
        normalization_start_time = time.time()
        unified_normalization_tasks = [run_unified_normalization(field) for field in available_fields_to_normalize]

        if enable_parallel_execution:
            unified_normalization_results = await asyncio.gather(
                *unified_normalization_tasks, return_exceptions=True
            )
        else:
            unified_normalization_results = []
            for task in unified_normalization_tasks:
                result = await task
                unified_normalization_results.append(result)
        
        normalization_end_time = time.time()
        print(f"✅ Unified normalization completed in {normalization_end_time - normalization_start_time:.2f} seconds")

        # Process unified normalization results
        unified_normalization_data = {}

        for result in unified_normalization_results:
            if isinstance(result, Exception):
                print(f"❌ Unified normalization task failed with exception: {result}")
                continue

            target_field, normalizer_output = result
            if normalizer_output is None:
                print(f"⚠️  Unified normalization failed for {target_field}: No output returned")
                continue

            # Extract normalization results
            field_normalization_results = extract_normalization_results(
                normalizer_output=normalizer_output,
                target_field=target_field,
                sample_ids=sample_ids,
            )
            unified_normalization_data[target_field] = field_normalization_results


        # ====================================================================
        # PHASE 7: RESULT ASSEMBLY
        # ====================================================================
        print("📊 PHASE 7: RESULT ASSEMBLY")

        # Combine all curation results
        all_curation_results = {}
        all_curation_results.update(initial_curation_data)
        all_curation_results.update(conditional_curation_data)

        # Use unified normalization results
        all_normalization_results = unified_normalization_data

        # Create separate outputs for each sample type
        sample_type_outputs = {}
        
        for sample_type, samples_list in grouped_samples.items():
            if not samples_list or sample_type == "failed":
                continue
                
            print(f"📋 Creating output for {sample_type} group ({len(samples_list)} samples)")
            
            # Combine results for this sample type
            sample_type_results = combine_target_field_results(
                sample_ids=samples_list,
                direct_fields=direct_fields,
                curation_results=all_curation_results,
                normalization_results=all_normalization_results,
            )

            # Save batch results for this sample type
            sample_type_output_file = save_batch_results(
                results=sample_type_results,
                session_directory=session_directory,
                filename=f"batch_targets_output_{sample_type}.json"
            )
            
            sample_type_outputs[sample_type] = {
                "output_file": sample_type_output_file,
                "sample_count": len(samples_list),
                "samples": samples_list
            }

        # Create unified output with all samples
        unified_results = combine_target_field_results(
            sample_ids=sample_ids,
            direct_fields=direct_fields,
            curation_results=all_curation_results,
            normalization_results=all_normalization_results,
        )

        # Save unified batch results
        unified_output_file = save_batch_results(
            results=unified_results,
            session_directory=session_directory,
            filename="batch_targets_output_unified.json"
        )

        end_time = time.time()
        total_duration = end_time - start_time



        # Return summary results
        return {
            "success": True,
            "session_id": session_id,
            "session_directory": session_directory,
            "workflow_type": "reformed_conditional",
            "total_duration": total_duration,
            "sample_count": len(sample_ids),
            "sample_type_distribution": {
                sample_type: len(samples) for sample_type, samples in grouped_samples.items() if samples
            },
            "sample_type_outputs": sample_type_outputs,
            "unified_output": unified_output_file,
            "sample_type_mapping": sample_type_mapping,
            "phases_completed": [
                "data_intake",
                "direct_extraction", 
                "initial_curation",
                "sample_type_grouping",
                "conditional_processing",
                "unified_normalization",
                "result_assembly"
            ]
        }

    except Exception as e:
        end_time = time.time()
        error_duration = end_time - start_time
        
        error_msg = f"Reformed batch targets workflow failed after {error_duration:.2f}s: {str(e)}"
        print(f"❌ {error_msg}")
        
        if error_tracker and hasattr(error_tracker, 'track_workflow_error'):
            error_tracker.track_workflow_error(
                workflow="reformed_batch_targets",
                error=str(e),
                duration=error_duration
            )
        
        return {
            "success": False,
            "session_id": session_id if 'session_id' in locals() else None,
            "session_directory": session_directory if 'session_directory' in locals() else None,
            "workflow_type": "reformed_conditional",
            "error": str(e),
            "duration": error_duration
        }


# ============================================================================
# MODULAR BATCH TARGETS COMPONENTS
# ============================================================================

class InitialProcessingResult(BaseModel):
    """Result of initial processing phases (data intake, direct extraction, initial curation, sample type grouping)."""
    
    success: bool
    session_id: str
    session_directory: str
    data_intake_output: Any
    sample_ids: List[str]
    direct_fields: Dict[str, Any]
    initial_curation_data: Dict[str, Any]
    initial_curator_outputs: Dict[str, Any]
    sample_type_mapping: Dict[str, str]
    grouped_samples: Dict[str, List[str]]
    error_message: Optional[str] = None


class ConditionalProcessingResult(BaseModel):
    """Result of conditional processing phases (conditional curation, unified normalization)."""
    
    success: bool
    conditional_curation_data: Dict[str, Any]
    unified_normalization_data: Dict[str, Any]
    all_sample_type_outputs: Dict[str, Any]
    error_message: Optional[str] = None


async def run_initial_processing(
    input_text: str,
    session_id: str = None,
    sandbox_dir: str = "sandbox",
    model_provider=None,
    max_tokens: int = None,
    max_turns: int = 100,
    enable_parallel_execution: bool = True,
    error_tracker=None,
    batch_number: int = None,
) -> InitialProcessingResult:
    """
    Run initial processing phases: Data Intake → Direct Extraction → Initial Curation → Sample Type Grouping.
    
    This function handles the first 4 phases of the batch targets workflow and returns
    the results needed for conditional processing.
    """
    
    start_time = time.time()
    
    try:
        # Generate session ID if not provided
        if session_id is None:
            session_id = f"initial_{str(uuid4())}"


        # Create session directory (use sandbox_dir directly for unified discovery)
        if session_id == "discovery":
            # For unified discovery structure, use the discovery directory directly
            session_directory = sandbox_dir
            session_path = Path(session_directory)
        else:
            # For other sessions, create subdirectory as usual
            session_path = Path(sandbox_dir) / session_id
            session_path.mkdir(parents=True, exist_ok=True)
            session_directory = str(session_path)

        # ====================================================================
        # PHASE 1: DATA INTAKE
        # ====================================================================

        try:
            data_intake_output = run_data_intake_workflow(
                input_text=input_text,
                session_id=session_id,
                sandbox_dir=sandbox_dir,
                workflow_type="complete",
            )

            if not data_intake_output.success:
                error_msg = f"Data intake failed: {data_intake_output.message}"
                print(f"❌ {error_msg}")
                
                if error_tracker and hasattr(error_tracker, 'track_stage_error'):
                    error_tracker.track_stage_error(
                        stage="data_intake",
                        error=data_intake_output.message,
                        affected_items=["all_samples"]
                    )
                
                return InitialProcessingResult(
                    success=False,
                    session_id=session_id,
                    session_directory=session_directory,
                    data_intake_output=None,
                    sample_ids=[],
                    direct_fields={},
                    initial_curation_data={},
                    initial_curator_outputs={},
                    sample_type_mapping={},
                    grouped_samples={},
                    error_message=error_msg
                )


        except Exception as e:
            error_msg = f"Data intake workflow failed: {str(e)}"
            print(f"❌ {error_msg}")
            
            if error_tracker and hasattr(error_tracker, 'track_stage_error'):
                error_tracker.track_stage_error(
                    stage="data_intake",
                    error=str(e),
                    affected_items=["all_samples"]
                )
            
            return InitialProcessingResult(
                success=False,
                session_id=session_id,
                session_directory=session_directory,
                data_intake_output=None,
                sample_ids=[],
                direct_fields={},
                initial_curation_data={},
                initial_curator_outputs={},
                sample_type_mapping={},
                grouped_samples={},
                error_message=error_msg
            )

        sample_ids = data_intake_output.sample_ids_for_curation
        if not sample_ids:
            error_msg = "No samples available for processing"
            return InitialProcessingResult(
                success=False,
                session_id=session_id,
                session_directory=session_directory,
                data_intake_output=data_intake_output,
                sample_ids=[],
                direct_fields={},
                initial_curation_data={},
                initial_curator_outputs={},
                sample_type_mapping={},
                grouped_samples={},
                error_message=error_msg
            )

        # Save data intake output for debugging/reference with batch number
        try:
            import json
            # Save to outputs subdirectory with batch number (unified discovery structure)
            outputs_dir = Path(session_directory) / "outputs"
            outputs_dir.mkdir(exist_ok=True)
            output_file = outputs_dir / f"data_intake_output_batch_{batch_number}.json"
            
            with open(output_file, "w") as f:
                json.dump(data_intake_output.model_dump(), f, indent=2)
            
        except Exception as save_error:
            print(f"⚠️  Could not save data intake output: {save_error}")
            import traceback
            traceback.print_exc()

        # ====================================================================
        # PHASE 2: DIRECT FIELD EXTRACTION
        # ====================================================================

        direct_fields = extract_direct_fields_from_data_intake(
            data_intake_output=data_intake_output, sample_ids=sample_ids
        )


        # ====================================================================
        # PHASE 3: INITIAL CURATION (sample_type only)
        # ====================================================================

        initial_curation_fields = TARGET_FIELD_CONFIG["initial_phase"]["curation"]
        
        # Create subdirectories for initial curation fields
        initial_field_subdirs = create_target_field_subdirectories(
            session_directory, initial_curation_fields
        )

        async def run_single_curation(target_field: str):
            """Run curation for a single target field with retry logic."""
            # Create sample type curation-specific model provider
            curation_model_provider = create_model_provider_for_operation("sample_type_curation", model_provider)
            
            async def _run_curation():
                curator_output = await run_curator_agent(
                    data_intake_output=data_intake_output,
                    target_field=target_field,
                    session_id=session_id,
                    sandbox_dir=sandbox_dir,
                    model_provider=curation_model_provider,
                    max_tokens=max_tokens,
                    max_turns=max_turns,
                    verbose_output=False,
                )
                return curator_output
            
            return await retry_operation_with_backoff(
                operation_func=_run_curation,
                operation_name="curation",
                target_field=target_field,
                sample_ids=sample_ids,
                error_tracker=error_tracker
            )

        # Run initial curation in parallel
        curation_start_time = time.time()
        initial_curation_tasks = [run_single_curation(field) for field in initial_curation_fields]

        if enable_parallel_execution:
            initial_curation_results = await asyncio.gather(
                *initial_curation_tasks, return_exceptions=True
            )
        else:
            initial_curation_results = []
            for task in initial_curation_tasks:
                result = await task
                initial_curation_results.append(result)
        
        curation_end_time = time.time()

        # Process initial curation results
        initial_curator_outputs = {}
        initial_curation_data = {}

        for result in initial_curation_results:
            if isinstance(result, Exception):
                print(f"❌ Initial curation task failed with exception: {result}")
                print(f"🔍 DEBUG: Exception type: {type(result)}")
                print(f"🔍 DEBUG: Exception traceback:")
                import traceback
                traceback.print_exc()
                continue

            target_field, curator_output = result
            if curator_output is None:
                print(f"⚠️  Initial curation failed for {target_field}: No output returned")
                print(f"🔍 DEBUG: This usually means run_curator_agent raised an exception")
                continue

            if not hasattr(curator_output, "curation_results") or not curator_output.curation_results:
                print(f"⚠️  Initial curation failed for {target_field}: No curation results returned")
                print(f"🔍 DEBUG: Curator output type: {type(curator_output)}")
                print(f"🔍 DEBUG: Curator output attributes: {dir(curator_output) if hasattr(curator_output, '__dict__') else 'No attributes'}")
                print(f"🔍 DEBUG: Curator output content: {curator_output}")
                if hasattr(curator_output, 'curation_results'):
                    print(f"🔍 DEBUG: curation_results attribute exists but is: {curator_output.curation_results}")
                continue

            # Store results
            initial_curator_outputs[target_field.lower()] = curator_output

            # Save curator output (original method for non-unified workflow)
            curator_output_path = Path(initial_field_subdirs[target_field]) / "curator_output.json"
            with open(curator_output_path, "w") as f:
                json.dump(curator_output.model_dump(), f, indent=2)

            # Extract curation candidates
            field_curation_results = extract_curation_candidates(
                curator_output=curator_output,
                target_field=target_field,
                sample_ids=sample_ids,
                error_tracker=error_tracker,
            )
            initial_curation_data[target_field] = field_curation_results


        # ====================================================================
        # PHASE 4: SAMPLE TYPE EVALUATION AND GROUPING
        # ====================================================================

        # Extract sample_type results
        sample_type_mapping = extract_sample_type_results(initial_curator_outputs, sample_ids)
        
        # Save sample_type mapping for reference
        try:
            with open(f"{session_directory}/sample_type_mapping.json", "w") as f:
                json.dump(sample_type_mapping, f, indent=2)
        except Exception as save_error:
            print(f"⚠️  Could not save sample_type mapping: {save_error}")
            print("❌ Full traceback:")
            import traceback
            print(traceback.format_exc())

        # Group samples by sample_type
        grouped_samples = group_samples_by_type(sample_type_mapping)


        end_time = time.time()

        return InitialProcessingResult(
            success=True,
            session_id=session_id,
            session_directory=session_directory,
            data_intake_output=data_intake_output,
            sample_ids=sample_ids,
            direct_fields=direct_fields,
            initial_curation_data=initial_curation_data,
            initial_curator_outputs=initial_curator_outputs,
            sample_type_mapping=sample_type_mapping,
            grouped_samples=grouped_samples
        )

    except Exception as e:
        end_time = time.time()
        error_msg = f"Initial processing failed after {end_time - start_time:.2f}s: {str(e)}"
        print(f"❌ {error_msg}")
        
        return InitialProcessingResult(
            success=False,
            session_id=session_id if 'session_id' in locals() else None,
            session_directory=session_directory if 'session_directory' in locals() else None,
            data_intake_output=None,
            sample_ids=[],
            direct_fields={},
            initial_curation_data={},
            initial_curator_outputs={},
            sample_type_mapping={},
            grouped_samples={},
            error_message=error_msg
        )


async def run_conditional_processing(
    initial_result: InitialProcessingResult,
    model_provider=None,
    max_tokens: int = None,
    max_turns: int = 100,
    enable_parallel_execution: bool = True,
    error_tracker=None,
) -> ConditionalProcessingResult:
    """
    Run conditional processing phases: Conditional Curation → Unified Normalization.
    
    This function takes the results from initial processing and performs conditional
    curation and normalization based on sample types.
    """
    
    start_time = time.time()
    
    try:
        if not initial_result.success:
            return ConditionalProcessingResult(
                success=False,
                conditional_curation_data={},
                unified_normalization_data={},
                all_sample_type_outputs={},
                error_message=f"Initial processing failed: {initial_result.error_message}"
            )
        
        
        session_directory = initial_result.session_directory
        grouped_samples = initial_result.grouped_samples
        data_intake_output = initial_result.data_intake_output
        
        # ====================================================================
        # PHASE 5: CONDITIONAL PROCESSING BY SAMPLE TYPE
        # ====================================================================
        
        all_sample_type_outputs = {}
        conditional_curation_data = {}
        
        for sample_type, sample_ids in grouped_samples.items():
            if not sample_ids:
                continue  # Skip empty groups
                
            
            # Get conditional processing configuration
            conditional_config = TARGET_FIELD_CONFIG["conditional_processing"].get(sample_type, {})
            curation_fields = conditional_config.get("curation", [])
            not_applicable_fields = conditional_config.get("not_applicable", [])
            
            
            
            # Create subdirectories for conditional curation fields
            if curation_fields:
                conditional_field_subdirs = create_target_field_subdirectories(
                    session_directory, curation_fields
                )
            
            # Run conditional curation in parallel
            if curation_fields:
                
                # Filter curation packages for this sample type group
                filtered_curation_packages = []
                if initial_result.data_intake_output.curation_packages:
                    for package in initial_result.data_intake_output.curation_packages:
                        if package.sample_id in sample_ids:
                            filtered_curation_packages.append(package)
                
                # Create filtered LinkerOutput for this sample type group
                filtered_linker_output = LinkerOutput(
                    success=True,
                    message=f"Filtered for {sample_type} conditional processing",
                    execution_time_seconds=0.0,  # Placeholder
                    sample_ids_requested=sample_ids,
                    session_directory=session_directory,
                    fields_removed_during_cleaning=[],
                    files_created=[],
                    sample_ids_for_curation=sample_ids,
                    linked_data=initial_result.data_intake_output.linked_data,
                    curation_packages=filtered_curation_packages  # ✅ FIXED: Include filtered curation packages
                )
                
                async def run_conditional_curation(target_field: str):
                    """Run conditional curation for a single target field with retry logic."""
                    # Create conditional curation-specific model provider
                    curation_model_provider = create_model_provider_for_operation("conditional_curation", model_provider)
                    
                    async def _run_conditional_curation():
                        curator_output = await run_curator_agent(
                            data_intake_output=filtered_linker_output,
                            target_field=target_field,
                            session_id=initial_result.session_id,
                            sandbox_dir=Path(session_directory).parent,
                            model_provider=curation_model_provider,
                            max_tokens=max_tokens,
                            max_turns=max_turns,
                            verbose_output=False,
                        )
                        return curator_output
                    
                    return await retry_operation_with_backoff(
                        operation_func=_run_conditional_curation,
                        operation_name="curation",
                        target_field=target_field,
                        sample_ids=sample_ids,
                        error_tracker=error_tracker
                    )
                
                # Run conditional curation tasks
                conditional_curation_tasks = [run_conditional_curation(field) for field in curation_fields]
                
                if enable_parallel_execution:
                    conditional_curation_results = await asyncio.gather(
                        *conditional_curation_tasks, return_exceptions=True
                    )
                else:
                    conditional_curation_results = []
                    for task in conditional_curation_tasks:
                        result = await task
                        conditional_curation_results.append(result)
                
                # Process conditional curation results
                for result in conditional_curation_results:
                    if isinstance(result, Exception):
                        print(f"❌ Conditional curation task failed with exception: {result}")
                        print(f"🔍 DEBUG: Exception type: {type(result)}")
                        print(f"🔍 DEBUG: Exception traceback:")
                        import traceback
                        traceback.print_exc()
                        continue
                    
                    target_field, curator_output = result
                    if curator_output is None:
                        print(f"⚠️  Conditional curation failed for {target_field}: No output returned")
                        print(f"🔍 DEBUG: This usually means run_curator_agent raised an exception")
                        continue
                    
                    if not hasattr(curator_output, "curation_results") or not curator_output.curation_results:
                        print(f"⚠️  Conditional curation failed for {target_field}: No curation results returned (sample_ids: {sample_ids}, batch_id: {initial_result.session_id})")
                        print(f"🔍 DEBUG: Curator output type: {type(curator_output)}")
                        print(f"🔍 DEBUG: Curator output attributes: {dir(curator_output) if hasattr(curator_output, '__dict__') else 'No attributes'}")
                        print(f"🔍 DEBUG: Curator output content: {curator_output}")
                        if hasattr(curator_output, 'curation_results'):
                            print(f"🔍 DEBUG: curation_results attribute exists but is: {curator_output.curation_results}")
                        continue
                    
                    # Store results
                    field_key = target_field.lower()
                    all_sample_type_outputs[field_key] = curator_output
                    
                    # Save curator output with robust error handling
                    curator_output_path = Path(conditional_field_subdirs[target_field]) / f"curator_output_{sample_type}.json"
                    try:
                        curator_data = curator_output.model_dump()
                        # Write to temporary file first, then rename (atomic operation)
                        temp_path = curator_output_path.with_suffix('.json.tmp')
                        with open(temp_path, "w") as f:
                            json.dump(curator_data, f, indent=2)
                        temp_path.rename(curator_output_path)
                        print(f"✅ Successfully saved curator output for {target_field}")
                    except Exception as save_error:
                        error_msg = f"❌ Failed to save curator output for {target_field}: {str(save_error)}"
                        print(error_msg)
                        # Create empty placeholder to prevent parsing errors later
                        placeholder = {
                            "success": False,
                            "target_field": target_field,
                            "error": error_msg,
                            "curation_results": []
                        }
                        with open(curator_output_path, "w") as f:
                            json.dump(placeholder, f, indent=2)
                        continue
                    
                    # Extract curation candidates
                    field_curation_results = extract_curation_candidates(
                        curator_output=curator_output,
                        target_field=target_field,
                        sample_ids=sample_ids,
                    )
                    conditional_curation_data[target_field] = field_curation_results
                
            
            # Create "not applicable" results for excluded fields
            if not_applicable_fields:
                
                not_applicable_results = create_not_applicable_results(
                    sample_ids=sample_ids,
                    fields=not_applicable_fields,
                    target_field="conditional_processing"
                )
                
                # Add to conditional curation data
                for field, results in not_applicable_results.items():
                    conditional_curation_data[field] = results
                
        
        

        
        end_time = time.time()
        
        return ConditionalProcessingResult(
            success=True,
            conditional_curation_data=conditional_curation_data,
            unified_normalization_data={},  # No normalization in conditional processing
            all_sample_type_outputs=all_sample_type_outputs
        )
    
    except Exception as e:
        end_time = time.time()
        error_msg = f"Conditional processing failed after {end_time - start_time:.2f}s: {str(e)}"
        print(f"❌ {error_msg}")
        
        return ConditionalProcessingResult(
            success=False,
            conditional_curation_data={},
            unified_normalization_data={},
            all_sample_type_outputs={},
            error_message=error_msg
        )


async def run_unified_normalization(
    initial_result: InitialProcessingResult,
    conditional_result: ConditionalProcessingResult,
    model_provider=None,
    max_tokens: int = None,
    max_turns: int = 100,
    enable_parallel_execution: bool = True,
    error_tracker=None,
) -> Dict[str, Any]:
    """
    Run unified normalization for all relevant fields based on sample types.
    
    This function takes the results from initial processing and conditional curation
    and performs normalization for fields that require it based on the TARGET_FIELD_CONFIG.
    """
    
    start_time = time.time()
    
    try:
        if not initial_result.success or not conditional_result.success:
            print("❌ Cannot run normalization: Initial or conditional processing failed")
            return {}
        
        
        session_directory = initial_result.session_directory
        grouped_samples = initial_result.grouped_samples
        
        # Combine initial and conditional curator outputs
        # Note: initial_curator_outputs now only contains sample_type (not used for normalization)
        all_curator_outputs = {}
        
        # Add conditional curator outputs directly (now they have simple field names)
        for field, curator_output in conditional_result.all_sample_type_outputs.items():
            all_curator_outputs[field] = curator_output
        
        # Determine fields to normalize based on which sample types we have
        fields_to_normalize = set()
        
        # No initial normalization fields anymore (sample_type is not normalized)
        
        # Add conditional normalization fields based on which sample types we have
        for sample_type, samples_list in grouped_samples.items():
            if samples_list and sample_type in TARGET_FIELD_CONFIG["conditional_processing"]:
                config = TARGET_FIELD_CONFIG["conditional_processing"][sample_type]
                fields_to_normalize.update(config.get("normalization", []))
        
        all_normalization_fields = list(fields_to_normalize)
        
        
        # Filter to only fields that have successful curation outputs
        available_normalization_fields = []
        for field in all_normalization_fields:
            field_key = field.lower()
            if field_key in all_curator_outputs:
                available_normalization_fields.append(field)
            else:
                print(f"⚠️  Skipping normalization for {field}: No curation output available")
                
                # Track missing curation output preventing normalization
                if error_tracker and hasattr(error_tracker, 'track_missing_result'):
                    error_tracker.track_missing_result(
                        sample_id="all_samples",  # This affects all samples requiring this field
                        target_field=field,
                        result_type="curation",
                        reason="no_curation_output_for_normalization"
                    )
        
        unified_normalization_data = {}
        
        if available_normalization_fields:
            
            async def run_single_normalization(target_field: str):
                """Run normalization for a single target field with retry logic."""
                # Create normalization-specific model provider
                normalization_model_provider = create_model_provider_for_operation("normalization", model_provider)
                
                async def _run_normalization():
                    field_key = target_field.lower()
                    curator_output = all_curator_outputs[field_key]
                    
                    from src.agents.normalizer import run_normalizer_agent
                    
                    normalizer_output = await run_normalizer_agent(
                        curator_output=curator_output,
                        target_field=target_field,
                        session_id=initial_result.session_id,
                        sandbox_dir=Path(session_directory).parent,
                        model_provider=normalization_model_provider,
                        max_tokens=max_tokens,
                        max_turns=max_turns,
                        verbose_output=False,
                    )
                    return normalizer_output
                
                return await retry_operation_with_backoff(
                    operation_func=_run_normalization,
                    operation_name="normalization",
                    target_field=target_field,
                    sample_ids=initial_result.sample_ids,
                    error_tracker=error_tracker
                )
            
            # Run normalization tasks in parallel
            normalization_tasks = [run_single_normalization(field) for field in available_normalization_fields]
            
            if enable_parallel_execution:
                normalization_results = await asyncio.gather(
                    *normalization_tasks, return_exceptions=True
                )
            else:
                normalization_results = []
                for task in normalization_tasks:
                    result = await task
                    normalization_results.append(result)
            
            # Process normalization results
            for result in normalization_results:
                if isinstance(result, Exception):
                    print(f"❌ Normalization task failed with exception: {result}")
                    continue
                
                target_field, normalizer_output = result
                if normalizer_output is None:
                    print(f"⚠️  Normalization failed for {target_field}: No output returned")
                    continue
                
                # Extract normalization results
                field_normalization_results = extract_normalization_results(
                    normalizer_output=normalizer_output,
                    target_field=target_field,
                    sample_ids=initial_result.sample_ids,
                    error_tracker=error_tracker,
                )
                unified_normalization_data[target_field] = field_normalization_results
            
        else:
            print("⚠️  No fields available for normalization")
        
        end_time = time.time()
        
        return unified_normalization_data
    
    except Exception as e:
        end_time = time.time()
        error_msg = f"Unified normalization failed after {end_time - start_time:.2f}s: {str(e)}"
        print(f"❌ {error_msg}")
        
        if error_tracker and hasattr(error_tracker, 'track_target_field_error'):
            error_tracker.track_target_field_error(
                target_field="unified_normalization",
                error=str(e),
                samples=initial_result.sample_ids,
                stage="unified_normalization"
            )
        
        return {}


async def run_batch_targets_workflow_async(
    input_text: str,
    session_id: str = None,
    sandbox_dir: str = "sandbox",
    model_provider=None,
    max_tokens: int = None,
    max_turns: int = 100,
    enable_parallel_execution: bool = True,
    target_fields: list = None,
    error_tracker=None,  # Add error_tracker parameter
) -> Dict[str, Any]:
    """
    Async wrapper for the reformed batch targets workflow.

    This function provides an async interface to the workflow,
    suitable for integration with the main.py workflow system.
    """

    return await run_batch_targets_workflow(
        input_text=input_text,
        session_id=session_id,
        sandbox_dir=sandbox_dir,
        model_provider=model_provider,
        max_tokens=max_tokens,
        max_turns=max_turns,
        enable_parallel_execution=enable_parallel_execution,
        target_fields=target_fields,
        error_tracker=error_tracker,  # Pass through error_tracker
    )
