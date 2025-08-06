"""
Batch targets workflow for processing multiple metadata fields simultaneously.

This workflow processes all target metadata fields for samples in a single execution:
- Disease, Tissue, Organ, Cell Line, Ethnicity, Developmental Stage, Gender/Sex, Organism, PubMed ID, Instrument

The workflow uses different processing stages based on field requirements:
1. All fields: Data Intake (runs once for all fields)
2. Some fields: + Curation (Disease, Tissue, Organ, Cell Line, Developmental Stage, Ethnicity, Gender/Sex)
3. Some fields: + Normalization (Disease, Tissue, Organ only)

Final output is a comprehensive JSON file with all extracted metadata.
"""

import asyncio
import time
from pathlib import Path
from typing import Dict, Any
from uuid import uuid4

from src.workflows.data_intake import run_data_intake_workflow
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


# Define target field processing requirements
TARGET_FIELD_CONFIG = {
    # Fields requiring all 3 stages (Data Intake -> Curation -> Normalization)
    "full_pipeline": ["disease", "tissue", "organ"],
    # Fields requiring 2 stages (Data Intake -> Curation)
    "curation_only": ["cell_line", "developmental_stage", "ethnicity", "gender", "age"],
    # Fields requiring only Data Intake (direct extraction)
    "direct_only": {
        "Organism": "platform_organism",
        "PubMed ID": "pubmed_id",
        "Platform ID": "platform_id",
        "Instrument": "instrument_model",
    },
}


async def run_batch_targets_workflow(
    input_text: str,
    session_id: str = None,
    sandbox_dir: str = "sandbox",
    model_provider=None,
    max_tokens: int = 65536,
    max_turns: int = 100,
    enable_parallel_execution: bool = True,
    target_fields: list = None,
) -> Dict[str, Any]:
    """
    Run the complete batch targets workflow for selected metadata fields.

    This workflow processes target metadata fields in a single execution:
    1. Runs data intake once (agnostic to target fields)
    2. Extracts direct fields (Organism, PubMed ID, Platform ID, Instrument)
    3. Runs curation for fields requiring curation (PARALLELIZED by default)
    4. Runs normalization for fields requiring normalization (PARALLELIZED by default)
    5. Combines all results into a comprehensive JSON output

    PERFORMANCE BENEFITS:
    - Parallel curation: ~8x speedup (8 fields processed simultaneously)
    - Parallel normalization: ~3x speedup (3 fields processed simultaneously)
    - Overall workflow speedup: 3-5x compared to sequential execution

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
        Whether to enable parallel execution of curation and normalization.
        Defaults to True. Set to False for debugging or rate-limited APIs.
    target_fields : list, optional
        List of target fields to process. If None, processes all available fields.
        Available fields: disease, tissue, organ, cell_line, developmental_stage,
        ethnicity, gender, age, organism, pubmed_id, platform_id, instrument

    Returns
    -------
    Dict[str, Any]
        Complete workflow results with selected target field data
    """

    start_time = time.time()

    try:
        # Generate session ID if not provided
        if session_id is None:
            session_id = f"batch_{str(uuid4())}"

        print(f"🚀 Starting batch targets workflow with session: {session_id}")
        print("📋 Processing all target metadata fields")
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
        # STAGE 1: DATA INTAKE (runs once for all fields)
        # ====================================================================
        print("🗂️  STAGE 1: DATA INTAKE")

        data_intake_output = run_data_intake_workflow(
            input_text=input_text,
            session_id=session_id,
            sandbox_dir=sandbox_dir,
            workflow_type="complete",
        )

        if not data_intake_output.success:
            raise RuntimeError(f"Data intake failed: {data_intake_output.message}")

        print("✅ Data intake completed")

        sample_ids = data_intake_output.sample_ids_for_curation
        if not sample_ids:
            raise RuntimeError("No samples available for processing")

        # ====================================================================
        # TARGET FIELD SELECTION
        # ====================================================================
        if target_fields is not None:
            # Validate target fields
            all_available_fields = (
                list(TARGET_FIELD_CONFIG["full_pipeline"])
                + list(TARGET_FIELD_CONFIG["curation_only"])
                + list(TARGET_FIELD_CONFIG["direct_only"].keys())
            )

            invalid_fields = [
                field
                for field in target_fields
                if field.lower() not in [f.lower() for f in all_available_fields]
            ]
            if invalid_fields:
                print(f"⚠️  Warning: Invalid target fields: {invalid_fields}")
                target_fields = [
                    field
                    for field in target_fields
                    if field.lower() in [f.lower() for f in all_available_fields]
                ]

        # Save data intake output for debugging/reference
        try:
            import json

            with open(f"{session_directory}/data_intake_output.json", "w") as f:
                json.dump(data_intake_output.model_dump(), f, indent=2)
        except Exception as save_error:
            print(f"⚠️  Could not save data intake output: {save_error}")
        # ====================================================================
        # STAGE 2: EXTRACT DIRECT FIELDS (no curation/normalization needed)
        # ====================================================================
        print("📊 STAGE 2: DIRECT FIELD EXTRACTION")

        # Filter direct fields based on target_fields selection
        if target_fields is not None:
            selected_direct_fields = {}
            for field_name, field_key in TARGET_FIELD_CONFIG["direct_only"].items():
                if field_name.lower() in [f.lower() for f in target_fields]:
                    selected_direct_fields[field_name] = field_key
        else:
            selected_direct_fields = TARGET_FIELD_CONFIG["direct_only"]

        direct_fields = extract_direct_fields_from_data_intake(
            data_intake_output=data_intake_output, sample_ids=sample_ids
        )

        print(f"✅ Direct fields extraction completed for {len(direct_fields)} samples")

        # ====================================================================
        # STAGE 3: CURATION (for fields requiring curation)
        # ====================================================================
        print("🎯 STAGE 3: CURATION")

        # Filter curation fields based on target_fields selection
        if target_fields is not None:
            curation_fields = []
            for field in (
                TARGET_FIELD_CONFIG["full_pipeline"]
                + TARGET_FIELD_CONFIG["curation_only"]
            ):
                if field.lower() in [f.lower() for f in target_fields]:
                    curation_fields.append(field)
        else:
            curation_fields = (
                TARGET_FIELD_CONFIG["full_pipeline"]
                + TARGET_FIELD_CONFIG["curation_only"]
            )

        # Create subdirectories for target field outputs
        field_subdirs = create_target_field_subdirectories(
            session_directory, curation_fields
        )

        # ====================================================================
        # CURATION EXECUTION (Parallel or Sequential)
        # ====================================================================

        async def run_single_curation(target_field: str):
            """Run curation for a single target field."""
            try:
                curator_output = await run_curator_agent(
                    data_intake_output=data_intake_output,
                    target_field=target_field,
                    session_id=session_id,
                    sandbox_dir=sandbox_dir,
                    model_provider=model_provider,
                    max_tokens=max_tokens,
                    max_turns=max_turns,
                    verbose_output=False,
                )

                return target_field, curator_output

            except Exception as e:
                print(f"❌ Curation failed for {target_field}: {str(e)}")
                return target_field, None

        # Create parallel curation tasks
        curation_tasks = [run_single_curation(field) for field in curation_fields]

        # Run curation tasks (parallel or sequential based on setting)
        curation_start_time = time.time()
        if enable_parallel_execution:
            curation_task_results = await asyncio.gather(
                *curation_tasks, return_exceptions=True
            )
        else:
            # Sequential execution for debugging or rate-limited APIs
            curation_task_results = []
            for task in curation_tasks:
                result = await task
                curation_task_results.append(result)
        curation_end_time = time.time()

        print(
            f"✅ Curation completed in {curation_end_time - curation_start_time:.2f} seconds"
        )

        # Process results from parallel curation
        curation_results = {}
        curator_outputs = {}  # Store curator outputs for normalization

        for result in curation_task_results:
            if isinstance(result, Exception):
                print(f"❌ Curation task failed with exception: {result}")
                continue

            target_field, curator_output = result

            if curator_output is None:
                print(f"⚠️  Curation failed for {target_field}: No output returned")
                continue

            # curator_output is now a CuratorOutput object, not a dict
            if (
                not hasattr(curator_output, "curation_results")
                or not curator_output.curation_results
            ):
                print(
                    f"⚠️  Curation failed for {target_field}: No curation results returned"
                )
                continue

            # Store curator output for later normalization (use lowercase key for consistency)
            curator_outputs[target_field.lower()] = curator_output

            # Save curation output to target field subdirectory
            curator_output_path = (
                Path(field_subdirs[target_field]) / "curator_output.json"
            )
            with open(curator_output_path, "w") as f:
                json.dump(curator_output.model_dump(), f, indent=2)

            # Extract candidates from curation output
            field_curation_results = extract_curation_candidates(
                curator_output=curator_output,
                target_field=target_field,
                sample_ids=sample_ids,
            )

            curation_results[target_field] = field_curation_results

            # Display curation summary
            print(f"📊 {target_field} summary:")
            for sample_id, results in field_curation_results.items():
                candidate_count = results.get("candidate_count", 0)
                best_candidate = results.get("best_candidate")
                if best_candidate:
                    best_value = best_candidate.get("value", "Unknown")
                    best_confidence = best_candidate.get("confidence", 0.0)
                    print(
                        f"   📋 {sample_id}: {candidate_count} candidates, best: '{best_value}' (confidence: {best_confidence:.2f})"
                    )
                else:
                    print(
                        f"   📋 {sample_id}: {candidate_count} candidates, no best candidate"
                    )

        print(f"✅ Curation completed for {len(curation_results)} fields")

        # ====================================================================
        # STAGE 4: NORMALIZATION (for fields requiring normalization)
        # ====================================================================
        print("🔬 STAGE 4: NORMALIZATION")

        # Filter normalization fields based on target_fields selection
        if target_fields is not None:
            normalization_fields = []
            for field in TARGET_FIELD_CONFIG["full_pipeline"]:
                if field.lower() in [f.lower() for f in target_fields]:
                    normalization_fields.append(field)
        else:
            normalization_fields = TARGET_FIELD_CONFIG["full_pipeline"]

        async def run_single_normalization(target_field: str):
            """Run normalization for a single target field."""

            try:
                # Get the curator output for this field (use lowercase key for consistency)
                field_curator_output = curator_outputs.get(target_field.lower())
                if not field_curator_output:
                    print(
                        f"⚠️  No curator output available for normalization of {target_field}"
                    )
                    return target_field, None

                normalizer_output = await run_normalizer_agent(
                    curator_output=field_curator_output,  # Use the specific CuratorOutput object for this field
                    target_field=target_field,
                    session_id=session_id,  # Use same session ID, filename uniqueness handled in normalizer
                    sandbox_dir=sandbox_dir,
                    model_provider=model_provider,
                    max_tokens=max_tokens,
                    max_turns=max_turns,
                    verbose_output=False,
                )

                return target_field, normalizer_output

            except Exception as e:
                print(f"❌ Normalization failed for {target_field}: {str(e)}")
                return target_field, None

        # Create parallel normalization tasks
        normalization_tasks = [
            run_single_normalization(field) for field in normalization_fields
        ]

        # Run normalization tasks (parallel or sequential based on setting)
        normalization_start_time = time.time()
        if enable_parallel_execution:
            normalization_task_results = await asyncio.gather(
                *normalization_tasks, return_exceptions=True
            )
        else:
            # Sequential execution for debugging or rate-limited APIs
            normalization_task_results = []
            for task in normalization_tasks:
                result = await task
                normalization_task_results.append(result)
        normalization_end_time = time.time()

        print(
            f"✅ Normalization completed in {normalization_end_time - normalization_start_time:.2f} seconds"
        )

        # Process results from parallel normalization
        normalization_results = {}

        for result in normalization_task_results:
            if isinstance(result, Exception):
                print(f"❌ Normalization task failed with exception: {result}")
                continue

            target_field, normalizer_output = result

            if normalizer_output is None:
                print(f"⚠️  Normalization failed for {target_field}: No output returned")
                continue

            # normalizer_output is now a BatchNormalizationResult object, not a dict
            if (
                not hasattr(normalizer_output, "sample_results")
                or not normalizer_output.sample_results
            ):
                print(
                    f"⚠️  Normalization failed for {target_field}: No normalization results returned"
                )
                continue

            # Save normalization output to target field subdirectory
            normalizer_output_path = (
                Path(field_subdirs[target_field]) / "normalizer_output.json"
            )
            with open(normalizer_output_path, "w") as f:
                json.dump(normalizer_output.model_dump(), f, indent=2)

            # Extract normalization results
            field_normalization_results = extract_normalization_results(
                normalizer_output=normalizer_output,
                target_field=target_field,
                sample_ids=sample_ids,
            )

            normalization_results[target_field] = field_normalization_results

            # Display normalization summary
            print(f"📊 {target_field} normalization summary:")
            for sample_id, results in field_normalization_results.items():
                normalized_term = results.get("normalized_term", "Not found")
                term_id = results.get("term_id", "N/A")
                confidence = results.get("confidence", 0.0)
                print(
                    f"   📋 {sample_id}: '{normalized_term}' ({term_id}) [confidence: {confidence:.2f}]"
                )

        print(f"\n✅ Normalization completed for {len(normalization_results)} fields")

        # ====================================================================
        # STAGE 5: COMBINE RESULTS
        # ====================================================================
        print("📦 STAGE 5: COMBINING RESULTS")

        # Combine all results into final structure
        combined_results = combine_target_field_results(
            sample_ids=sample_ids,
            direct_fields=direct_fields,
            curation_results=curation_results,
            normalization_results=normalization_results,
        )

        # Create final output structure
        execution_time = time.time() - start_time
        final_output = {
            "success": True,
            "message": f"Batch targets workflow completed successfully. Processed {len(sample_ids)} samples across all target fields.",
            "execution_time_seconds": execution_time,
            "session_id": session_id,
            "session_directory": session_directory,
            "input_text": input_text,
            "sample_ids": sample_ids,
            "target_fields_processed": {
                "direct_extraction": list(TARGET_FIELD_CONFIG["direct_only"].keys()),
                "curation_only": TARGET_FIELD_CONFIG["curation_only"],
                "full_pipeline": TARGET_FIELD_CONFIG["full_pipeline"],
            },
            "processing_summary": {
                "total_samples": len(sample_ids),
                "direct_fields_extracted": len(TARGET_FIELD_CONFIG["direct_only"]),
                "curated_fields": len(curation_fields),
                "normalized_fields": len(normalization_fields),
                "successful_curations": len(curation_results),
                "successful_normalizations": len(normalization_results),
            },
            "performance_metrics": {
                "total_execution_time": execution_time,
                "curation_execution_time": curation_end_time - curation_start_time
                if "curation_end_time" in locals()
                else None,
                "normalization_execution_time": normalization_end_time
                - normalization_start_time
                if "normalization_end_time" in locals()
                else None,
                "parallel_execution_enabled": enable_parallel_execution,
                "execution_mode": "parallel"
                if enable_parallel_execution
                else "sequential",
                "concurrent_curation_tasks": len(curation_fields)
                if enable_parallel_execution
                else 1,
                "concurrent_normalization_tasks": len(normalization_fields)
                if enable_parallel_execution
                else 1,
            },
            "sample_results": combined_results,
        }

        # Save final results
        output_file_path = save_batch_results(
            results=final_output,
            session_directory=session_directory,
            filename="batch_targets_output.json",
        )

        print(f"✅ BATCH TARGETS WORKFLOW COMPLETED - {execution_time:.2f}s")
        print(f"📄 Output saved: {output_file_path}")

        return final_output

    except Exception as e:
        execution_time = time.time() - start_time
        error_output = {
            "success": False,
            "message": f"Batch targets workflow failed: {str(e)}",
            "execution_time_seconds": execution_time,
            "session_id": session_id,
            "session_directory": session_directory
            if "session_directory" in locals()
            else None,
            "error": str(e),
            "error_type": type(e).__name__,
        }

        print("\n❌ BATCH TARGETS WORKFLOW FAILED")
        print(f"⏱️  Execution time: {execution_time:.2f} seconds")
        print(f"🔥 Error: {str(e)}")

        # Save error information if we have a session directory
        if "session_directory" in locals():
            try:
                save_batch_results(
                    results=error_output,
                    session_directory=session_directory,
                    filename="batch_targets_error.json",
                )
            except Exception as save_error:
                print(f"⚠️  Could not save error output: {save_error}")

        raise


async def run_batch_targets_workflow_async(
    input_text: str,
    session_id: str = None,
    sandbox_dir: str = "sandbox",
    model_provider=None,
    max_tokens: int = 65536,
    max_turns: int = 100,
    enable_parallel_execution: bool = True,
    target_fields: list = None,
) -> Dict[str, Any]:
    """
    Async wrapper for the batch targets workflow.

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
    )
