"""
Conditional processing workflow for field-specific curation and normalization.

This workflow handles the third stage of the batch processing pipeline:
1. Takes sample type batches from preprocessing workflow
2. Performs conditional curation for each target field based on sample type
3. Performs normalization for specific fields (Disease, Organ, Tissue)
4. Handles retry logic and error tracking
5. Outputs complete curation and normalization results

The workflow uses different models based on operation:
- Conditional Curation: Gemini Pro for high-quality field-specific reasoning
- Normalization: Gemini Flash for cost-effective standardization

Output files include:
- Individual batch directories with field-specific results
- Curation and normalization JSON files
- Error tracking and failed items reports
"""

import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import logging
try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable

from src.workflows.batch_targets import (
    run_conditional_processing,
    run_unified_normalization,
    create_model_provider_for_operation,
    TARGET_FIELD_CONFIG
)
from src.models import LinkerOutput

logger = logging.getLogger(__name__)


class ConditionalProcessingWorkflow:
    """
    Conditional processing workflow for field-specific curation and normalization.
    
    This workflow takes sample type batches and performs conditional processing
    based on sample types, including field-specific curation and normalization.
    """
    
    def __init__(
        self,
        session_directory: str,
        target_fields: List[str] = None,
        model_provider=None,
        max_tokens: int = None,
        max_workers: int = None,
    ):
        """
        Initialize the conditional processing workflow.
        
        Parameters
        ----------
        session_directory : str
            Path to the session directory
        target_fields : List[str], optional
            List of target fields to process. If None, processes all available fields.
            Available fields: disease, tissue, organ, cell_line, cell_type, developmental_stage,
            ethnicity, gender, age, organism, pubmed_id, platform_id, instrument
        model_provider : ModelProvider, optional
            Base model provider (will be specialized per operation)
        max_tokens : int, optional
            Maximum tokens for LLM responses
        """
        self.session_directory = Path(session_directory)
        self.target_fields = target_fields or [
            "disease", "tissue", "organ", "cell_line", "cell_type", "developmental_stage",
            "ethnicity", "gender", "age", "organism", "pubmed_id", "platform_id", "instrument"
        ]
        self.base_model_provider = model_provider
        self.max_tokens = max_tokens
        self.max_workers = max_workers
        
        # Create conditional processing directory structure
        self.conditional_dir = self.session_directory / "conditional_processing"
        self.conditional_dir.mkdir(exist_ok=True)
        
        # Output files
        self.conditional_output_file = self.conditional_dir / "conditional_processing_output.json"
        
        # Error tracking
        self.failed_items = {
            "curation_failures": {},  # target_field -> {sample_id -> error}
            "normalization_failures": {},  # target_field -> {sample_id -> error}
            "missing_results": {},  # track missing curation/normalization results
        }

    async def process_sample_type_batch(
        self,
        batch_samples: List[str],
        sample_type: str,
        batch_num: int,
        total_batches: int,
        data_intake_output: LinkerOutput,
    ) -> Dict[str, Any]:
        """
        Process a single sample type batch with conditional curation and normalization.
        
        Parameters
        ----------
        batch_samples : List[str]
            List of sample IDs in this batch
        sample_type : str
            Sample type for this batch (primary_sample, cell_line, unknown)
        batch_num : int
            Batch number for naming
        total_batches : int
            Total number of batches being processed
        data_intake_output : LinkerOutput
            Data intake output for this batch
            
        Returns
        -------
        Dict[str, Any]
            Batch processing results
        """
        batch_start_time = time.time()
        batch_name = f"{sample_type}_batch_{batch_num}"
        
        
        try:
            # Create batch directory
            batch_dir = self.conditional_dir / batch_name
            batch_dir.mkdir(exist_ok=True)
            
            # Create data intake output for this batch
            batch_data_intake = LinkerOutput(
                success=data_intake_output.success,
                message=f"Combined from 1 discovery sessions for {sample_type} processing",
                execution_time_seconds=0.0,
                sample_ids_requested=batch_samples,
                session_directory=str(batch_dir),
                fields_removed_during_cleaning=data_intake_output.fields_removed_during_cleaning,
                linked_data=data_intake_output.linked_data,
                files_created=data_intake_output.files_created,
                successfully_linked=batch_samples,
                failed_linking=[],
                warnings=[],
                sample_ids_for_curation=batch_samples,
                recommended_curation_fields=self.target_fields,
                cleaned_metadata_files=data_intake_output.cleaned_metadata_files,
                cleaned_series_metadata=data_intake_output.cleaned_series_metadata,
                cleaned_sample_metadata=data_intake_output.cleaned_sample_metadata,
                cleaned_abstract_metadata=data_intake_output.cleaned_abstract_metadata,
                curation_packages=data_intake_output.curation_packages,
            )
            
            # Save batch data intake output
            batch_data_intake_file = batch_dir / "data_intake_output.json"
            with open(batch_data_intake_file, "w") as f:
                json.dump(batch_data_intake.model_dump(), f, indent=2)
            
            # Get sample type-specific configuration
            if sample_type not in TARGET_FIELD_CONFIG["conditional_processing"]:
                logger.warning(f"⚠️ No configuration found for sample_type: {sample_type}")
                return {
                    "success": False,
                    "error": f"No TARGET_FIELD_CONFIG found for sample_type: {sample_type}"
                }
            
            config = TARGET_FIELD_CONFIG["conditional_processing"][sample_type]
            curation_fields = config["curation"]
            normalization_fields = config["normalization"]
            not_applicable_fields = config.get("not_applicable", [])
            
            # Run conditional processing with specialized model
            curation_model_provider = create_model_provider_for_operation(
                "conditional_curation", self.base_model_provider
            )
            
            
            # Need to create an InitialProcessingResult-like object for conditional processing
            # Since we're bypassing the initial processing, we need to create a mock result
            from src.workflows.batch_targets import InitialProcessingResult
            
            # Create a mock initial result with the data we have
            mock_initial_result = InitialProcessingResult(
                success=True,
                session_id=str(batch_dir),
                session_directory=str(batch_dir),
                data_intake_output=batch_data_intake,
                sample_ids=batch_samples,
                direct_fields={},  # Empty for now
                initial_curation_data={},  # Empty for now
                initial_curator_outputs={},  # Empty since we're not using sample type curation here
                sample_type_mapping={sample_id: sample_type for sample_id in batch_samples},
                grouped_samples={sample_type: batch_samples},
                error_message=None,
            )
            
            conditional_result = await run_conditional_processing(
                initial_result=mock_initial_result,
                model_provider=curation_model_provider,
                max_tokens=self.max_tokens,
            )
            
            if not conditional_result.success:
                logger.error(f"❌ Conditional processing failed for {batch_name}: {conditional_result.message}")
                return {
                    "success": False,
                    "batch_name": batch_name,
                    "sample_type": sample_type,
                    "batch_samples": batch_samples,
                    "message": f"Conditional processing failed: {conditional_result.message}",
                    "execution_time_seconds": time.time() - batch_start_time,
                }
            
            
            # Run unified normalization with specialized model
            normalization_model_provider = create_model_provider_for_operation(
                "normalization", self.base_model_provider
            )
            
            # Use sample type-specific normalization fields from TARGET_FIELD_CONFIG
            fields_to_normalize = normalization_fields
            
            if fields_to_normalize:
                
                try:
                    normalization_result = await run_unified_normalization(
                        initial_result=mock_initial_result,
                        conditional_result=conditional_result,
                        model_provider=normalization_model_provider,
                        max_tokens=self.max_tokens,
                    )
                    
                    # Handle different result types
                    if hasattr(normalization_result, 'success'):
                        if not normalization_result.success:
                            logger.warning(f"⚠️ Normalization failed for {batch_name}: {normalization_result.message}")
                        
                        
                except Exception as e:
                    logger.warning(f"⚠️ Normalization failed for {batch_name} with exception: {str(e)}")
                    normalization_result = None
            else:
                logger.info(f"⏭️ No normalization fields to process for {batch_name}")
                normalization_result = None
            
            # Save batch results
            batch_output = {
                "success": True,
                "batch_name": batch_name,
                "sample_type": sample_type,
                "batch_samples": batch_samples,
                "execution_time_seconds": time.time() - batch_start_time,
                "conditional_result": conditional_result.model_dump() if hasattr(conditional_result, 'model_dump') else conditional_result,
                "normalization_result": normalization_result.model_dump() if normalization_result and hasattr(normalization_result, 'model_dump') else normalization_result,
                "batch_directory": str(batch_dir),
                "target_fields_processed": curation_fields,  # Use sample type-specific curation fields
                "normalization_fields_processed": fields_to_normalize if fields_to_normalize else [],
                "not_applicable_fields": not_applicable_fields,  # Track fields that are not applicable for this sample type
                "timestamp": datetime.now().isoformat(),
            }
            
            # Save batch targets output
            batch_targets_file = batch_dir / "batch_targets_output.json"
            with open(batch_targets_file, "w") as f:
                json.dump(batch_output, f, indent=2)
            
            
            return batch_output
            
        except Exception as e:
            logger.error(f"❌ Batch {batch_name} failed with exception: {str(e)}")
            
            return {
                "success": False,
                "batch_name": batch_name,
                "sample_type": sample_type,
                "batch_samples": batch_samples,
                "message": f"Batch processing failed with exception: {str(e)}",
                "error": str(e),
                "execution_time_seconds": time.time() - batch_start_time,
                "timestamp": datetime.now().isoformat(),
            }

    async def run_conditional_processing(
        self,
        sample_type_batches: Dict[str, List[List[str]]],
        data_intake_output: LinkerOutput,
    ) -> Dict[str, Any]:
        """
        Run the complete conditional processing workflow.
        
        Parameters
        ----------
        sample_type_batches : Dict[str, List[List[str]]]
            Dictionary mapping sample_type -> list of batches
        data_intake_output : LinkerOutput
            Output from the data intake workflow
            
        Returns
        -------
        Dict[str, Any]
            Conditional processing workflow results
        """
        start_time = time.time()
        
        
        # Calculate total batches
        total_batches = sum(len(batches) for batches in sample_type_batches.values())
        # Unified progress bar across all sample types
        pbar = tqdm(total=total_batches, desc="Conditional - batches", unit="batch")
        
        all_batch_results = []
        successful_batches = 0
        failed_batches = 0
        
        try:
            # Prepare all batch tasks across sample types
            batch_jobs = []  # (global_index, sample_type, batch_samples)
            for sample_type, batches in sample_type_batches.items():
                if sample_type not in TARGET_FIELD_CONFIG["conditional_processing"]:
                    logger.warning(f"⚠️ No configuration found for sample_type: {sample_type}. Skipping {len(batches)} batches.")
                    try:
                        pbar.update(len(batches))
                    except Exception:
                        pass
                    continue
                for batch_samples in batches:
                    batch_jobs.append((len(batch_jobs) + 1, sample_type, batch_samples))

            sem = asyncio.Semaphore(self.max_workers) if self.max_workers else None

            async def run_one(job_index: int, sample_type: str, batch_samples: List[str]):
                try:
                    if sem:
                        async with sem:
                            pass
                    try:
                        pbar.set_description(f"Conditional - {sample_type}")
                        pbar.set_postfix_str(f"type={sample_type}, samples={len(batch_samples)}")
                    except Exception:
                        pass
                    result = await self.process_sample_type_batch(
                        batch_samples=batch_samples,
                        sample_type=sample_type,
                        batch_num=job_index,
                        total_batches=total_batches,
                        data_intake_output=data_intake_output,
                    )
                    return job_index, sample_type, batch_samples, result
                finally:
                    try:
                        pbar.update(1)
                    except Exception:
                        pass

            tasks = [run_one(idx, st, bs) for idx, st, bs in batch_jobs]
            results = []
            for coro in asyncio.as_completed(tasks):
                results.append(await coro)

            for job_index, sample_type, batch_samples, batch_result in results:
                all_batch_results.append(batch_result)
                if batch_result.get("success"):
                    successful_batches += 1
                else:
                    failed_batches += 1
                    self.failed_items["missing_results"][f"{sample_type}_batch_{job_index}"] = {
                        "error": batch_result.get("message", "Unknown error"),
                        "samples": batch_samples,
                        "sample_type": sample_type
                    }
            
            execution_time = time.time() - start_time
            
            # Calculate statistics (robust to missing keys in failed batch results)
            total_samples = 0
            for batch_result in all_batch_results:
                try:
                    if isinstance(batch_result, dict) and "batch_samples" in batch_result:
                        total_samples += len(batch_result["batch_samples"])
                except Exception:
                    pass
            successful_samples = sum(len(batch_result["batch_samples"]) for batch_result in all_batch_results if batch_result.get("success") and "batch_samples" in batch_result)
            failed_samples = total_samples - successful_samples
            
            # Create output structure
            output = {
                "success": failed_batches == 0,
                "message": f"Conditional processing completed: {successful_batches}/{total_batches} batches successful",
                "execution_time_seconds": execution_time,
                "statistics": {
                    "total_batches": total_batches,
                    "successful_batches": successful_batches,
                    "failed_batches": failed_batches,
                    "total_samples": total_samples,
                    "successful_samples": successful_samples,
                    "failed_samples": failed_samples,
                    "target_fields_processed": self.target_fields,  # Keep global list for summary
                },
                "batch_results": all_batch_results,
                "failed_items": self.failed_items,
                "session_directory": str(self.session_directory),
                "conditional_processing_directory": str(self.conditional_dir),
                "output_files": {
                    "conditional_processing_output": str(self.conditional_output_file)
                },
                "timestamp": datetime.now().isoformat()
            }
            
            # Save conditional processing output
            with open(self.conditional_output_file, "w") as f:
                json.dump(output, f, indent=2)
            
            if output["success"]:
                logger.info(f"✅ Conditional processing workflow completed successfully in {execution_time:.2f} seconds")
            else:
                logger.warning(f"⚠️ Conditional processing workflow completed with {failed_batches} failed batches in {execution_time:.2f} seconds")
            
            logger.info(f"📊 Statistics: {output['statistics']}")
            
            return output
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"❌ Conditional processing workflow failed: {str(e)}")
            
            error_output = {
                "success": False,
                "message": f"Conditional processing workflow failed: {str(e)}",
                "execution_time_seconds": execution_time,
                "error": str(e),
                "session_directory": str(self.session_directory),
                "timestamp": datetime.now().isoformat()
            }
            
            # Save error output
            with open(self.conditional_output_file, "w") as f:
                json.dump(error_output, f, indent=2)
            
            return error_output
        finally:
            try:
                pbar.close()
            except Exception:
                pass


async def run_conditional_processing_workflow(
    sample_type_batches: Dict[str, List[List[str]]],
    data_intake_output: LinkerOutput,
    session_directory: str,
    target_fields: List[str] = None,
    model_provider=None,
    max_tokens: int = None,
    max_workers: int = None,
) -> Dict[str, Any]:
    """
    Run the conditional processing workflow.
    
    Parameters
    ----------
    sample_type_batches : Dict[str, List[List[str]]]
        Dictionary mapping sample_type -> list of batches
    data_intake_output : LinkerOutput
        Output from the data intake workflow
    session_directory : str
        Path to the session directory
    target_fields : List[str], optional
        List of target fields to process
    model_provider : ModelProvider, optional
        Base model provider (will be specialized per operation)
    max_tokens : int, optional
        Maximum tokens for LLM responses
        
    Returns
    -------
    Dict[str, Any]
        Conditional processing workflow results
    """
    workflow = ConditionalProcessingWorkflow(
        session_directory=session_directory,
        target_fields=target_fields,
        model_provider=model_provider,
        max_tokens=max_tokens,
        max_workers=max_workers,
    )
    
    return await workflow.run_conditional_processing(sample_type_batches, data_intake_output)


if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="Conditional processing workflow for field-specific curation and normalization")
    
    parser.add_argument("--session-dir", "-d", required=True, help="Session directory path")
    parser.add_argument("--sample-type-batches", "-b", required=True, help="JSON file with sample type batches")
    parser.add_argument("--target-fields", "-f", help="Comma-separated list of target fields")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Set up logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Parse target fields
    target_fields = None
    if args.target_fields:
        target_fields = [f.strip() for f in args.target_fields.split(",")]
    
    # Load sample type batches
    try:
        with open(args.sample_type_batches, "r") as f:
            sample_type_batches = json.load(f)
    except Exception as e:
        print(f"❌ Failed to load sample type batches: {e}")
        sys.exit(1)
    
    # Mock data intake output for standalone testing
    from src.models import LinkerOutput
    mock_data_intake = LinkerOutput(
        success=True,
        message="Mock data intake output",
        execution_time_seconds=0.0,
        sample_ids_requested=[],
        session_directory=args.session_dir,
        files_created=[],
        successfully_linked=[],
        failed_linking=[],
        warnings=[],
        sample_ids_for_curation=[],
        recommended_curation_fields=target_fields or ["Disease", "Tissue", "Age", "Organ"],
        fields_removed_during_cleaning=[],
        linked_data=None,
        cleaned_metadata_files=None,
        cleaned_series_metadata=None,
        cleaned_sample_metadata=None,
        cleaned_abstract_metadata=None,
    )
    
    # Run workflow
    async def main():
        result = await run_conditional_processing_workflow(
            sample_type_batches=sample_type_batches,
            data_intake_output=mock_data_intake,
            session_directory=args.session_dir,
            target_fields=target_fields,
        )
        
        print(f"Conditional Processing Result: {'✅ Success' if result['success'] else '❌ Failed'}")
        print(f"Message: {result['message']}")
        if result['success']:
            print(f"Statistics: {result['statistics']}")
        
        return 0 if result['success'] else 1
    
    # Run the async main function
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
