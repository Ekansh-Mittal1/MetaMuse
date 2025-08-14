"""
Batch samples workflow for processing multiple GEO samples with comprehensive metadata extraction.

This script randomly selects 100 unique GSM samples from Age.txt and processes them in batches of 5
using the batch_targets workflow. It extracts all target metadata fields and organizes results
in two parquet formats: a streamlined parquet file with key columns for analysis and a comprehensive parquet file with
detailed curation and normalization data.

Output Structure:
batch/
├── batch_results.parquet                    # Streamlined results parquet (key columns only)
├── comprehensive_batch_results.parquet      # Comprehensive results parquet (all data)
├── processing_log.txt                   # Detailed processing log
├── sample_tracking.json                 # Sandbox ID mapping and status
├── failed_samples.json                 # Failed samples for retry
├── GSM1006725/
│   ├── series_metadata.json           # Raw series data
│   ├── sample_metadata.json           # Raw sample data
│   └── abstract_metadata.json         # Raw abstract data
└── ...
"""

import asyncio
import csv
import json
import pandas as pd
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import logging
from tqdm import tqdm

from dotenv import load_dotenv
from agents import ModelProvider

from src.workflows.batch_targets import run_initial_processing, run_conditional_processing, run_unified_normalization, InitialProcessingResult
from src.models import LinkerOutput
from src.models.common import KeyValue

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Suppress HTTP request logging from httpx/openai
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


class BatchSamplesProcessor:
    """
    Processes batches of GEO samples with comprehensive metadata extraction.
    """

    def __init__(
        self,
        output_dir: str = "batch",
        sample_count: int = 100,
        batch_size: int = 5,
        age_file: str = "Age.txt",
        model_provider: ModelProvider = None,
        max_tokens: int = None,
        target_fields: list = None,
        sample_type_filter: str = None,
    ):
        """
        Initialize the batch samples processor.

        Parameters
        ----------
        output_dir : str
            Directory to save batch results
        sample_count : int
            Number of samples to process (default: 100)
        batch_size : int
            Number of samples per batch (default: 5)
        age_file : str
            Path to Age.txt file containing GSM IDs
        model_provider : ModelProvider, optional
            Model provider for LLM requests
        max_tokens : int, optional
            Maximum tokens for LLM responses
        target_fields : list, optional
            List of target fields to process. If None, processes all available fields.
            Available fields: disease, tissue, organ, cell_line, developmental_stage,
            ethnicity, gender, age, organism, pubmed_id, platform_id, instrument
        sample_type_filter : str, optional
            Filter to process only specific sample type. If None, processes all sample types.
            Available types: primary_sample, cell_line, unknown
        """
        self.output_dir = Path(output_dir)
        self.sample_count = sample_count
        self.batch_size = batch_size
        self.age_file = age_file
        self.model_provider = model_provider
        self.max_tokens = max_tokens
        self.target_fields = target_fields
        self.sample_type_filter = sample_type_filter
        
        # Validate sample_type_filter if provided
        if self.sample_type_filter and self.sample_type_filter not in ["primary_sample", "cell_line", "unknown"]:
            raise ValueError(f"Invalid sample_type_filter: {self.sample_type_filter}. Must be one of: primary_sample, cell_line, unknown")

        # Create timestamped output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.batch_dir = self.output_dir / f"batch_{timestamp}"
        self.batch_dir.mkdir(parents=True, exist_ok=True)
        
        # Create unified discovery directory structure
        self.discovery_dir = self.batch_dir / "discovery"
        self.discovery_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories within discovery
        self.discovery_outputs_dir = self.discovery_dir / "outputs"
        self.discovery_outputs_dir.mkdir(exist_ok=True)
        
        self.discovery_raw_data_dir = self.discovery_dir / "raw_data"
        self.discovery_raw_data_dir.mkdir(exist_ok=True)

        # Initialize tracking data structures
        self.sample_tracking = {}
        self.processed_samples = []
        self.failed_samples = []
        
        # Error tracking structures
        self.batch_errors = {}  # Track errors by batch number
        self.target_field_errors = {}  # Track errors by target field
        self.sample_errors = {}  # Track errors by sample ID
        self.stage_errors = {}  # Track errors by processing stage
        self.error_summary = {
            "total_batches": 0,
            "failed_batches": 0,
            "total_samples": 0,
            "failed_samples": 0,
            "total_target_fields": 0,
            "failed_target_fields": 0,
            "stage_failures": {},
            "error_types": {},
            "timestamp": datetime.now().isoformat()
        }
        
        # Enhanced error tracking for rerun capability
        self.failed_items = {
            "samples": {},  # sample_id -> failure details
            "target_fields": {},  # target_field -> {sample_id -> failure details}
            "curation_failures": {},  # target_field -> {sample_id -> error}
            "normalization_failures": {},  # target_field -> {sample_id -> error}
            "missing_results": {},  # track missing curation/normalization results
        }
        
        # Store original configuration for rerun
        self.batch_config = {
            "sample_count": sample_count,
            "batch_size": batch_size,
            "target_fields": target_fields or "all",
            "sample_type_filter": sample_type_filter or "all",
            "model_provider": str(model_provider) if model_provider else None,
            "max_tokens": max_tokens,
            "age_file": age_file,
        }

        # Set up logging to file
        log_handler = logging.FileHandler(self.batch_dir / "processing_log.txt")
        log_handler.setLevel(logging.INFO)
        log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        log_handler.setFormatter(log_formatter)
        logger.addHandler(log_handler)

        # Caching for sample type discovery
        self.cached_initial_results: Dict[str, Any] = {}

        logger.info(f"Output directory: {self.batch_dir}")
        logger.info(f"Sample count: {sample_count}, Batch size: {batch_size}")
        if target_fields:
            logger.info(f"Target fields: {', '.join(target_fields)}")
        else:
            logger.info("Target fields: All available fields")

    def load_age_samples(self) -> List[str]:
        """
        Load and validate GSM samples from Age.txt file.

        Returns
        -------
        List[str]
            List of valid GSM IDs
        """

        try:
            with open(self.age_file, "r") as f:
                lines = f.readlines()

            # Extract valid GSM IDs
            gsm_samples = []
            for line in lines:
                line = line.strip()
                if line.startswith("GSM") and line[3:].isdigit():
                    gsm_samples.append(line)
            return gsm_samples

        except FileNotFoundError:
            logger.error(f"Age file not found: {self.age_file}")
            print("❌ Full traceback:")
            import traceback
            print(traceback.format_exc())
            raise
        except Exception as e:
            logger.error(f"Error loading age file: {e}")
            print("❌ Full traceback:")
            import traceback
            print(traceback.format_exc())
            raise

    def select_random_samples(self, all_samples: List[str]) -> List[str]:
        """
        Randomly select unique samples for processing.

        Parameters
        ----------
        all_samples : List[str]
            All available GSM samples

        Returns
        -------
        List[str]
            Selected GSM samples
        """
        if len(all_samples) < self.sample_count:
            logger.warning(
                f"Requested {self.sample_count} samples but only {len(all_samples)} available"
            )
            selected = all_samples.copy()
        else:
            selected = random.sample(all_samples, self.sample_count)

        # Note: Selected samples will be saved in consolidate_output_files()

        return selected

    def create_batches(self, samples: List[str]) -> List[List[str]]:
        """
        Split samples into batches of specified size.

        Parameters
        ----------
        samples : List[str]
            List of GSM samples

        Returns
        -------
        List[List[str]]
            List of sample batches
        """
        batches = []
        for i in range(0, len(samples), self.batch_size):
            batch = samples[i : i + self.batch_size]
            batches.append(batch)

        logger.info(f"Created {len(batches)} batches of size {self.batch_size}")
        return batches

    async def discover_sample_types(
        self, samples: List[str], discovery_batch_size: int = 10
    ) -> Dict[str, str]:
        """
        Discover sample types for all samples using initial processing in small batches.
        
        Parameters
        ----------
        samples : List[str]
            List of GSM samples to discover types for
        discovery_batch_size : int
            Batch size for discovery (default: 10)
            
        Returns
        -------
        Dict[str, str]
            Dictionary mapping sample_id -> sample_type
        """
        
        logger.info(f"🔍 Starting sample type discovery for {len(samples)} samples")
        
        # Create discovery batches
        discovery_batches = []
        for i in range(0, len(samples), discovery_batch_size):
            batch = samples[i : i + discovery_batch_size]
            discovery_batches.append(batch)
        
        logger.info(f"📋 Created {len(discovery_batches)} discovery batches")
        
        # Store all sample type mappings, sandbox IDs, and cached results
        all_sample_type_mapping = {}
        sandbox_id_mapping = {}
        cached_initial_results = {}
        
        # Process each discovery batch
        for batch_num, batch_samples in enumerate(discovery_batches, 1):
            logger.info(f"🔍 Processing discovery batch {batch_num}/{len(discovery_batches)}: {batch_samples}")
            
            try:
                # Prepare input for initial processing
                input_text = " ".join(batch_samples)
                
                # Run initial processing to discover sample types using unified discovery directory
                initial_result = await run_initial_processing(
                    input_text=input_text,
                    session_id="discovery",  # Use unified discovery session
                    sandbox_dir=str(self.discovery_dir),
                    model_provider=self.model_provider,
                    max_tokens=self.max_tokens,
                    enable_parallel_execution=True,
                    error_tracker=self,
                    batch_number=batch_num,  # Pass batch number for output file naming
                )
                
                if initial_result.success:
                    # Store sample type mapping
                    all_sample_type_mapping.update(initial_result.sample_type_mapping)
                    
                    # Store sandbox ID mapping and cache results per individual sample
                    for sample_id in batch_samples:
                        sandbox_id_mapping[sample_id] = initial_result.session_id
                        
                        # Cache discovery session info and initial curation results for each individual sample
                        cached_initial_results[sample_id] = {
                            "discovery_session_id": initial_result.session_id,
                            "session_directory": initial_result.session_directory,
                            "sample_type": initial_result.sample_type_mapping.get(sample_id, "failed"),
                            "batch_id": batch_num,  # Include batch ID
                            "initial_curator_outputs": initial_result.initial_curator_outputs  # Contains sample_type curation results only
                        }
                    
                    
                    # Log sample type distribution for this batch
                    batch_distribution = {}
                    for sample_id in batch_samples:
                        sample_type = initial_result.sample_type_mapping.get(sample_id, "failed")
                        batch_distribution[sample_type] = batch_distribution.get(sample_type, 0) + 1
                    
                    
                else:
                    logger.error(f"❌ Discovery batch {batch_num} failed: {initial_result.error_message}")
                    print("❌ Full traceback:")
                    import traceback
                    print(traceback.format_exc())
                    
                    # Mark all samples in this batch as failed
                    for sample_id in batch_samples:
                        all_sample_type_mapping[sample_id] = "failed"
                        
            except Exception as e:
                logger.error(f"❌ Discovery batch {batch_num} failed with exception: {str(e)}")
                print("❌ Full traceback:")
                import traceback
                print(traceback.format_exc())
                
                # Mark all samples in this batch as failed
                for sample_id in batch_samples:
                    all_sample_type_mapping[sample_id] = "failed"
                    # Still cache failed samples for tracking
                    cached_initial_results[sample_id] = {
                        "discovery_session_id": "discovery",
                        "session_directory": str(self.discovery_dir),
                        "sample_type": "failed",
                        "batch_id": batch_num
                    }
        
                # Create unified sample type mapping with batch IDs
        unified_sample_type_mapping = {}
        for sample_id, cache_info in cached_initial_results.items():
            unified_sample_type_mapping[sample_id] = {
                "sample_type": cache_info["sample_type"],
                "batch_id": cache_info["batch_id"]
            }
        
        # Save unified sample_type_mapping.json to discovery directory
        unified_mapping_file = self.discovery_dir / "sample_type_mapping.json"
        with open(unified_mapping_file, "w") as f:
            json.dump(unified_sample_type_mapping, f, indent=2)
                
        # Store data for consolidated output (will be saved in consolidate_output_files())
        self.sample_type_mapping = all_sample_type_mapping
        self.unified_sample_type_mapping = unified_sample_type_mapping
        self.sandbox_id_mapping = sandbox_id_mapping
        
        # Store in instance for immediate use
        self.cached_initial_results = cached_initial_results
        
        # Debug: Log cached sample IDs for troubleshooting
        cached_sample_ids = list(cached_initial_results.keys())
        logger.info(f"💾 Cached {len(cached_sample_ids)} sample mappings: {cached_sample_ids}")
        
        # Store debugging info for consolidated output
        self.discovery_debug_info = {
            "cached_sample_ids": cached_sample_ids,
            "total_samples": len(samples)
        }
        
        # Log final sample type distribution
        final_distribution = {}
        for sample_type in all_sample_type_mapping.values():
            final_distribution[sample_type] = final_distribution.get(sample_type, 0) + 1
        
        logger.info(f"🎯 Final sample type distribution: {final_distribution}")
        
        # Report failed samples
        failed_samples = [sid for sid, stype in all_sample_type_mapping.items() if stype == "failed"]
        if failed_samples:
            logger.warning(f"⚠️  {len(failed_samples)} samples failed sample type determination: {failed_samples}")
            
            # Save failed samples for reporting
            try:
                with open(self.batch_dir / "failed_sample_type_discovery.json", "w") as f:
                    json.dump({
                        "failed_samples": failed_samples,
                        "reason": "Sample type determination failed during discovery phase",
                        "total_failed": len(failed_samples),
                        "total_samples": len(samples)
                    }, f, indent=2)
            except Exception as save_error:
                logger.error(f"⚠️  Could not save failed samples report: {save_error}")
                print("❌ Full traceback:")
                import traceback
                print(traceback.format_exc())
        
        return all_sample_type_mapping

    def create_sample_type_batches(
        self, samples: List[str], sample_type_mapping: Dict[str, str]
    ) -> Dict[str, List[List[str]]]:
        """
        Create batches grouped by sample type with fixed batch size and overflow handling.
        
        Parameters
        ----------
        samples : List[str]
            List of GSM samples
        sample_type_mapping : Dict[str, str]
            Dictionary mapping sample_id -> sample_type
            
        Returns
        -------
        Dict[str, List[List[str]]]
            Dictionary mapping sample_type -> list of batches
        """
        
        # Group samples by sample type
        sample_type_groups = {
            "primary_sample": [],
            "cell_line": [],
            "unknown": [],
            "failed": []
        }
        
        for sample_id in samples:
            sample_type = sample_type_mapping.get(sample_id, "failed")
            sample_type_groups[sample_type].append(sample_id)
        
        # Create batches for each sample type
        sample_type_batches = {}
        
        for sample_type, type_samples in sample_type_groups.items():
            if not type_samples:
                continue  # Skip empty groups
                
            type_batches = []
            for i in range(0, len(type_samples), self.batch_size):
                batch = type_samples[i : i + self.batch_size]
                type_batches.append(batch)
            
            sample_type_batches[sample_type] = type_batches
            
       
        return sample_type_batches

    async def process_sample_type_batch(
        self, batch_samples: List[str], sample_type: str, batch_num: int, total_batches: int,
        cached_initial_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process a single batch of samples with consistent sample type using cached initial results.

        Parameters
        ----------
        batch_samples : List[str]
            List of GSM samples in this batch (all of same sample_type)
        sample_type : str
            The sample type for this batch (primary_sample, cell_line, unknown, failed)
        batch_num : int
            Batch number for tracking within this sample type
        total_batches : int
            Total number of batches for this sample type
        cached_initial_results : Dict[str, Any]
            Cached initial processing results from discovery phase

        Returns
        -------
        Dict[str, Any]
            Batch processing results
        """
        logger.info(f"🎯 Processing {sample_type} batch {batch_num}/{total_batches} with samples: {batch_samples}")

        batch_start_time = time.time()

        try:
            # Step 1: Load cached discovery session data for samples in this batch            
            # Group samples by their discovery sessions
            discovery_sessions = {}
            missing_samples = []
                        
            for sample_id in batch_samples:
                if sample_id in cached_initial_results:
                    cache_info = cached_initial_results[sample_id]
                    discovery_session_id = cache_info["discovery_session_id"]
                    
                    if discovery_session_id not in discovery_sessions:
                        discovery_sessions[discovery_session_id] = {
                            "session_directory": cache_info["session_directory"],
                            "samples": []
                        }
                    discovery_sessions[discovery_session_id]["samples"].append(sample_id)
                else:
                    missing_samples.append(sample_id)
            
            # Debug: Log what we found vs what's missing
            
            if missing_samples:
                logger.error(f"❌ No cached discovery data found for samples: {missing_samples}")
                print("❌ Full traceback:")
                import traceback
                print(traceback.format_exc())
                
                # Track these samples as failed
                for sample_id in batch_samples:
                    self.failed_samples.append(sample_id)
                    self.sample_tracking[sample_id] = {
                        "batch_num": batch_num,
                        "sample_type": sample_type,
                        "sandbox_id": "unknown",
                        "status": "failed",
                        "error": f"Missing cached data for sample: {sample_id}",
                        "timestamp": datetime.now().isoformat(),
                    }
                
                return {
                    "success": False,
                    "sample_type": sample_type,
                    "batch_samples": batch_samples,
                    "error": f"Missing cached data for samples: {missing_samples}",
                    "processing_time": time.time() - batch_start_time
                }
            
            if not discovery_sessions:
                logger.error(f"❌ No discovery sessions found for {sample_type} batch {batch_num}")
                print("❌ Full traceback:")
                import traceback
                print(traceback.format_exc())
                return {
                    "success": False,
                    "sample_type": sample_type,
                    "batch_samples": batch_samples,
                    "error": "No discovery sessions found",
                    "processing_time": time.time() - batch_start_time
                }

            # Load and combine data from all discovery sessions that contain our batch samples
            combined_linked_data = {}
            
            try:
                # Collect unique batch numbers from samples in this batch
                batch_numbers = set()
                for sample_id in batch_samples:
                    if sample_id in self.cached_initial_results:
                        batch_numbers.add(self.cached_initial_results[sample_id]["batch_id"])
                
                # Read from numbered output files in unified discovery structure
                for batch_num in batch_numbers:
                    outputs_dir = self.discovery_dir / "outputs"
                    data_intake_file = outputs_dir / f"data_intake_output_batch_{batch_num}.json"
                    
                    if not data_intake_file.exists():
                        logger.error(f"❌ data_intake_output_batch_{batch_num}.json not found in {outputs_dir}")
                        print("❌ Full traceback:")
                        import traceback
                        print(traceback.format_exc())
                        return {
                            "success": False,
                            "sample_type": sample_type,
                            "batch_samples": batch_samples,
                            "error": f"Discovery batch data not found: {data_intake_file}",
                            "processing_time": time.time() - batch_start_time
                        }
                    
                    with open(data_intake_file, "r") as f:
                        session_data = json.load(f)
                    
                    # Use curation packages instead of linked_data (which is null)
                    session_curation_packages = session_data.get("curation_packages") or []
                    
                    # Get samples from this batch that belong to current discovery batch
                    batch_samples_in_discovery_batch = [
                        sample_id for sample_id in batch_samples 
                        if sample_id in self.cached_initial_results and 
                        self.cached_initial_results[sample_id]["batch_id"] == batch_num
                    ]
                    
                    # Convert curation packages to linked_data format for samples in this batch
                    for i, package in enumerate(session_curation_packages):
                        sample_id = package.get("sample_id")
                        if sample_id and sample_id in batch_samples_in_discovery_batch:
                            # Create linked data entry using sample_id as key
                            combined_linked_data[sample_id] = {
                                "series_metadata": package.get("series_metadata", {}),
                                "sample_metadata": package.get("sample_metadata", {}),
                                "abstract_metadata": package.get("abstract_metadata", {})
                            }
                            
                    
                
            except Exception as load_error:
                logger.error(f"❌ Failed to load discovery session data: {load_error}")
                print("❌ Full traceback:")
                import traceback
                print(traceback.format_exc())
                return {
                    "success": False,
                    "sample_type": sample_type,
                    "batch_samples": batch_samples,
                    "error": f"Failed to load discovery data: {load_error}",
                    "processing_time": time.time() - batch_start_time
                }

            # Create session directory for this conditional batch
            batch_session_dir = self.batch_dir / f"{sample_type}_batch_{batch_num}"
            batch_session_dir.mkdir(parents=True, exist_ok=True)
            
            # Create LinkerOutput using the combined filtered data
            try:
                # Convert combined_linked_data dict to List[KeyValue] format expected by LinkerOutput
                linked_data_list = []
                for sample_id, metadata in combined_linked_data.items():
                    # Convert the metadata dict to a JSON string for the value
                    linked_data_list.append(KeyValue(
                        key=sample_id,
                        value=json.dumps(metadata)
                    ))
                
                # Reconstruct curation_packages from combined_linked_data to fix the NoneType error
                curation_packages = []
                for sample_id, metadata in combined_linked_data.items():
                    # Create CurationDataPackage from the metadata structure
                    curation_package = {
                        "sample_id": sample_id,
                        "series_metadata": metadata.get("series_metadata"),
                        "sample_metadata": metadata.get("sample_metadata"),
                        "abstract_metadata": metadata.get("abstract_metadata")
                    }
                    curation_packages.append(curation_package)
                
                # Validate that we have curation packages for all samples
                if len(curation_packages) != len(batch_samples):
                    logger.warning(f"⚠️  Mismatch: {len(curation_packages)} curation packages vs {len(batch_samples)} batch samples")
                    # Ensure we have packages for all samples, even if empty
                    for sample_id in batch_samples:
                        if not any(pkg.get("sample_id") == sample_id for pkg in curation_packages):
                            logger.warning(f"⚠️  Creating empty curation package for sample {sample_id}")
                            curation_packages.append({
                                "sample_id": sample_id,
                                "series_metadata": {},
                                "sample_metadata": {},
                                "abstract_metadata": {}
                            })
                                
                # Create LinkerOutput for conditional processing using combined data from all discovery sessions
                batch_data_intake_output = LinkerOutput(
                    success=True,
                    message=f"Combined from {len(discovery_sessions)} discovery sessions for {sample_type} processing",
                    execution_time_seconds=0.0,  # Combined processing time
                    sample_ids_requested=batch_samples,
                    session_directory=str(batch_session_dir),
                    fields_removed_during_cleaning=[],
                    files_created=[],
                    sample_ids_for_curation=batch_samples,
                    linked_data=linked_data_list,
                    curation_packages=curation_packages  # Add the reconstructed curation packages
                )
                              
            except Exception as creation_error:
                logger.error(f"❌ Failed to create LinkerOutput: {creation_error}")
                print("❌ Full traceback:")
                import traceback
                print(traceback.format_exc())
                return {
                    "success": False,
                    "sample_type": sample_type,
                    "batch_samples": batch_samples,
                    "error": f"Failed to create LinkerOutput: {creation_error}",
                    "processing_time": time.time() - batch_start_time
                }

            # Create sample type mapping for this batch
            batch_sample_type_mapping = {
                sample_id: sample_type for sample_id in batch_samples
            }
            
            # Create grouped samples structure
            grouped_samples = {sample_type: batch_samples}
            
            # Construct the InitialProcessingResult structure for conditional processing
            initial_result = InitialProcessingResult(
                success=True,
                session_id=f"{sample_type}_batch_{batch_num}",
                session_directory=str(batch_session_dir),
                data_intake_output=batch_data_intake_output,
                sample_ids=batch_samples,
                direct_fields={},  # Not needed for conditional processing
                initial_curation_data={},  # Not needed for conditional processing  
                initial_curator_outputs={},  # Not needed for conditional processing
                sample_type_mapping=batch_sample_type_mapping,
                grouped_samples=grouped_samples
            )

            # Step 2: Run conditional processing using cached initial results
            
            # Combine initial curator outputs from all samples in the batch
            # Note: Now only contains sample_type results since disease/organ moved to conditional processing
            combined_initial_curator_outputs = {}
            for sample_id in batch_samples:
                sample_curator_outputs = cached_initial_results[sample_id]["initial_curator_outputs"]
                for field_name, curator_output in sample_curator_outputs.items():
                    if field_name not in combined_initial_curator_outputs:
                        combined_initial_curator_outputs[field_name] = curator_output
                    else:
                        # If we already have this field, we need to merge the curation_results
                        existing_output = combined_initial_curator_outputs[field_name]
                        if isinstance(existing_output, dict) and isinstance(curator_output, dict):
                            # Get existing curation_results
                            existing_results = existing_output.get("curation_results", [])
                            new_results = curator_output.get("curation_results", [])
                            
                            # Merge unique results (avoid duplicates by sample_id)
                            existing_sample_ids = {result.get("sample_id") for result in existing_results}
                            for result in new_results:
                                if result.get("sample_id") not in existing_sample_ids:
                                    existing_results.append(result)
                            
                            # Update the combined output
                            combined_initial_curator_outputs[field_name] = {
                                **existing_output,
                                "curation_results": existing_results,
                                "sample_ids_requested": list(set(
                                    existing_output.get("sample_ids_requested", []) + 
                                    curator_output.get("sample_ids_requested", [])
                                ))
                            }
                     
            # Update initial_result with the combined curator outputs
            initial_result.initial_curator_outputs = combined_initial_curator_outputs
           
            conditional_result = await run_conditional_processing(
                initial_result=initial_result,
                model_provider=self.model_provider,
                max_tokens=self.max_tokens,
                enable_parallel_execution=True,
                error_tracker=self,
            )

            if not conditional_result.success:
                logger.error(f"❌ Conditional processing failed for {sample_type} batch {batch_num}: {conditional_result.error_message}")
                print("❌ Full traceback:")
                import traceback
                print(traceback.format_exc())
                return {
                    "success": False,
                    "sample_type": sample_type,
                    "batch_samples": batch_samples,
                    "error": conditional_result.error_message,
                    "processing_time": time.time() - batch_start_time
                }

            # Run unified normalization for all relevant fields
            logger.info("🔬 Running unified normalization...")
            normalization_data = await run_unified_normalization(
                initial_result=initial_result,
                conditional_result=conditional_result,
                model_provider=self.model_provider,
                max_tokens=self.max_tokens,
                enable_parallel_execution=True,
                error_tracker=self,
            )

            # Update the conditional result with the normalization data
            conditional_result.unified_normalization_data = normalization_data

            # Save the combined results to batch_targets_output.json
            from src.tools.batch_processing_tools import (
                save_batch_results,
                combine_target_field_results,
                extract_direct_fields_from_data_intake,
                extract_curation_candidates,
            )
            
            try:
                # 1) Extract direct fields for these samples from the data intake output
                try:
                    direct_fields = extract_direct_fields_from_data_intake(
                        data_intake_output=batch_data_intake_output,
                        sample_ids=batch_samples,
                    )
                except Exception as extract_error:
                    logger.warning(f"⚠️  Failed to extract direct fields: {extract_error}")
                    logger.warning("Continuing with empty direct fields...")
                    # Continue with empty direct fields instead of crashing
                    direct_fields = {sample_id: {} for sample_id in batch_samples}

                # 2) Convert curator outputs into per-field per-sample curation results
                #    expected by combine_target_field_results
                curator_outputs_by_field = {
                    **(initial_result.initial_curator_outputs or {}),
                    **(conditional_result.all_sample_type_outputs or {}),
                }

                curation_results_by_field = {}
                for field_key, curator_output in curator_outputs_by_field.items():
                    try:
                        # Normalize field key and extract candidates for all samples in this batch
                        normalized_field_key = str(field_key).lower().replace(" ", "_")
                        
                        # For sample_type field, filter samples to only those that have cached results
                        # This prevents "No curation result found" warnings for samples that weren't in the discovery batch
                        if normalized_field_key == "sample_type":
                            # Only include samples that are in our cached_initial_results
                            filtered_sample_ids = [s for s in batch_samples if s in self.cached_initial_results]
                            if not filtered_sample_ids:
                                logger.warning("No samples found in cached results for sample_type field")
                                continue
                            samples_to_process = filtered_sample_ids
                        else:
                            samples_to_process = batch_samples
                            
                        field_results = extract_curation_candidates(
                            curator_output=curator_output,
                            target_field=normalized_field_key,
                            sample_ids=samples_to_process,
                            error_tracker=self,
                        )
                        
                        # For sample_type, add fallback results for any missing samples
                        if normalized_field_key == "sample_type":
                            for sample_id in batch_samples:
                                if sample_id not in field_results and sample_id in self.cached_initial_results:
                                    # Use cached sample_type as fallback
                                    cached_sample_type = self.cached_initial_results[sample_id]["sample_type"]
                                    field_results[sample_id] = {
                                        "candidates": [{
                                            "value": cached_sample_type,
                                            "confidence": 1.0,
                                            "source": "cached_discovery_result",
                                            "context": "Retrieved from discovery phase cache"
                                        }],
                                        "best_candidate": {
                                            "value": cached_sample_type,
                                            "confidence": 1.0,
                                            "source": "cached_discovery_result",
                                            "context": "Retrieved from discovery phase cache"
                                        },
                                        "candidate_count": 1
                                    }
                        
                        curation_results_by_field[normalized_field_key] = field_results
                    except Exception as e:
                        logger.warning(
                            f"Failed to extract curation candidates for field '{field_key}': {e}"
                        )

                # 3) Combine all results for this batch
                unified_results = combine_target_field_results(
                    sample_ids=batch_samples,
                    direct_fields=direct_fields,
                    curation_results=curation_results_by_field,
                    normalization_results=normalization_data,
                )
                
                # Save unified batch results to the session directory
                batch_session_dir = self.batch_dir / f"{sample_type}_batch_{batch_num}"
                
                # Wrap results in the expected structure for CSV generation
                wrapped_results = {
                    "sample_results": unified_results,
                    "processing_summary": {
                        "sample_type": sample_type,
                        "batch_num": batch_num,
                        "samples_processed": len(batch_samples)
                    }
                }
                
                save_batch_results(
                    results=wrapped_results,
                    session_directory=str(batch_session_dir),
                    filename="batch_targets_output.json"
                )
                
                
                # Also save the data_intake_output.json for this batch
                data_intake_output_path = batch_session_dir / "data_intake_output.json"
                with open(data_intake_output_path, "w") as f:
                    # Save the LinkerOutput that was created for this batch
                    json.dump(batch_data_intake_output.model_dump(), f, indent=2)
                
                
            except Exception as save_error:
                logger.error(f"❌ Failed to save batch results: {save_error}")
                print("❌ Full traceback:")
                import traceback
                print(traceback.format_exc())

            batch_end_time = time.time()
            processing_time = batch_end_time - batch_start_time

            # Extract sandbox session ID from result
            sandbox_id = initial_result.session_id

            # Track samples in this batch
            for sample in batch_samples:
                self.sample_tracking[sample] = {
                    "batch_num": batch_num,
                    "sample_type": sample_type,
                    "sandbox_id": sandbox_id,
                    "status": "completed",
                    "processing_time": processing_time / len(batch_samples),  # Average per sample
                    "timestamp": datetime.now().isoformat(),
                }

            logger.info(f"✅ {sample_type} batch {batch_num}/{total_batches} completed successfully in {processing_time:.2f}s")
            self.processed_samples.extend(batch_samples)

            # Create result structure
            result = {
                "success": True,
                "sample_type": sample_type,
                "batch_samples": batch_samples,
                "initial_result": initial_result,
                "conditional_result": conditional_result,
                "session_id": sandbox_id,
                "processing_time": processing_time,
                "samples_processed": len(batch_samples)
            }

            return result

        except Exception as e:
            logger.error(f"❌ Error processing {sample_type} batch {batch_num}: {e}")
            print("❌ Full traceback:")
            import traceback
            print(traceback.format_exc())
            
            # Mark all samples in batch as failed
            for sample in batch_samples:
                self.sample_tracking[sample] = {
                    "batch_num": batch_num,
                    "sample_type": sample_type,
                    "sandbox_id": "unknown",
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                }
                # Track individual sample errors
                self.track_sample_error(
                    sample_id=sample,
                    error=str(e),
                    stage="batch_processing"
                )
                
                # Track complete sample failure for rerun capability
                self.track_sample_failure(
                    sample_id=sample,
                    error=str(e),
                    stage="batch_processing",
                    failure_type="batch_processing_error"
                )
            
            self.failed_samples.extend(batch_samples)
            
            # Track batch error
            self.track_batch_error(
                batch_num=batch_num,
                error=str(e),
                samples=batch_samples,
                stage="batch_processing"
            )
            
            return {"success": False, "message": str(e)}

    def extract_raw_metadata(self, sample_id: str, sandbox_id: str) -> None:
        """
        Extract and save raw metadata JSON files for a sample.

        Parameters
        ----------
        sample_id : str
            GSM sample ID
        sandbox_id : str
            Sandbox session ID containing the data
        """

        try:
            # Create raw_data directory and sample subdirectory using unified discovery structure
            sample_dir = self.discovery_raw_data_dir / sample_id
            sample_dir.mkdir(exist_ok=True)

            # Get the original discovery session directory for this sample
            if sample_id not in self.cached_initial_results:
                logger.warning(f"No cached discovery data found for {sample_id}")
                return
                
            # Use the unified discovery directory structure
            discovery_path = self.discovery_dir

            if not discovery_path.exists():
                logger.warning(
                    f"Discovery directory not found: {discovery_path}"
                )
                return

            # Load series-sample mapping to find the correct series directory
            mapping_file = discovery_path / "series_sample_mapping.json"
            if not mapping_file.exists():
                logger.warning(f"No series_sample_mapping.json found in {discovery_path}")
                return

            with open(mapping_file, "r") as f:
                mapping_data = json.load(f)

            # Find the correct series for this sample
            reverse_mapping = mapping_data.get("reverse_mapping", {})
            series_id = reverse_mapping.get(sample_id)

            if not series_id:
                logger.warning(f"No series found for sample {sample_id} in mapping")
                return

            # Find the correct series directory in the discovery session
            series_dir = discovery_path / series_id
            if not series_dir.exists():
                logger.warning(
                    f"Series directory not found for {sample_id}: {series_dir}"
                )
                return

            # Extract series metadata
            series_file = series_dir / f"{series_dir.name}_metadata.json"
            if series_file.exists():
                with open(series_file, "r") as f:
                    series_data = json.load(f)
                with open(sample_dir / "series_metadata.json", "w") as f:
                    json.dump(series_data, f, indent=2)

            # Extract sample metadata
            sample_file = series_dir / f"{sample_id}_metadata.json"
            if sample_file.exists():
                with open(sample_file, "r") as f:
                    sample_data = json.load(f)
                with open(sample_dir / "sample_metadata.json", "w") as f:
                    json.dump(sample_data, f, indent=2)
            else:
                logger.warning(
                    f"Sample metadata file not found for {sample_id}: {sample_file}"
                )

            # Extract abstract metadata (PMID file)
            pmid_files = list(series_dir.glob("PMID_*_metadata.json"))
            if pmid_files:
                with open(pmid_files[0], "r") as f:
                    abstract_data = json.load(f)
                with open(sample_dir / "abstract_metadata.json", "w") as f:
                    json.dump(abstract_data, f, indent=2)

        except Exception as e:
            logger.error(f"Error extracting raw metadata for {sample_id}: {e}")
            print("❌ Full traceback:")
            import traceback
            print(traceback.format_exc())

    def consolidate_output_files(self, selected_samples: List[str], sample_type_batches: Dict[str, List[List[str]]], all_results: List[Dict], start_time: float) -> None:
        """
        Consolidate all output data into comprehensive files instead of many small ones.
        
        Parameters
        ----------
        selected_samples : List[str]
            Initially selected samples
        sample_type_batches : Dict[str, List[List[str]]]
            Sample type batches created
        all_results : List[Dict]
            All batch processing results
        start_time : float
            Workflow start time
        """
        try:
            # Create comprehensive workflow summary
            workflow_summary = {
                "workflow_info": {
                    "type": "sample_type_based_batch_processing",
                    "timestamp": datetime.now().isoformat(),
                    "total_runtime_seconds": time.time() - start_time,
                    "configuration": {
                        "batch_size": self.batch_size,
                        "discovery_batch_size": 5,
                        "sample_count": len(selected_samples),
                        "model_provider": str(self.model_provider)
                    }
                },
                "sample_selection": {
                    "total_selected": len(selected_samples),
                    "selected_samples": selected_samples
                },
                "sample_type_discovery": {
                    "unified_sample_type_mapping": getattr(self, 'unified_sample_type_mapping', {}),
                    "legacy_sample_type_mapping": getattr(self, 'sample_type_mapping', {}),
                    "distribution": {sample_type: sum(1 for mapping in getattr(self, 'unified_sample_type_mapping', {}).values() if mapping.get('sample_type') == sample_type) for sample_type in set(mapping.get('sample_type') for mapping in getattr(self, 'unified_sample_type_mapping', {}).values())},
                    "discovery_structure": {
                        "discovery_directory": str(self.discovery_dir),
                        "outputs_directory": str(self.discovery_outputs_dir),
                        "raw_data_directory": str(self.discovery_raw_data_dir),
                        "unified_mapping_file": str(self.discovery_dir / "sample_type_mapping.json")
                    },
                    "discovery_sessions": {
                        sample_id: {
                            "discovery_session_id": cache_info["discovery_session_id"],
                            "session_directory": cache_info["session_directory"],
                            "sample_type": cache_info["sample_type"],
                            "batch_id": cache_info["batch_id"]
                        } for sample_id, cache_info in getattr(self, 'cached_initial_results', {}).items()
                    },
                    "debug_info": getattr(self, 'discovery_debug_info', {})
                },
                "batch_processing": {
                    "sample_type_batches": sample_type_batches,
                    "total_batches": sum(len(batches) for batches in sample_type_batches.values()),
                    "batch_results": [
                        {
                            "success": r.get("success", False),
                            "sample_type": r.get("sample_type", "unknown"),
                            "batch_samples": r.get("batch_samples", []),
                            "processing_time": r.get("processing_time", 0),
                            "samples_processed": r.get("samples_processed", 0),
                            "error": r.get("error", None) if not r.get("success", False) else None
                        } for r in all_results
                    ],
                    "overall_statistics": {
                        "total_samples_processed": len(self.processed_samples),
                        "total_samples_failed": len(self.failed_samples),
                        "overall_success_rate": len(self.processed_samples) / len(selected_samples) if selected_samples else 0,
                        "successful_batches": sum(1 for r in all_results if r.get("success", False)),
                        "failed_batches": sum(1 for r in all_results if not r.get("success", False))
                    }
                }
            }

            # Create comprehensive sample metadata
            sample_metadata = {
                "sample_tracking": self.sample_tracking,
                "sandbox_mapping": getattr(self, 'sandbox_id_mapping', {}),
                "processing_summary": {
                    "total_samples": len(selected_samples),
                    "processed_successfully": len(self.processed_samples),
                    "failed_samples": len(self.failed_samples),
                    "success_rate": len(self.processed_samples) / len(selected_samples) if selected_samples else 0,
                    "processed_samples_list": self.processed_samples,
                    "failed_samples_list": self.failed_samples
                },
                "error_tracking": {
                    "batch_errors": self.batch_errors,
                    "target_field_errors": self.target_field_errors,
                    "sample_errors": self.sample_errors,
                    "stage_errors": self.stage_errors
                }
            }

            # Save consolidated files
            with open(self.batch_dir / "workflow_summary.json", "w") as f:
                json.dump(workflow_summary, f, indent=2)

            with open(self.batch_dir / "sample_metadata.json", "w") as f:
                json.dump(sample_metadata, f, indent=2)

            logger.info("✅ Consolidated output files saved to workflow_summary.json and sample_metadata.json")

        except Exception as e:
            logger.error(f"❌ Failed to consolidate output files: {e}")
            print("❌ Full traceback:")
            import traceback
            print(traceback.format_exc())

    def copy_batch_targets_output(self, sandbox_id: str, batch_num: int) -> None:
        """
        Copy batch_targets_output.json from sandbox to batch_outputs subdirectory.

        Parameters
        ----------
        sandbox_id : str
            Sandbox session ID
        batch_num : int
            Batch number for labeling the file
        """
        try:
            # Use the batch directory instead of sandbox
            source_file = self.batch_dir / sandbox_id / "batch_targets_output.json"

            # Create batch_outputs subdirectory
            batch_outputs_dir = self.batch_dir / "batch_outputs"
            batch_outputs_dir.mkdir(exist_ok=True)

            # Create target file with batch number
            target_file = (
                batch_outputs_dir / f"batch_{batch_num:02d}_targets_output.json"
            )

            if not source_file.exists():
                logger.warning(f"No batch_targets_output.json found in {source_file.parent}")
                return

            # Copy the file
            import shutil

            shutil.copy2(source_file, target_file)

        except Exception as e:
            logger.error(f"Error copying batch_targets_output.json: {e}")
            print("❌ Full traceback:")
            import traceback
            print(traceback.format_exc())

    def copy_data_intake_output(self, sandbox_id: str, batch_num: int) -> None:
        """
        Copy data_intake_output.json from sandbox to batch_outputs subdirectory.

        Parameters
        ----------
        sandbox_id : str
            Sandbox session ID
        batch_num : int
            Batch number for labeling the file
        """
        try:
            # Use the batch directory instead of sandbox
            source_file = self.batch_dir / sandbox_id / "data_intake_output.json"

            # Create batch_outputs subdirectory
            batch_outputs_dir = self.batch_dir / "batch_outputs"
            batch_outputs_dir.mkdir(exist_ok=True)

            # Create target file with batch number
            target_file = (
                batch_outputs_dir / f"batch_{batch_num:02d}_data_intake_output.json"
            )

            if not source_file.exists():
                logger.warning(f"No data_intake_output.json found in {source_file.parent}")
                return

            # Copy the file
            import shutil

            shutil.copy2(source_file, target_file)


        except Exception as e:
            logger.error(f"Error copying data_intake_output.json: {e}")
            print("❌ Full traceback:")
            import traceback
            print(traceback.format_exc())

    def extract_sample_results(self, sandbox_id: str) -> Dict[str, Any]:
        """
        Extract comprehensive results from a sandbox batch_targets_output.json.

        Parameters
        ----------
        sandbox_id : str
            Sandbox session ID

        Returns
        -------
        Dict[str, Any]
            Extracted results by sample ID
        """
        try:
            # Use batch directory instead of sandbox
            results_file = self.batch_dir / sandbox_id / "batch_targets_output.json"

            if not results_file.exists():
                logger.warning(f"No batch_targets_output.json found in {results_file.parent}")
                return {}

            with open(results_file, "r") as f:
                batch_data = json.load(f)

            # Check for the new structure first (sample_results)
            if "sample_results" in batch_data:
                sample_results = batch_data["sample_results"]

                # Enhance with actual normalization data from individual output files
                self._enhance_with_normalization_data(results_file.parent, sample_results)

                return sample_results

            # Fallback to old structure
            logger.info(f"Using legacy batch_targets structure for {sandbox_id}")
            sample_results = {}

            # Get direct fields (organism, pubmed_id, platform_id, instrument)
            direct_fields = batch_data.get("direct_field_results", {})

            # Get curation results
            curation_results = batch_data.get("curation_results", {})

            # Get normalization results
            normalization_results = batch_data.get("normalization_results", {})

            # Combine all data by sample
            all_samples = set()
            all_samples.update(direct_fields.keys())
            for field_results in curation_results.values():
                all_samples.update(field_results.keys())
            for field_results in normalization_results.values():
                all_samples.update(field_results.keys())

            for sample_id in all_samples:
                sample_results[sample_id] = {
                    "direct_fields": direct_fields.get(sample_id, {}),
                    "curated_fields": {},
                    "normalized_fields": {},
                }

                # Extract curation results for each field
                for field, field_results in curation_results.items():
                    if sample_id in field_results:
                        sample_results[sample_id]["curated_fields"][field] = (
                            field_results[sample_id]
                        )

                # Extract normalization results for each field
                for field, field_results in normalization_results.items():
                    if sample_id in field_results:
                        sample_results[sample_id]["normalized_fields"][field] = (
                            field_results[sample_id]
                        )

            return sample_results

        except Exception as e:
            logger.error(f"Error extracting results from {sandbox_id}: {e}")
            print("❌ Full traceback:")
            import traceback
            print(traceback.format_exc())
            return {}

    def _enhance_with_normalization_data(
        self, sandbox_dir: Path, sample_results: Dict[str, Any]
    ) -> None:
        """
        Enhance sample results with actual normalization data from individual output files.

        Parameters
        ----------
        sandbox_dir : Path
            Path to sandbox directory
        sample_results : Dict[str, Any]
            Sample results to enhance (modified in place)
        """
        # Fields that have normalization
        normalization_fields = ["disease", "tissue", "organ"]

        for field in normalization_fields:
            field_dir = sandbox_dir / field
            normalizer_output_file = field_dir / "normalizer_output.json"

            if normalizer_output_file.exists():
                try:
                    with open(normalizer_output_file, "r") as f:
                        normalizer_data = json.load(f)

                    # Extract normalization results by sample
                    if "sample_results" in normalizer_data:
                        for sample_result in normalizer_data["sample_results"]:
                            sample_id = sample_result.get("sample_id")
                            result = sample_result.get("result", {})

                            if sample_id and sample_id in sample_results:
                                # Extract best normalized match from final_normalized_candidates
                                final_normalized_candidates = result.get(
                                    "final_normalized_candidates", []
                                )
                                if final_normalized_candidates:
                                    best_normalized = final_normalized_candidates[
                                        0
                                    ]  # First is best
                                    top_matches = best_normalized.get(
                                        "top_ontology_matches", []
                                    )

                                    if top_matches:
                                        best_match = top_matches[
                                            0
                                        ]  # First match is best

                                        # Update the normalized fields in sample_results
                                        if (
                                            "normalized_fields"
                                            not in sample_results[sample_id]
                                        ):
                                            sample_results[sample_id][
                                                "normalized_fields"
                                            ] = {}

                                        sample_results[sample_id]["normalized_fields"][
                                            field
                                        ] = {
                                            "normalized_term": best_match.get(
                                                "term", ""
                                            ),
                                            "term_id": best_match.get("term_id", ""),
                                            "ontology": best_match.get("ontology", ""),
                                            "confidence": best_match.get("score", 0.0),
                                            "original_value": best_normalized.get(
                                                "value", ""
                                            ),
                                        }

                                        logger.debug(
                                            f"Enhanced {sample_id} {field} with normalization: {best_match.get('term')} ({best_match.get('term_id')})"
                                        )

                except Exception as e:
                    logger.warning(f"Error reading normalization data for {field}: {e}")
                    continue

    def generate_comprehensive_csv(self) -> None:
        """
        Generate comprehensive parquet file with all curated and normalized data.
        """
        logger.info("Generating comprehensive results parquet file")

        parquet_file = self.batch_dir / "comprehensive_batch_results.parquet"

        # Define comprehensive CSV columns
        columns = [
            # Sample identification
            "sample_id",
            "sandbox_id",
            "batch_num",
            "processing_status",
            "processing_time",
            "error_message",
            # Direct fields
            "organism",
            "pubmed_id",
            "platform_id",
            "instrument",
            "series_id",
            # Disease (full pipeline: curation + normalization)
            "disease_final_candidate",
            "disease_confidence",
            "disease_context",
            "disease_rationale",
            "disease_prenormalized",
            "disease_normalized_term",
            "disease_normalized_id",
            "disease_ontology",
            # Tissue (full pipeline: curation + normalization)
            "tissue_final_candidate",
            "tissue_confidence",
            "tissue_context",
            "tissue_rationale",
            "tissue_prenormalized",
            "tissue_normalized_term",
            "tissue_normalized_id",
            "tissue_ontology",
            # Organ (full pipeline: curation + normalization)
            "organ_final_candidate",
            "organ_confidence",
            "organ_context",
            "organ_rationale",
            "organ_prenormalized",
            "organ_normalized_term",
            "organ_normalized_id",
            "organ_ontology",
            # Cell Line (curation only)
            "cell_line_final_candidate",
            "cell_line_confidence",
            "cell_line_context",
            "cell_line_rationale",
            "cell_line_prenormalized",
            # Developmental Stage (curation only)
            "developmental_stage_final_candidate",
            "developmental_stage_confidence",
            "developmental_stage_context",
            "developmental_stage_rationale",
            "developmental_stage_prenormalized",
            # Ethnicity (curation only)
            "ethnicity_final_candidate",
            "ethnicity_confidence",
            "ethnicity_context",
            "ethnicity_rationale",
            "ethnicity_prenormalized",
            # Gender (curation only)
            "gender_final_candidate",
            "gender_confidence",
            "gender_context",
            "gender_rationale",
            "gender_prenormalized",
            # Age (curation only)
            "age_final_candidate",
            "age_confidence",
            "age_context",
            "age_rationale",
            "age_prenormalized",
            # Treatment (curation only)
            "treatment_final_candidate",
            "treatment_confidence",
            "treatment_context",
            "treatment_rationale",
            "treatment_prenormalized",
            # Assay Type (full pipeline: curation + normalization)
            "assay_type_final_candidate",
            "assay_type_confidence",
            "assay_type_context",
            "assay_type_rationale",
            "assay_type_prenormalized",
            # Processing metadata
            "sources_processed",
            "reconciliation_needed",
            "reconciliation_reason",
            "total_candidates_found",
            "processing_timestamp",
        ]

        # Collect all data rows
        data_rows = []
        for sample_id, tracking_info in self.sample_tracking.items():
            row = self._create_csv_row(sample_id, tracking_info, columns)
            data_rows.append(row)

        # Convert to DataFrame and save as parquet
        df = pd.DataFrame(data_rows)
        
        # Ensure numeric columns have proper types before saving
        for col in df.columns:
            if col.endswith("_confidence") or col in ["processing_time", "total_candidates_found"]:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        # Log DataFrame info for debugging
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame dtypes: {df.dtypes.to_dict()}")
        
        # Additional validation for confidence fields
        confidence_columns = [col for col in df.columns if col.endswith("_confidence")]
        for col in confidence_columns:
            if col in df.columns:
                non_numeric_count = df[col].apply(lambda x: not isinstance(x, (int, float)) or pd.isna(x)).sum()
                if non_numeric_count > 0:
                    logger.warning(f"Column {col} has {non_numeric_count} non-numeric values, converting to numeric")
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        try:
            df.to_parquet(parquet_file, index=False)
        except Exception as e:
            logger.error(f"Failed to save parquet file: {e}")
            # Try to save as CSV as fallback
            csv_file = parquet_file.with_suffix('.csv')
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved as CSV instead: {csv_file}")
            raise

        logger.info(f"Generated comprehensive parquet file: {parquet_file}")

    def generate_streamlined_csv(self) -> None:
        """
        Generate streamlined parquet file with only key columns for analysis.
        """
        logger.info("Generating streamlined results parquet file")

        parquet_file = self.batch_dir / "batch_results.parquet"

        # Define streamlined CSV columns - only essential data
        columns = [
            # Core metadata
            "sample_id",
            "sample_type", 
            "batch_num",
            "sandbox_id",
            # Direct extraction fields
            "organism",
            "pubmed_id", 
            "platform_id",
            "instrument",
            "series_id",
            "treatment",
            # Target fields - final candidates only
            "disease_final_candidate",
            "tissue_final_candidate", 
            "organ_final_candidate",
            "cell_line_final_candidate",
            "developmental_stage_final_candidate",
            "ethnicity_final_candidate",
            "gender_final_candidate",
            "age_final_candidate",
            "assay_type_final_candidate",
            "treatment_final_candidate",
            # Normalized fields (only for fields that go through normalization)
            "disease_normalized_term",
            "disease_normalized_id", 
            "tissue_normalized_term",
            "tissue_normalized_id",
            "organ_normalized_term", 
            "organ_normalized_id",
        ]

        # Collect all data rows
        data_rows = []
        for sample_id, tracking_info in self.sample_tracking.items():
            row = self._create_streamlined_csv_row(sample_id, tracking_info, columns)
            data_rows.append(row)

        # Convert to DataFrame and save as parquet
        df = pd.DataFrame(data_rows)
        
        # Ensure numeric columns have proper types before saving (though streamlined CSV shouldn't have confidence fields)
        for col in df.columns:
            if col in ["processing_time", "total_candidates_found"]:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        # Log DataFrame info for debugging
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame dtypes: {df.dtypes.to_dict()}")
        
        try:
            df.to_parquet(parquet_file, index=False)
        except Exception as e:
            logger.error(f"Failed to save parquet file: {e}")
            # Try to save as CSV as fallback
            csv_file = parquet_file.with_suffix('.csv')
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved as CSV instead: {csv_file}")
            raise

        logger.info(f"Generated streamlined parquet file: {parquet_file}")

    def _create_streamlined_csv_row(
        self, sample_id: str, tracking_info: Dict[str, Any], columns: List[str]
    ) -> Dict[str, Any]:
        """
        Create a streamlined CSV row with only essential data.

        Parameters
        ----------
        sample_id : str
            GSM sample ID
        tracking_info : Dict[str, Any]
            Sample tracking information
        columns : List[str]
            CSV column names

        Returns
        -------
        Dict[str, Any]
            Streamlined CSV row data
        """
        # Initialize row with empty values
        row = {col: "" for col in columns}

        # Basic sample info
        row["sample_id"] = sample_id
        row["sandbox_id"] = tracking_info.get("sandbox_id", "")
        row["batch_num"] = tracking_info.get("batch_num", "")
        
        # Get sample type from tracking info or extract from results
        sample_type = tracking_info.get("sample_type", "")
        if not sample_type and tracking_info.get("status") == "completed":
            sandbox_id = tracking_info.get("sandbox_id")
            if sandbox_id and sandbox_id != "unknown":
                sample_results = self.extract_sample_results(sandbox_id)
                if sample_id in sample_results:
                    # Try to get sample_type from the results
                    sample_data = sample_results[sample_id]
                    curation_results = sample_data.get("curated_fields", {}) or sample_data.get("curation_results", {})
                    sample_type_result = curation_results.get("sample_type", {})
                    if sample_type_result and "best_candidate" in sample_type_result:
                        sample_type = sample_type_result["best_candidate"].get("value", "")
        row["sample_type"] = sample_type

        # Extract streamlined data if processing was successful
        if tracking_info.get("status") == "completed":
            sandbox_id = tracking_info.get("sandbox_id")
            if sandbox_id and sandbox_id != "unknown":
                sample_results = self.extract_sample_results(sandbox_id)
                if sample_id in sample_results:
                    self._populate_streamlined_row_data(row, sample_results[sample_id])

        return row

    def _populate_streamlined_row_data(
        self, row: Dict[str, Any], sample_data: Dict[str, Any]
    ) -> None:
        """
        Populate streamlined CSV row with only essential data.

        Parameters
        ----------
        row : Dict[str, Any]
            CSV row to populate
        sample_data : Dict[str, Any]
            Extracted sample data
        """
        # Direct fields - handle both old and new structure
        direct_fields = sample_data.get("direct_fields", {})

        # New structure has direct field names as keys
        row["organism"] = direct_fields.get("organism", {}).get(
            "value", ""
        ) or direct_fields.get("Organism", {}).get("value", "")
        row["pubmed_id"] = direct_fields.get("pubmed_id", {}).get(
            "value", ""
        ) or direct_fields.get("PubMed ID", {}).get("value", "")
        row["platform_id"] = direct_fields.get("platform_id", {}).get(
            "value", ""
        ) or direct_fields.get("Platform ID", {}).get("value", "")
        row["instrument"] = direct_fields.get("instrument_model", {}).get(
            "value", ""
        ) or direct_fields.get("Instrument", {}).get("value", "")
        row["series_id"] = direct_fields.get("series_id", {}).get(
            "value", ""
        ) or direct_fields.get("Series ID", {}).get("value", "")
        
        # Treatment is a direct field that gets curated
        row["treatment"] = direct_fields.get("treatment", {}).get(
            "value", ""
        ) or direct_fields.get("Treatment", {}).get("value", "")

        # Curated fields - only final candidates
        curation_results = sample_data.get("curated_fields", {}) or sample_data.get(
            "curation_results", {}
        )
        normalization_results = sample_data.get(
            "normalized_fields", {}
        ) or sample_data.get("normalization_results", {})

        # Process each target field for final candidates only
        target_fields = [
            "disease",
            "tissue", 
            "organ",
            "cell_line",
            "developmental_stage",
            "ethnicity",
            "gender",
            "age",
            "treatment",
            "assay_type",
        ]

        for field in target_fields:
            self._populate_streamlined_field_data(
                row, field, curation_results, normalization_results
            )

    def _populate_streamlined_field_data(
        self,
        row: Dict[str, Any],
        field: str,
        curation_results: Dict[str, Any],
        normalization_results: Dict[str, Any],
    ) -> None:
        """
        Populate streamlined CSV row data for a specific target field.

        Parameters
        ----------
        row : Dict[str, Any]
            CSV row to populate
        field : str
            Target field name
        curation_results : Dict[str, Any]
            Curation results
        normalization_results : Dict[str, Any]
            Normalization results
        """
        # Get curation data for this field - only final candidate
        field_curation = curation_results.get(field, {})
        best_candidate = field_curation.get("best_candidate")

        if best_candidate:
            row[f"{field}_final_candidate"] = best_candidate.get("value", "")

        # Get normalization data for this field (if available and if field goes through normalization)
        field_normalization = normalization_results.get(field, {})
        if field_normalization:
            # Only include normalized fields for fields that actually go through normalization
            # Based on TARGET_FIELD_CONFIG: disease, organ, tissue are normalized
            if field in ["disease", "organ", "tissue"]:
                row[f"{field}_normalized_term"] = field_normalization.get(
                    "normalized_term", ""
                ) or field_normalization.get("final_normalized_term", "")
                row[f"{field}_normalized_id"] = field_normalization.get(
                    "term_id", ""
                ) or field_normalization.get("final_normalized_id", "")

    def _create_csv_row(
        self, sample_id: str, tracking_info: Dict[str, Any], columns: List[str]
    ) -> Dict[str, Any]:
        """
        Create a CSV row for a single sample with comprehensive data.

        Parameters
        ----------
        sample_id : str
            GSM sample ID
        tracking_info : Dict[str, Any]
            Sample tracking information
        columns : List[str]
            CSV column names

        Returns
        -------
        Dict[str, Any]
            CSV row data
        """
        # Initialize row with appropriate default values
        row = {}
        for col in columns:
            if col.endswith("_confidence"):
                row[col] = 0.0  # Confidence fields should be numeric
            elif col in ["processing_time", "total_candidates_found"]:
                row[col] = 0.0  # Numeric fields
            else:
                row[col] = ""

        # Basic sample info
        row["sample_id"] = sample_id
        row["sandbox_id"] = tracking_info.get("sandbox_id", "")
        row["batch_num"] = tracking_info.get("batch_num", "")
        row["processing_status"] = tracking_info.get("status", "")
        # Handle processing_time as numeric
        processing_time = tracking_info.get("processing_time")
        if processing_time is None or processing_time == "":
            row["processing_time"] = 0.0
        else:
            try:
                row["processing_time"] = float(processing_time)
            except (ValueError, TypeError):
                row["processing_time"] = 0.0
        row["error_message"] = tracking_info.get("error", "")
        row["processing_timestamp"] = tracking_info.get("timestamp", "")

        # Extract comprehensive data if processing was successful
        if tracking_info.get("status") == "completed":
            sandbox_id = tracking_info.get("sandbox_id")
            if sandbox_id and sandbox_id != "unknown":
                sample_results = self.extract_sample_results(sandbox_id)
                if sample_id in sample_results:
                    self._populate_row_data(row, sample_results[sample_id])

        return row

    def _populate_row_data(
        self, row: Dict[str, Any], sample_data: Dict[str, Any]
    ) -> None:
        """
        Populate CSV row with extracted sample data.

        Parameters
        ----------
        row : Dict[str, Any]
            CSV row to populate
        sample_data : Dict[str, Any]
            Extracted sample data
        """
        # Direct fields - handle both old and new structure
        direct_fields = sample_data.get("direct_fields", {})

        # New structure has direct field names as keys
        row["organism"] = direct_fields.get("organism", {}).get(
            "value", ""
        ) or direct_fields.get("Organism", {}).get("value", "")
        row["pubmed_id"] = direct_fields.get("pubmed_id", {}).get(
            "value", ""
        ) or direct_fields.get("PubMed ID", {}).get("value", "")
        row["platform_id"] = direct_fields.get("platform_id", {}).get(
            "value", ""
        ) or direct_fields.get("Platform ID", {}).get("value", "")
        row["instrument"] = direct_fields.get("instrument_model", {}).get(
            "value", ""
        ) or direct_fields.get("Instrument", {}).get("value", "")
        row["series_id"] = direct_fields.get("series_id", {}).get(
            "value", ""
        ) or direct_fields.get("Series ID", {}).get("value", "")

        # Curated and normalized fields - handle both old and new structure
        curation_results = sample_data.get("curated_fields", {}) or sample_data.get(
            "curation_results", {}
        )
        normalization_results = sample_data.get(
            "normalized_fields", {}
        ) or sample_data.get("normalization_results", {})

        # Process each target field
        target_fields = [
            "disease",
            "tissue",
            "organ",
            "cell_line",
            "developmental_stage",
            "ethnicity",
            "gender",
            "age",
            "treatment",
            "assay_type",
        ]

        for field in target_fields:
            self._populate_field_data(
                row, field, curation_results, normalization_results
            )

    def _populate_field_data(
        self,
        row: Dict[str, Any],
        field: str,
        curation_results: Dict[str, Any],
        normalization_results: Dict[str, Any],
    ) -> None:
        """
        Populate CSV row data for a specific target field.

        Parameters
        ----------
        row : Dict[str, Any]
            CSV row to populate
        field : str
            Target field name
        curation_results : Dict[str, Any]
            Curation results
        normalization_results : Dict[str, Any]
            Normalization results
        """
        # Get curation data for this field
        field_curation = curation_results.get(field, {})
        best_candidate = field_curation.get("best_candidate")

        if best_candidate:
            row[f"{field}_final_candidate"] = best_candidate.get("value", "")
            # Handle confidence as numeric - convert empty/None to 0.0
            confidence = best_candidate.get("confidence")
            if confidence is None or confidence == "":
                row[f"{field}_confidence"] = 0.0
            else:
                try:
                    row[f"{field}_confidence"] = float(confidence)
                except (ValueError, TypeError):
                    row[f"{field}_confidence"] = 0.0
            row[f"{field}_context"] = best_candidate.get("context", "")
            row[f"{field}_rationale"] = best_candidate.get("rationale", "")
            row[f"{field}_prenormalized"] = best_candidate.get("prenormalized", "")

        # Add metadata
        row["sources_processed"] = ", ".join(
            field_curation.get("sources_processed", [])
        )
        row["reconciliation_needed"] = field_curation.get("reconciliation_needed", "")
        row["reconciliation_reason"] = field_curation.get("reconciliation_reason", "")
        # Handle candidate count as numeric
        candidate_count = field_curation.get("candidate_count")
        if candidate_count is None or candidate_count == "":
            row["total_candidates_found"] = 0
        else:
            try:
                row["total_candidates_found"] = int(candidate_count)
            except (ValueError, TypeError):
                row["total_candidates_found"] = 0

        # Get normalization data for this field (if available)
        field_normalization = normalization_results.get(field, {})
        if field_normalization:
            # Handle both old and new normalization result structures
            row[f"{field}_normalized_term"] = field_normalization.get(
                "normalized_term", ""
            ) or field_normalization.get("final_normalized_term", "")
            row[f"{field}_normalized_id"] = field_normalization.get(
                "term_id", ""
            ) or field_normalization.get("final_normalized_id", "")
            row[f"{field}_ontology"] = field_normalization.get(
                "ontology", ""
            ) or field_normalization.get("final_ontology", "")

    def save_tracking_data(self) -> None:
        """
        Prepare tracking data for consolidation (individual files no longer saved).
        """
        # Create processing summary for logging
        summary = {
            "total_samples": len(self.sample_tracking),
            "processed_successfully": len(self.processed_samples),
            "failed_samples": len(self.failed_samples),
            "success_rate": len(self.processed_samples) / len(self.sample_tracking)
            if self.sample_tracking
            else 0,
            "batch_size": self.batch_size,
            "total_batches": len(self.sample_tracking) // self.batch_size
            + (1 if len(self.sample_tracking) % self.batch_size > 0 else 0),
            "timestamp": datetime.now().isoformat(),
        }

        logger.info(f"Processing summary: {summary}")

    def track_batch_error(self, batch_num: int, error: str, samples: List[str], stage: str = "unknown") -> None:
        """
        Track an error that occurred during batch processing.
        
        Parameters
        ----------
        batch_num : int
            Batch number where error occurred
        error : str
            Error message
        samples : List[str]
            List of samples in the batch
        stage : str
            Processing stage where error occurred
        """
        if batch_num not in self.batch_errors:
            self.batch_errors[batch_num] = []
        
        error_info = {
            "error": error,
            "samples": samples,
            "stage": stage,
            "timestamp": datetime.now().isoformat()
        }
        self.batch_errors[batch_num].append(error_info)
        
        # Update error summary
        self.error_summary["failed_batches"] += 1
        self.error_summary["failed_samples"] += len(samples)
        
        # Track error type
        error_type = type(error).__name__ if hasattr(error, '__class__') else "Unknown"
        if error_type not in self.error_summary["error_types"]:
            self.error_summary["error_types"][error_type] = 0
        self.error_summary["error_types"][error_type] += 1
        
        # Track stage failures
        if stage not in self.error_summary["stage_failures"]:
            self.error_summary["stage_failures"][stage] = 0
        self.error_summary["stage_failures"][stage] += 1

    def track_target_field_error(self, target_field: str, error: str, samples: List[str], stage: str = "unknown") -> None:
        """
        Track an error that occurred during target field processing.
        
        Parameters
        ----------
        target_field : str
            Target field that failed
        error : str
            Error message
        samples : List[str]
            List of samples affected
        stage : str
            Processing stage where error occurred
        """
        if target_field not in self.target_field_errors:
            self.target_field_errors[target_field] = []
        
        error_info = {
            "error": error,
            "samples": samples,
            "stage": stage,
            "timestamp": datetime.now().isoformat()
        }
        self.target_field_errors[target_field].append(error_info)
        
        # Update error summary
        self.error_summary["failed_target_fields"] += 1

    def track_sample_error(self, sample_id: str, error: str, stage: str = "unknown") -> None:
        """
        Track an error that occurred for a specific sample.
        
        Parameters
        ----------
        sample_id : str
            Sample ID that failed
        error : str
            Error message
        stage : str
            Processing stage where error occurred
        """
        if sample_id not in self.sample_errors:
            self.sample_errors[sample_id] = []
        
        error_info = {
            "error": error,
            "stage": stage,
            "timestamp": datetime.now().isoformat()
        }
        self.sample_errors[sample_id].append(error_info)

    def track_stage_error(self, stage: str, error: str, affected_items: List[str]) -> None:
        """
        Track an error that occurred during a specific processing stage.
        
        Parameters
        ----------
        stage : str
            Processing stage that failed
        error : str
            Error message
        affected_items : List[str]
            List of items affected by the error
        """
        if stage not in self.stage_errors:
            self.stage_errors[stage] = []
        
        error_info = {
            "error": error,
            "affected_items": affected_items,
            "timestamp": datetime.now().isoformat()
        }
        self.stage_errors[stage].append(error_info)

    def track_curation_failure(self, sample_id: str, target_field: str, error: str, stage: str = "curation") -> None:
        """
        Track a curation failure for specific sample/target field combination.
        
        Parameters
        ----------
        sample_id : str
            Sample ID that failed
        target_field : str
            Target field that failed curation
        error : str
            Error message
        stage : str
            Processing stage where error occurred
        """
        if target_field not in self.failed_items["curation_failures"]:
            self.failed_items["curation_failures"][target_field] = {}
        
        self.failed_items["curation_failures"][target_field][sample_id] = {
            "error": error,
            "stage": stage,
            "timestamp": datetime.now().isoformat(),
            "retry_needed": True,
            "failure_type": "curation_error"
        }
        
        # Also track in target_fields for compatibility
        if target_field not in self.failed_items["target_fields"]:
            self.failed_items["target_fields"][target_field] = {}
        self.failed_items["target_fields"][target_field][sample_id] = self.failed_items["curation_failures"][target_field][sample_id]

    def track_normalization_failure(self, sample_id: str, target_field: str, error: str, stage: str = "normalization") -> None:
        """
        Track a normalization failure for specific sample/target field combination.
        
        Parameters
        ----------
        sample_id : str
            Sample ID that failed
        target_field : str
            Target field that failed normalization
        error : str
            Error message
        stage : str
            Processing stage where error occurred
        """
        if target_field not in self.failed_items["normalization_failures"]:
            self.failed_items["normalization_failures"][target_field] = {}
        
        self.failed_items["normalization_failures"][target_field][sample_id] = {
            "error": error,
            "stage": stage,
            "timestamp": datetime.now().isoformat(),
            "retry_needed": True,
            "failure_type": "normalization_error"
        }
        
        # Also track in target_fields for compatibility
        if target_field not in self.failed_items["target_fields"]:
            self.failed_items["target_fields"][target_field] = {}
        if sample_id not in self.failed_items["target_fields"][target_field]:
            self.failed_items["target_fields"][target_field][sample_id] = {}
        self.failed_items["target_fields"][target_field][sample_id].update({
            "normalization_error": error,
            "normalization_retry_needed": True
        })

    def track_missing_result(self, sample_id: str, target_field: str, result_type: str, reason: str = "no_results_found") -> None:
        """
        Track missing curation or normalization results.
        
        Parameters
        ----------
        sample_id : str
            Sample ID with missing results
        target_field : str
            Target field with missing results
        result_type : str
            Type of missing result ('curation' or 'normalization')
        reason : str
            Reason for missing results
        """
        if result_type not in self.failed_items["missing_results"]:
            self.failed_items["missing_results"][result_type] = {}
        if target_field not in self.failed_items["missing_results"][result_type]:
            self.failed_items["missing_results"][result_type][target_field] = {}
        
        self.failed_items["missing_results"][result_type][target_field][sample_id] = {
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
            "retry_needed": True,
            "failure_type": f"missing_{result_type}_result"
        }

    def track_sample_failure(self, sample_id: str, error: str, stage: str = "unknown", failure_type: str = "complete_failure") -> None:
        """
        Track complete sample failure.
        
        Parameters
        ----------
        sample_id : str
            Sample ID that failed completely
        error : str
            Error message
        stage : str
            Processing stage where error occurred
        failure_type : str
            Type of failure
        """
        self.failed_items["samples"][sample_id] = {
            "error": error,
            "stage": stage,
            "failure_type": failure_type,
            "timestamp": datetime.now().isoformat(),
            "retry_needed": True
        }

    def generate_error_summary(self) -> Dict[str, Any]:
        """
        Generate a comprehensive error summary.
        
        Returns
        -------
        Dict[str, Any]
            Complete error summary with statistics and details
        """
        # Update summary statistics
        self.error_summary["total_batches"] = len(self.batch_errors)
        self.error_summary["total_samples"] = len(self.sample_tracking)
        self.error_summary["total_target_fields"] = len(self.target_field_errors)
        
        # Calculate success rates
        if self.error_summary["total_batches"] > 0:
            self.error_summary["batch_success_rate"] = (
                (self.error_summary["total_batches"] - self.error_summary["failed_batches"]) 
                / self.error_summary["total_batches"]
            )
        
        if self.error_summary["total_samples"] > 0:
            self.error_summary["sample_success_rate"] = (
                (self.error_summary["total_samples"] - self.error_summary["failed_samples"]) 
                / self.error_summary["total_samples"]
            )
        
        # Add detailed error information
        self.error_summary["batch_errors"] = self.batch_errors
        self.error_summary["target_field_errors"] = self.target_field_errors
        self.error_summary["sample_errors"] = self.sample_errors
        self.error_summary["stage_errors"] = self.stage_errors
        
        return self.error_summary

    def save_error_summary(self) -> None:
        """
        Generate error summary for consolidation (individual files no longer saved).
        """
        self.generate_error_summary()  # Update internal error summary
        
        # Calculate total errors from all error types
        total_errors = (
            len(self.batch_errors) + 
            len(self.target_field_errors) + 
            len(self.sample_errors) + 
            len(self.stage_errors)
        )
        
        logger.info(f"Error summary generated with {total_errors} total errors across {len(self.batch_errors)} batches")

    def export_failed_items_json(self) -> str:
        """
        Export comprehensive failed items data as JSON for rerun capability.
        
        Returns
        -------
        str
            Path to the exported JSON file
        """
        # Prepare comprehensive error report
        error_report = {
            "batch_info": {
                "batch_dir": str(self.batch_dir),
                "timestamp": datetime.now().isoformat(),
                "original_config": self.batch_config,
                "total_samples": len(self.sample_tracking),
                "failed_samples_count": len(self.failed_samples),
                "processed_samples_count": len(self.processed_samples)
            },
            "failed_items": self.failed_items,
            "legacy_errors": {
                "batch_errors": self.batch_errors,
                "target_field_errors": self.target_field_errors,
                "sample_errors": self.sample_errors,
                "stage_errors": self.stage_errors
            },
            "error_summary": self.generate_error_summary(),
            "sample_tracking": self.sample_tracking,
            "retry_instructions": {
                "curation_retries": {},
                "normalization_retries": {},
                "complete_sample_retries": []
            }
        }
        
        # Generate retry instructions
        # Curation retries
        for target_field, failures in self.failed_items["curation_failures"].items():
            retry_samples = [sample_id for sample_id, details in failures.items() if details.get("retry_needed", True)]
            if retry_samples:
                error_report["retry_instructions"]["curation_retries"][target_field] = retry_samples
        
        # Normalization retries
        for target_field, failures in self.failed_items["normalization_failures"].items():
            retry_samples = [sample_id for sample_id, details in failures.items() if details.get("retry_needed", True)]
            if retry_samples:
                error_report["retry_instructions"]["normalization_retries"][target_field] = retry_samples
        
        # Missing results retries
        for result_type, type_failures in self.failed_items["missing_results"].items():
            retry_key = f"{result_type}_retries"
            if retry_key not in error_report["retry_instructions"]:
                error_report["retry_instructions"][retry_key] = {}
            
            for target_field, field_failures in type_failures.items():
                retry_samples = [sample_id for sample_id, details in field_failures.items() if details.get("retry_needed", True)]
                if retry_samples:
                    if target_field not in error_report["retry_instructions"][retry_key]:
                        error_report["retry_instructions"][retry_key][target_field] = []
                    error_report["retry_instructions"][retry_key][target_field].extend(retry_samples)
        
        # Complete sample retries
        for sample_id, details in self.failed_items["samples"].items():
            if details.get("retry_needed", True):
                error_report["retry_instructions"]["complete_sample_retries"].append(sample_id)
        
        # Save JSON file
        json_path = self.batch_dir / "failed_items_report.json"
        with open(json_path, 'w') as f:
            json.dump(error_report, f, indent=2, default=str)
        
        logger.info(f"🔄 Failed items report exported to: {json_path}")
        return str(json_path)

    def generate_sample_type_csvs(self, sample_type_mapping: Dict[str, str]) -> None:
        """
        Generate separate CSV files for each sample type.
        
        Parameters
        ----------
        sample_type_mapping : Dict[str, str]
            Dictionary mapping sample_id -> sample_type
        """
        logger.info("📊 Generating sample type-specific CSV files...")
        
        # Group samples by type
        sample_type_groups = {}
        for sample_id, sample_type in sample_type_mapping.items():
            if sample_type not in sample_type_groups:
                sample_type_groups[sample_type] = []
            sample_type_groups[sample_type].append(sample_id)
        
        # Generate CSV for each sample type
        for sample_type, sample_ids in sample_type_groups.items():
            if not sample_ids:
                continue
                
            parquet_filename = f"comprehensive_batch_results_{sample_type}.parquet"
            parquet_path = self.batch_dir / parquet_filename
            
            # Filter data for this sample type
            type_data = []
            for sample_id in sample_ids:
                if sample_id in self.sample_tracking:
                    tracking_info = self.sample_tracking[sample_id]
                    # Get column names for CSV (basic set for now)
                    columns = ["sample_id", "sample_type", "batch_num", "sandbox_id", "status", "processing_time", "timestamp"]
                    row_data = self._create_csv_row(sample_id, tracking_info, columns)
                    if row_data:
                        type_data.append(row_data)
            
            if type_data:
                df = pd.DataFrame(type_data)
                df.to_parquet(parquet_path, index=False)
                logger.info(f"   ✅ Created {parquet_filename} with {len(type_data)} samples")
            else:
                logger.warning(f"   ⚠️  No data available for {sample_type} samples")

    def save_sample_type_summary(self, sample_type_mapping: Dict[str, str], all_results: List[Dict[str, Any]]) -> None:
        """
        Save a comprehensive summary of sample type processing results.
        
        Parameters
        ----------
        sample_type_mapping : Dict[str, str]
            Dictionary mapping sample_id -> sample_type
        all_results : List[Dict[str, Any]]
            List of all batch processing results
        """
        logger.info("📄 Saving sample type processing summary...")
        
        # Calculate sample type statistics
        sample_type_stats = {}
        for sample_type in ["primary_sample", "cell_line", "unknown", "failed"]:
            type_samples = [sid for sid, stype in sample_type_mapping.items() if stype == sample_type]
            processed_samples = [sid for sid in type_samples if sid in self.processed_samples]
            failed_samples = [sid for sid in type_samples if sid in self.failed_samples]
            
            sample_type_stats[sample_type] = {
                "total_samples": len(type_samples),
                "processed_samples": len(processed_samples),
                "failed_samples": len(failed_samples),
                "success_rate": len(processed_samples) / len(type_samples) if type_samples else 0,
                "sample_list": type_samples,
                "processed_list": processed_samples,
                "failed_list": failed_samples
            }
        
        # Calculate batch statistics
        batch_stats = {}
        for result in all_results:
            sample_type = result.get("sample_type", "unknown")
            if sample_type not in batch_stats:
                batch_stats[sample_type] = {
                    "total_batches": 0,
                    "successful_batches": 0,
                    "failed_batches": 0,
                    "total_processing_time": 0.0,
                    "average_processing_time": 0.0
                }
            
            batch_stats[sample_type]["total_batches"] += 1
            if result.get("success", False):
                batch_stats[sample_type]["successful_batches"] += 1
            else:
                batch_stats[sample_type]["failed_batches"] += 1
            
            batch_stats[sample_type]["total_processing_time"] += result.get("processing_time", 0.0)
        
        # Calculate averages
        for sample_type, stats in batch_stats.items():
            if stats["total_batches"] > 0:
                stats["average_processing_time"] = stats["total_processing_time"] / stats["total_batches"]
        
        # Create comprehensive summary
        summary = {
            "workflow_type": "sample_type_based_batch_processing",
            "timestamp": datetime.now().isoformat(),
            "sample_type_statistics": sample_type_stats,
            "batch_statistics": batch_stats,
            "overall_statistics": {
                "total_samples_requested": len(sample_type_mapping),
                "total_samples_processed": len(self.processed_samples),
                "total_samples_failed": len(self.failed_samples),
                "overall_success_rate": len(self.processed_samples) / len(sample_type_mapping) if sample_type_mapping else 0,
                "total_batches_processed": len(all_results),
                "successful_batches": sum(1 for r in all_results if r.get("success", False)),
                "failed_batches": sum(1 for r in all_results if not r.get("success", False))
            },
            "configuration": {
                "batch_size": self.batch_size,
                "discovery_batch_size": 5,  # As used in the workflow
                "sample_count": len(sample_type_mapping),
                "model_provider": str(self.model_provider)
            }
        }
        
        # Save summary
        summary_path = self.batch_dir / "sample_type_processing_summary.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"✅ Sample type summary saved to {summary_path}")

    async def run(self) -> None:
        """
        Run the complete sample type-based batch processing workflow.
        
        New workflow:
        1. Load and select samples
        2. Discover sample types (initial processing in small batches)
        3. Create sample type-consistent batches
        4. Process batches conditionally based on sample type
        5. Generate sample type-specific and unified outputs
        """
        start_time = time.time()
        logger.info("🚀 Starting sample type-based batch processing workflow")
        
        # Log configuration
        if self.sample_type_filter:
            logger.info(f"🎯 Sample type filter: {self.sample_type_filter}")
        else:
            logger.info("🎯 Processing all sample types")

        try:
            # ================================================================
            # PHASE 1: SAMPLE SELECTION
            # ================================================================
            all_samples = self.load_age_samples()
            selected_samples = self.select_random_samples(all_samples)

            # ================================================================
            # PHASE 2: SAMPLE TYPE DISCOVERY
            # ================================================================
            sample_type_mapping = await self.discover_sample_types(selected_samples, discovery_batch_size=5)
            
            # Log discovery results
            discovery_stats = {}
            for sample_type in sample_type_mapping.values():
                discovery_stats[sample_type] = discovery_stats.get(sample_type, 0) + 1

            # Apply sample type filtering if specified
            if self.sample_type_filter:
                logger.info(f"🔍 Filtering samples to only process: {self.sample_type_filter}")
                # Filter sample_type_mapping to only include samples of the specified type
                filtered_mapping = {}
                for sample_id, sample_type in sample_type_mapping.items():
                    if sample_type == self.sample_type_filter:
                        filtered_mapping[sample_id] = sample_type
                
                # Update selected_samples to only include filtered samples
                selected_samples = [sample_id for sample_id in selected_samples if sample_id in filtered_mapping]
                sample_type_mapping = filtered_mapping
                
                logger.info(f"📊 After filtering: {len(selected_samples)} samples of type '{self.sample_type_filter}'")
                
                # Update discovery stats to reflect filtering
                discovery_stats = {self.sample_type_filter: len(selected_samples)}

            # ================================================================
            # PHASE 3: SAMPLE TYPE-CONSISTENT BATCHING
            # ================================================================
            sample_type_batches = self.create_sample_type_batches(selected_samples, sample_type_mapping)
            
            # Calculate total batches
            total_batch_count = sum(len(batches) for batches in sample_type_batches.values())

            # ================================================================
            # PHASE 4: CONDITIONAL BATCH PROCESSING
            # ================================================================
            
            all_results = []
            processed_batch_count = 0
            
            with tqdm(total=total_batch_count, desc="Processing sample type batches", unit="batch") as pbar:
                
                for sample_type, type_batches in sample_type_batches.items():
                    if not type_batches:
                        continue
                        
                    logger.info(f"\n🔄 Processing {sample_type} samples: {len(type_batches)} batches")
                    
                    for batch_num, batch_samples in enumerate(type_batches, 1):                        
                        batch_result = await self.process_sample_type_batch(
                            batch_samples=batch_samples,
                            sample_type=sample_type,
                            batch_num=batch_num,
                            total_batches=len(type_batches),
                            cached_initial_results=self.cached_initial_results
                        )
                        
                        all_results.append(batch_result)
                        processed_batch_count += 1

                        # Extract raw metadata and copy outputs for successful batches
                        if batch_result.get("success", False):
                            sandbox_id = batch_result.get("session_id", "unknown")
                            for sample_id in batch_samples:
                                self.extract_raw_metadata(sample_id, sandbox_id)

                                # Copy outputs with sample type prefix
                                self.copy_batch_targets_output(sandbox_id, batch_num)
                                self.copy_data_intake_output(sandbox_id, batch_num)

                        pbar.update(1)
                        pbar.set_postfix({
                            "Sample Type": sample_type,
                            "Batch": f"{batch_num}/{len(type_batches)}",
                            "Total Progress": f"{processed_batch_count}/{total_batch_count}"
                        })

                        # Save progress periodically
                        if processed_batch_count % 3 == 0:  # More frequent saves for smaller batches
                            self.save_tracking_data()

            # ================================================================
            # PHASE 5: OUTPUT GENERATION
            # ================================================================
            logger.info("📊 PHASE 5: OUTPUT GENERATION")
            
            # Generate sample type-specific CSVs
            self.generate_sample_type_csvs(sample_type_mapping)
            
            # Generate streamlined CSV (main output)
            self.generate_streamlined_csv()
            
            # Generate comprehensive CSV (detailed output)
            self.generate_comprehensive_csv()
            
            # Save consolidated output files (replaces individual JSON files)
            self.save_tracking_data()  # For logging only
            self.save_error_summary()  # For logging only
            self.consolidate_output_files(selected_samples, sample_type_batches, all_results, start_time)
            
            # Export failed items report for rerun capability
            try:
                failed_items_json_path = self.export_failed_items_json()
                logger.info(f"🔄 Failed items report saved: {failed_items_json_path}")
            except Exception as e:
                logger.error(f"⚠️  Failed to export failed items JSON: {e}")
                print("❌ Full traceback:")
                import traceback
                print(traceback.format_exc())

            end_time = time.time()
            total_time = end_time - start_time

            logger.info(f"✅ Sample type-based batch processing completed in {total_time:.2f} seconds")
            logger.info(f"📈 Successfully processed: {len(self.processed_samples)} samples")
            logger.info(f"❌ Failed samples: {len(self.failed_samples)}")
            logger.info(f"📂 Output directory: {self.batch_dir}")
            
            # Log final sample type distribution
            final_stats = {}
            for sample_id in self.processed_samples:
                sample_type = sample_type_mapping.get(sample_id, "unknown")
                final_stats[sample_type] = final_stats.get(sample_type, 0) + 1
            logger.info(f"📊 Final processed sample distribution: {final_stats}")

        except Exception as e:
            logger.error(f"❌ Error in sample type-based batch processing workflow: {e}")
            print("❌ Full traceback:")
            import traceback
            print(traceback.format_exc())
            # Track workflow-level error
            self.track_stage_error(
                stage="sample_type_batch_workflow",
                error=str(e),
                affected_items=[f"sample_type_batch_{i+1}" for i in range(total_batch_count if 'total_batch_count' in locals() else 0)]
            )
            self.save_error_summary()  # Save error summary even on failure
            raise


async def run_batch_samples_workflow(
    sample_count: int = 100,
    batch_size: int = 5,
    output_dir: str = "batch",
    age_file: str = "Age.txt",
    model_provider: ModelProvider = None,
    max_tokens: int = None,
    target_fields: list = None,
    sample_type_filter: str = None,
) -> str:
    """
    Run the batch samples workflow.

    Parameters
    ----------
    sample_count : int
        Number of samples to process
    batch_size : int
        Number of samples per batch
    output_dir : str
        Output directory for results
    age_file : str
        Path to Age.txt file
    model_provider : ModelProvider, optional
        Model provider for LLM requests
    max_tokens : int, optional
        Maximum tokens for LLM responses
            target_fields : list, optional
            List of target fields to process. If None, processes all available fields.
            Available fields: disease, tissue, organ, cell_line, developmental_stage,
            ethnicity, gender, age, organism, pubmed_id, platform_id, instrument
        sample_type_filter : str, optional
            Filter to process only specific sample type. If None, processes all sample types.
            Available types: primary_sample, cell_line, unknown

        Returns
        -------
        str
            Path to output directory
    """
    processor = BatchSamplesProcessor(
        output_dir=output_dir,
        sample_count=sample_count,
        batch_size=batch_size,
        age_file=age_file,
        model_provider=model_provider,
        max_tokens=max_tokens,
        target_fields=target_fields,
        sample_type_filter=sample_type_filter,
    )

    await processor.run()
    return str(processor.batch_dir)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process batch GEO samples")
    parser.add_argument(
        "--sample-count", type=int, default=100, help="Number of samples to process"
    )
    parser.add_argument(
        "--batch-size", type=int, default=5, help="Number of samples per batch"
    )
    parser.add_argument(
        "--output-dir", type=str, default="batch", help="Output directory"
    )
    parser.add_argument(
        "--age-file", type=str, default="Age.txt", help="Path to Age.txt file"
    )

    args = parser.parse_args()

    asyncio.run(
        run_batch_samples_workflow(
            sample_count=args.sample_count,
            batch_size=args.batch_size,
            output_dir=args.output_dir,
            age_file=args.age_file,
        )
    )
