"""
Efficient batch samples workflow using the new three-stage architecture.

This is a refactored version of the batch_samples workflow that uses the new
three-stage architecture:

1. Data Intake - Use the data_intake_sql workflow to take in the raw metadata
2. Preprocessing - Perform sample_type curation and split into batches by sample type
3. Conditional Processing - Perform conditional curation and normalization

This workflow orchestrates the three stages and handles output file generation,
maintaining backward compatibility with the original batch_samples interface.

Key improvements:
- Cleaner separation of concerns
- Better error handling and auditability
- Consistent file-based data passing between stages
- Optimized model usage (Gemini Flash for sample types and normalization, Gemini Pro for curation)
"""

import asyncio
import json
import pandas as pd
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging
from tqdm import tqdm  # noqa: F401 (not used directly in this module)

from dotenv import load_dotenv
from agents import ModelProvider

# Import the three workflows
from src.workflows.data_intake_sql import run_data_intake_sql_workflow
from src.workflows.preprocessing import run_preprocessing_workflow
from src.workflows.conditional_processing import run_conditional_processing_workflow
from src.workflows.eval_conditional import run_eval_conditional

from src.models import LinkerOutput
# from src.models.common import KeyValue  # Unused
from src.tools.batch_processing_tools import extract_direct_fields_from_data_intake

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Suppress HTTP request logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


class EfficientBatchSamplesProcessor:
    """
    Efficient batch samples processor using the new three-stage architecture.
    
    This processor orchestrates the data intake, preprocessing, and conditional
    processing workflows to provide the same functionality as the original
    batch_samples workflow with improved modularity and maintainability.
    """

    def __init__(
        self,
        output_dir: str = "batch",
        sample_count: int = 100,
        batch_size: int = 5,
        samples_file: str = "archs4_samples/archs4_gsm_ids.txt",
        model_provider: ModelProvider = None,
        max_tokens: int = None,
        target_fields: list = None,
        sample_type_filter: str = None,
        batch_name: str = None,
        output_format: str = "parquet",
        max_workers: Optional[int] = None,
        enable_profiling: bool = False,
    ):
        """
        Initialize the efficient batch samples processor.

        Parameters
        ----------
        output_dir : str
            Directory to save batch results
        sample_count : int
            Number of samples to process (default: 100)
        batch_size : int
            Number of samples per batch (default: 5)
        samples_file : str
            Path to archs4_gsm_ids.txt file containing GSM IDs
        model_provider : ModelProvider, optional
            Base model provider for LLM requests
        max_tokens : int, optional
            Maximum tokens for LLM responses
        target_fields : list, optional
            List of target fields to process. If None, processes all available fields.
        sample_type_filter : str, optional
            Filter to process only specific sample type.
        batch_name : str, optional
            Custom name for the batch directory.
        output_format : str, optional
            Output format for batch results. Options: 'parquet' (default) or 'csv'.
        """
        self.output_dir = Path(output_dir)
        self.sample_count = sample_count
        self.batch_size = batch_size
        self.samples_file = samples_file
        self.base_model_provider = model_provider
        self.max_tokens = max_tokens
        self.target_fields = target_fields or [
            "disease", "tissue", "organ", "cell_line", "cell_type", "developmental_stage",
            "ethnicity", "gender", "age", "organism", "pubmed_id", "platform_id", "instrument"
        ]
        self.sample_type_filter = sample_type_filter
        self.batch_name = batch_name
        self.output_format = output_format
        self.max_workers = max_workers
        self.enable_profiling = enable_profiling
        # Toggle for eval conditional workflow
        self.conditional_mode = "eval"  # values: "classic" or "eval" - eval is default for quality
        # Default max iterations for arbitrator evaluation cycles
        self.max_iterations = 2  # Default: 2 iterations for quality vs speed balance
        
        # Validate parameters
        self._validate_parameters()
        
        # Create timestamped output directory
        self._create_output_directory()
        
        # Initialize tracking data structures
        self.sample_tracking = {}
        self.processed_samples = []
        self.failed_samples = []
        
        # Store configuration for auditability
        self.batch_config = {
            "sample_count": sample_count,
            "batch_size": batch_size,
            "target_fields": target_fields or "all",
            "sample_type_filter": sample_type_filter or "all",
            "model_provider": str(model_provider) if model_provider else None,
            "max_tokens": max_tokens,
            "samples_file": samples_file,
            "output_format": output_format,
            "batch_name": batch_name,
            "max_workers": max_workers,
            "enable_profiling": enable_profiling,
        }

        # Set up logging to file
        self._setup_logging()

    def _validate_parameters(self):
        """Validate initialization parameters."""
        if self.output_format not in ["parquet", "csv"]:
            raise ValueError(f"Invalid output_format: {self.output_format}. Must be 'parquet' or 'csv'")
        
        if self.batch_name:
            import re
            invalid_chars = r'[<>:"/\\|?*]'
            if re.search(invalid_chars, self.batch_name):
                raise ValueError(f"Invalid batch_name: {self.batch_name}. Cannot contain characters: < > : \" / \\ | ? *")
            
            reserved_names = ['CON', 'PRN', 'AUX', 'NUL'] + [f'COM{i}' for i in range(1, 10)] + [f'LPT{i}' for i in range(1, 10)]
            if self.batch_name.upper() in reserved_names:
                raise ValueError(f"Invalid batch_name: {self.batch_name}. Cannot use reserved Windows names")
        
        if self.sample_type_filter and self.sample_type_filter not in ["primary_sample", "cell_line", "unknown"]:
            raise ValueError(f"Invalid sample_type_filter: {self.sample_type_filter}. Must be one of: primary_sample, cell_line, unknown")

    def _create_output_directory(self):
        """Create the output directory structure."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if self.batch_name:
            self.batch_dir = self.output_dir / f"batch_{self.batch_name}_{timestamp}"
            logger.info(f"🎯 Using custom batch name: batch_{self.batch_name}_{timestamp}")
        else:
            self.batch_dir = self.output_dir / f"batch_{timestamp}"
        
        self.batch_dir.mkdir(parents=True, exist_ok=True)
        
        # Discovery directory no longer used

    def _setup_logging(self):
        """Set up file logging for the batch process."""
        log_file = self.batch_dir / "processing_log.txt"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    def _create_sample_type_model_providers(self):
        """Create specialized model providers for different operations."""
        # Use Gemini Flash for sample type curation (faster, cost-effective)
        sample_type_provider = None
        if self.base_model_provider:
            try:
                sample_type_provider = type(self.base_model_provider)(default_model="google/gemini-2.5-flash")
            except Exception:
                sample_type_provider = self.base_model_provider
        
        # Use Gemini Pro for conditional curation (higher quality)
        curation_provider = None
        if self.base_model_provider:
            try:
                curation_provider = type(self.base_model_provider)(default_model="openai/gpt-5")
            except Exception:
                curation_provider = self.base_model_provider
        
        # Use Gemini Flash for normalization (faster, cost-effective)
        normalization_provider = None
        if self.base_model_provider:
            try:
                normalization_provider = type(self.base_model_provider)(default_model="google/gemini-2.5-flash")
            except Exception:
                normalization_provider = self.base_model_provider
        
        return sample_type_provider, curation_provider, normalization_provider

    def load_samples(self) -> List[str]:
        """
        Load and randomly select samples from the age file.
        
        Returns
        -------
        List[str]
            List of selected GSM sample IDs
        """
        logger.info(f"📂 Loading samples from {self.samples_file}")
        
        try:
            with open(self.samples_file, "r") as f:
                all_samples = [line.strip() for line in f if line.strip() and line.strip().startswith("GSM")]
            
            if len(all_samples) < self.sample_count:
                logger.warning(f"⚠️ Only {len(all_samples)} samples available, requesting {self.sample_count}")
                selected_samples = all_samples
            else:
                # Randomly select samples
                random.seed(42)  # For reproducibility
                selected_samples = random.sample(all_samples, self.sample_count)
            
            logger.info(f"✅ Selected {len(selected_samples)} samples for processing")
            return selected_samples
            
        except FileNotFoundError:
            logger.error(f"❌ Samples file not found: {self.samples_file}")
            raise
        except Exception as e:
            logger.error(f"❌ Error loading samples: {e}")
            raise

    async def run_data_intake_stage(self, samples: List[str]) -> LinkerOutput:
        """
        Run the data intake stage using the data_intake_sql workflow.
        
        Parameters
        ----------
        samples : List[str]
            List of sample IDs to process
            
        Returns
        -------
        LinkerOutput
            Data intake workflow output
        """
        logger.info(f"🚀 Stage 1: Running data intake for {len(samples)} samples")
        stage_start_time = time.time()
        
        try:
            # Create sample input string
            sample_input = ", ".join(samples)
            
            # Run data intake SQL workflow
            data_intake_result = run_data_intake_sql_workflow(
                input_text=f"Extract metadata for {sample_input}",
                session_id="discovery",
                sandbox_dir=str(self.batch_dir),
                workflow_type="complete",
                create_series_directories=True,
                enable_profiling=self.enable_profiling,
                max_workers=self.max_workers,
            )
            
            if not data_intake_result.success:
                raise RuntimeError(f"Data intake failed: {data_intake_result.message}")
            
            stage_duration = time.time() - stage_start_time
            logger.info(f"✅ Stage 1 completed in {stage_duration:.2f} seconds")
            
            # Save data intake output under data_intake/
            data_intake_dir = self.batch_dir / "data_intake"
            data_intake_dir.mkdir(exist_ok=True)
            data_intake_output_file = data_intake_dir / "data_intake_stage_output.json"
            with open(data_intake_output_file, "w") as f:
                json.dump(data_intake_result.model_dump(), f, indent=2)
            
            # Also save with the expected name for direct fields extraction
            data_intake_output_main = self.batch_dir / "data_intake_output.json"
            with open(data_intake_output_main, "w") as f:
                json.dump(data_intake_result.model_dump(), f, indent=2)
            
            return data_intake_result
            
        except Exception as e:
            logger.error(f"❌ Stage 1 (Data Intake) failed: {str(e)}")
            raise

    async def run_preprocessing_stage(self, data_intake_output: LinkerOutput, samples: List[str]) -> Dict[str, Any]:
        """
        Run the preprocessing stage using the preprocessing workflow.
        
        Parameters
        ----------
        data_intake_output : LinkerOutput
            Output from the data intake stage
        samples : List[str]
            List of sample IDs to process
            
        Returns
        -------
        Dict[str, Any]
            Preprocessing workflow output
        """
        logger.info("🚀 Stage 2: Running preprocessing (sample type curation and batching)")
        stage_start_time = time.time()
        
        try:
            # Create model provider for sample type curation (Gemini Flash)
            sample_type_provider, _, _ = self._create_sample_type_model_providers()
            
            # Run preprocessing workflow
            preprocessing_result = await run_preprocessing_workflow(
                data_intake_output=data_intake_output,
                samples=samples,
                session_directory=str(self.batch_dir),
                batch_size=self.batch_size,
                sample_type_filter=self.sample_type_filter,
                model_provider=sample_type_provider,
                max_tokens=self.max_tokens,
                max_workers=self.max_workers,
            )
            
            if not preprocessing_result["success"]:
                raise RuntimeError(f"Preprocessing failed: {preprocessing_result['message']}")
            
            stage_duration = time.time() - stage_start_time
            logger.info(f"✅ Stage 2 completed in {stage_duration:.2f} seconds")
            
            # Save preprocessing output under preprocessing/
            preprocessing_dir = self.batch_dir / "preprocessing"
            preprocessing_dir.mkdir(exist_ok=True)
            preprocessing_output_file = preprocessing_dir / "preprocessing_output.json"
            with open(preprocessing_output_file, "w") as f:
                json.dump(preprocessing_result, f, indent=2)
            
            return preprocessing_result
            
        except Exception as e:
            logger.error(f"❌ Stage 2 (Preprocessing) failed: {str(e)}")
            raise

    async def run_conditional_processing_stage(
        self, 
        preprocessing_output: Dict[str, Any], 
        data_intake_output: LinkerOutput,
        arbitrator_test_mode: bool = False
    ) -> Dict[str, Any]:
        """
        Run the conditional processing stage using the conditional processing workflow.
        
        Parameters
        ----------
        preprocessing_output : Dict[str, Any]
            Output from the preprocessing stage
        data_intake_output : LinkerOutput
            Output from the data intake stage
            
        Returns
        -------
        Dict[str, Any]
            Conditional processing workflow output
        """
        logger.info("🚀 Stage 3: Running conditional processing (curation and normalization)")
        stage_start_time = time.time()
        
        try:
            # Extract sample type batches
            sample_type_batches = preprocessing_output["sample_type_batches"]
            
            # Create model provider for conditional processing (Gemini Pro/Flash hybrid)
            _, curation_provider, _ = self._create_sample_type_model_providers()
            
            # Run conditional processing workflow (classic or eval)
            logger.info(f"🔧 Using conditional processing mode: {self.conditional_mode}")
            if self.conditional_mode == "eval":
                # Pass batch structure to eval workflow - DON'T flatten, let eval workflow handle batch sizing
                conditional_result = await run_eval_conditional(
                    session_directory=str(self.batch_dir),
                    sample_type_batches=sample_type_batches,  # Pass structured batches, not flattened
                    target_fields=self.target_fields,
                    model_provider=curation_provider,
                    max_tokens=self.max_tokens,
                    max_workers=self.max_workers,
                    max_iterations=self.max_iterations,
                    data_intake_output=data_intake_output,
                    arbitrator_test_mode=arbitrator_test_mode,
                    incremental_csv_callback=lambda batch_result: self.append_batch_to_csv(batch_result, data_intake_output),
                )
            else:
                # 🚀 ADVANCED SAMPLE TYPE PARALLELIZATION: Process sample types concurrently
                if len(sample_type_batches) > 1:
                    logger.info(f"🔧 Processing {len(sample_type_batches)} sample types with advanced parallelization")
                    
                    # Process each sample type concurrently when they don't interdepend
                    async def process_sample_type_concurrent(sample_type, batches):
                        """Process a single sample type with all its batches."""
                        logger.info(f"🔧 Starting concurrent processing for sample type: {sample_type}")
                        
                        # Create isolated sample type batches structure
                        isolated_batches = {sample_type: batches}
                        
                        result = await run_conditional_processing_workflow(
                            sample_type_batches=isolated_batches,
                            data_intake_output=data_intake_output,
                            session_directory=str(self.batch_dir),
                            target_fields=self.target_fields,
                            model_provider=curation_provider,
                            max_tokens=self.max_tokens,
                            max_workers=max(1, self.max_workers // len(sample_type_batches)) if self.max_workers else None,
                        )
                        
                        logger.info(f"🔧 Completed concurrent processing for sample type: {sample_type}")
                        return sample_type, result
                    
                    # Run all sample types concurrently
                    sample_type_tasks = [
                        process_sample_type_concurrent(st, batches) 
                        for st, batches in sample_type_batches.items()
                        if batches  # Only process non-empty sample types
                    ]
                    
                    if sample_type_tasks:
                        sample_type_results = await asyncio.gather(*sample_type_tasks)
                        
                        # Merge results from all sample types
                        merged_batch_results = []
                        merged_statistics = {
                            "total_batches": 0,
                            "successful_batches": 0,
                            "failed_batches": 0,
                            "total_samples": 0,
                            "successful_samples": 0
                        }
                        
                        for sample_type, result in sample_type_results:
                            if result.get("success"):
                                merged_batch_results.extend(result.get("batch_results", []))
                                stats = result.get("statistics", {})
                                merged_statistics["total_batches"] += stats.get("total_batches", 0)
                                merged_statistics["successful_batches"] += stats.get("successful_batches", 0)
                                merged_statistics["failed_batches"] += stats.get("failed_batches", 0)
                                merged_statistics["total_samples"] += stats.get("total_samples", 0)
                                merged_statistics["successful_samples"] += stats.get("successful_samples", 0)
                        
                        conditional_result = {
                            "success": True,
                            "message": f"Concurrent sample type processing completed for {len(sample_type_results)} types",
                            "execution_time_seconds": 0,  # Will be calculated by stage timing
                            "batch_results": merged_batch_results,
                            "statistics": merged_statistics
                        }
                        
                        logger.info(f"🔧 Merged results from {len(sample_type_results)} concurrent sample type processes")
                    else:
                        # Fallback to standard processing
                        conditional_result = await run_conditional_processing_workflow(
                            sample_type_batches=sample_type_batches,
                            data_intake_output=data_intake_output,
                            session_directory=str(self.batch_dir),
                            target_fields=self.target_fields,
                            model_provider=curation_provider,
                            max_tokens=self.max_tokens,
                            max_workers=self.max_workers,
                        )
                        
                else:
                    # Standard processing for single sample type
                    conditional_result = await run_conditional_processing_workflow(
                        sample_type_batches=sample_type_batches,
                        data_intake_output=data_intake_output,
                        session_directory=str(self.batch_dir),
                        target_fields=self.target_fields,
                        model_provider=curation_provider,
                        max_tokens=self.max_tokens,
                        max_workers=self.max_workers,
                    )
            
            if not conditional_result.get("success", True):
                logger.warning(f"⚠️ Conditional processing completed with errors: {conditional_result['message']}")
            
            stage_duration = time.time() - stage_start_time
            logger.info(f"✅ Stage 3 completed in {stage_duration:.2f} seconds")
            
            # Save conditional processing output under conditional_processing/
            conditional_dir = self.batch_dir / "conditional_processing"
            conditional_dir.mkdir(exist_ok=True)
            conditional_output_file = conditional_dir / "conditional_processing_stage_output.json"
            with open(conditional_output_file, "w") as f:
                json.dump(conditional_result, f, indent=2)
            
            return conditional_result
            
        except Exception as e:
            logger.error(f"❌ Stage 3 (Conditional Processing) failed: {str(e)}")
            raise

    async def extract_sample_results_from_batch(self, batch_dir: Path) -> Dict[str, Dict[str, Any]]:
        """
        Extract curated and normalized results from a batch directory.
        
        Returns
        -------
        Dict[str, Dict[str, Any]]
            Sample ID mapped to extracted results containing curated and normalized values
        """
        batch_results = {}
        
        try:
            # Load data intake output to get sample list
            data_intake_file = batch_dir / "data_intake_output.json"
            if not data_intake_file.exists():
                logger.warning(f"No data_intake_output.json found in {batch_dir}")
                return batch_results
                
            with open(data_intake_file, "r") as f:
                data_intake = json.load(f)
            
            # Extract sample IDs
            sample_ids = []
            if isinstance(data_intake, dict):
                # Handle LinkerOutput structure
                if "sample_ids_requested" in data_intake:
                    sample_ids = data_intake["sample_ids_requested"]
                elif "curation_packages" in data_intake:
                    sample_ids = [pkg["sample_id"] for pkg in data_intake["curation_packages"]]
            
            # For each sample, extract curated and normalized values
            for sample_id in sample_ids:
                sample_data = {
                    "sample_id": sample_id,
                    "curated_fields": {},
                    "normalized_fields": {},
                    "direct_fields": {}
                }
                
                # Extract direct fields from data intake (fallback to cleaned metadata content)
                if "curation_packages" in data_intake:
                    for pkg in data_intake["curation_packages"]:
                        if pkg.get("sample_id") == sample_id:
                            series_id = pkg.get("series_id", "")
                            organism = ""
                            platform_id = ""
                            instrument = ""
                            pubmed_id = ""

                            # Sample metadata content lookup
                            sm = pkg.get("sample_metadata") or {}
                            sm_content = sm.get("content") or []
                            for kv in sm_content:
                                key = kv.get("key", "").lower()
                                val = kv.get("value", "")
                                if not organism and key == "organism":
                                    organism = val
                                if not platform_id and key in ("platform_id", "gpl", "platform"):
                                    platform_id = val
                                if not instrument and key in ("instrument", "instrument_model", "sequencer"):
                                    instrument = val

                            # Abstract metadata preferred for pubmed_id
                            am = pkg.get("abstract_metadata") or {}
                            if am.get("pmid"):
                                pubmed_id = str(am.get("pmid"))
                            else:
                                # Fallback to series metadata content
                                sr = pkg.get("series_metadata") or {}
                                sr_content = sr.get("content") or []
                                for kv in sr_content:
                                    if kv.get("key", "").lower() == "pubmed_id" and kv.get("value"):
                                        pubmed_id = str(kv.get("value"))
                                        break

                            sample_data["direct_fields"] = {
                                "organism": organism,
                                "series_id": series_id,
                                "pubmed_id": pubmed_id,
                                "platform_id": platform_id,
                                "instrument": instrument,
                            }
                            break
                
                # Extract curation results from curator_results_for_normalization files
                for curator_file in batch_dir.glob("curator_results_for_normalization_*.json"):
                    field_name = curator_file.stem.replace("curator_results_for_normalization_", "")
                    try:
                        with open(curator_file, "r") as f:
                            curator_data = json.load(f)
                        
                        # Handle list format (curator_results_for_normalization files are arrays)
                        if isinstance(curator_data, list):
                            for curator_result in curator_data:
                                if curator_result.get("sample_id") == sample_id:
                                    # Handle assay_type field which uses 'assay_type' instead of 'final_candidate'
                                    if field_name == "assay_type" and "assay_type" in curator_result:
                                        sample_data["curated_fields"][field_name] = {
                                            "final_candidate": curator_result["assay_type"],
                                            "confidence": curator_result.get("confidence", ""),
                                            "context": "",  # assay_type doesn't have context in the same format
                                            "rationale": ""  # assay_type doesn't have rationale in the same format
                                        }
                                    elif "final_candidate" in curator_result or "final_candidates" in curator_result:
                                        # Handle cases where final_candidate might be None but final_candidates array exists
                                        final_candidate_value = curator_result.get("final_candidate")
                                        final_confidence = curator_result.get("final_confidence")
                                        context = ""
                                        rationale = ""
                                        
                                        # If final_candidate is None but final_candidates array exists, use the first one
                                        if (final_candidate_value is None or final_confidence is None) and "final_candidates" in curator_result and curator_result["final_candidates"]:
                                            final_candidate_data = curator_result["final_candidates"][0]
                                            final_candidate_value = final_candidate_data.get("value", "")
                                            final_confidence = final_candidate_data.get("confidence", "")
                                            context = final_candidate_data.get("context", "")
                                            rationale = final_candidate_data.get("rationale", "")
                                        elif "final_candidates" in curator_result and curator_result["final_candidates"]:
                                            # Extract context and rationale from final_candidates array
                                            final_candidate_data = curator_result["final_candidates"][0]
                                            context = final_candidate_data.get("context", "")
                                            rationale = final_candidate_data.get("rationale", "")
                                        
                                        sample_data["curated_fields"][field_name] = {
                                            "final_candidate": final_candidate_value or "",
                                            "confidence": final_confidence or "",
                                            "context": context,
                                            "rationale": rationale
                                        }
                                    break
                    except Exception as e:
                        error_msg = f"Error reading curator file {curator_file}: {e}"
                        logger.error(error_msg)
                        # If this is a critical curator file (contains actual results), fail the consolidation
                        if "curator_output_primary_sample.json" in str(curator_file) or "curator_output_cell_line.json" in str(curator_file):
                            if curator_file.exists() and curator_file.stat().st_size > 100:  # File exists but is corrupted
                                raise ValueError(f"Critical curator file is corrupted: {curator_file}. This indicates a failed curation process. Cannot proceed with empty results.")
                
                # Also extract from field directories (alternative structure)
                for field_dir in batch_dir.iterdir():
                    if field_dir.is_dir() and field_dir.name in [
                        "disease", "tissue", "organ", "cell_line", "cell_type", "developmental_stage",
                        "ethnicity", "gender", "age", "assay_type", "treatment"
                    ]:
                        field_name = field_dir.name
                        curator_file = field_dir / "curator_output_primary_sample.json"
                        if not curator_file.exists():
                            # Try other sample type names
                            for sample_type in ["cell_line", "unknown"]:
                                curator_file = field_dir / f"curator_output_{sample_type}.json"
                                if curator_file.exists():
                                    break
                        
                        if curator_file.exists() and field_name not in sample_data["curated_fields"]:
                            try:
                                with open(curator_file, "r") as f:
                                    curator_data = json.load(f)
                                
                                # Extract final candidate from curation_results
                                if "curation_results" in curator_data:
                                    for curation_result in curator_data["curation_results"]:
                                        if curation_result.get("sample_id") == sample_id:
                                            # Handle assay_type field which uses 'assay_type' instead of 'final_candidate'
                                            if field_name == "assay_type" and "assay_type" in curation_result:
                                                sample_data["curated_fields"][field_name] = {
                                                    "final_candidate": curation_result["assay_type"],
                                                    "confidence": curation_result.get("confidence", ""),
                                                    "context": "",  # assay_type doesn't have context in the same format
                                                    "rationale": ""  # assay_type doesn't have rationale in the same format
                                                }
                                            elif "final_candidate" in curation_result or "final_candidates" in curation_result:
                                                # Handle cases where final_candidate might be None but final_candidates array exists
                                                final_candidate_value = curation_result.get("final_candidate")
                                                final_confidence = curation_result.get("final_confidence")
                                                context = ""
                                                rationale = ""
                                                
                                                # If final_candidate is None but final_candidates array exists, use the first one
                                                if (final_candidate_value is None or final_confidence is None) and "final_candidates" in curation_result and curation_result["final_candidates"]:
                                                    final_candidate_data = curation_result["final_candidates"][0]
                                                    final_candidate_value = final_candidate_data.get("value", "")
                                                    final_confidence = final_candidate_data.get("confidence", "")
                                                    context = final_candidate_data.get("context", "")
                                                    rationale = final_candidate_data.get("rationale", "")
                                                elif "final_candidates" in curation_result and curation_result["final_candidates"]:
                                                    # Extract context and rationale from final_candidates array
                                                    final_candidate_data = curation_result["final_candidates"][0]
                                                    context = final_candidate_data.get("context", "")
                                                    rationale = final_candidate_data.get("rationale", "")
                                                
                                                sample_data["curated_fields"][field_name] = {
                                                    "final_candidate": final_candidate_value or "",
                                                    "confidence": final_confidence or "",
                                                    "context": context,
                                                    "rationale": rationale
                                                }
                                            break
                            except Exception as e:
                                error_msg = f"Error reading curator file {curator_file}: {e}"
                                logger.error(error_msg)
                                # If this is a critical curator file (contains actual results), fail the consolidation
                                if "curator_output_primary_sample.json" in str(curator_file) or "curator_output_cell_line.json" in str(curator_file):
                                    if curator_file.exists() and curator_file.stat().st_size > 100:  # File exists but is corrupted
                                        raise ValueError(f"Critical curator file is corrupted: {curator_file}. This indicates a failed curation process. Cannot proceed with empty results.")
                        # If no curation set and field is not applicable, fill N/A
                        if field_name not in sample_data["curated_fields"]:
                            st = "primary_sample" if "primary_sample" in str(batch_dir) else ("cell_line" if "cell_line" in str(batch_dir) else None)
                            if st:
                                try:
                                    from src.workflows.batch_targets import TARGET_FIELD_CONFIG
                                    cfg = TARGET_FIELD_CONFIG.get("conditional_processing", {}).get(st, {})
                                    if field_name in set(cfg.get("not_applicable", [])):
                                        sample_data["curated_fields"][field_name] = {
                                            "final_candidate": "N/A",
                                            "confidence": "",
                                            "context": "",
                                            "rationale": "not_applicable_for_sample_type",
                                        }
                                except Exception:
                                    pass
                
                # Extract normalization results from batch_targets_output.json or other normalization files
                batch_targets_file = batch_dir / "batch_targets_output.json"
                if batch_targets_file.exists():
                    try:
                        with open(batch_targets_file, "r") as f:
                            batch_targets_data = json.load(f)
                        
                        # Look for normalization results in the batch targets output
                        if "normalization_results" in batch_targets_data:
                            norm_results = batch_targets_data["normalization_results"]
                            for field_name, field_data in norm_results.items():
                                if isinstance(field_data, dict) and "sample_results" in field_data:
                                    for sr in field_data["sample_results"]:
                                        if sr.get("sample_id") == sample_id:
                                            result = sr.get("result", {})
                                            if "final_normalized_term" in result:
                                                sample_data["normalized_fields"][field_name] = {
                                                    "normalized_term": result.get("final_normalized_term", ""),
                                                    "normalized_id": result.get("final_normalized_id", ""),
                                                    "ontology": result.get("final_ontology", "")
                                                }
                                            break
                        # Fallback: handle "normalization_result" (singular) with field -> sample_id map
                        elif "normalization_result" in batch_targets_data:
                            norm_results = batch_targets_data["normalization_result"]
                            if isinstance(norm_results, dict):
                                for field_name, field_map in norm_results.items():
                                    if isinstance(field_map, dict):
                                        # field_map expected: sample_id -> {normalized_term, term_id, ontology, original_value}
                                        single = field_map.get(sample_id)
                                        if isinstance(single, dict):
                                            term = single.get("normalized_term") or ""
                                            term_id = single.get("term_id") or ""
                                            ontology = single.get("ontology") or ""
                                            if term or term_id:
                                                sample_data["normalized_fields"][field_name] = {
                                                    "normalized_term": term,
                                                    "normalized_id": term_id,
                                                    "ontology": ontology,
                                                }
                    except Exception as e:
                        logger.warning(f"Error reading batch targets file {batch_targets_file}: {e}")
                
                batch_results[sample_id] = sample_data
            
            logger.info(f"🔧 Extracted results for {len(batch_results)} samples using parallel processing")
        except Exception as e:
            logger.error(f"Error extracting results from batch {batch_dir}: {e}")
        
        return batch_results

    def create_streamlined_csv_row(self, sample_id: str, sample_data: Dict[str, Any], sample_type: str, batch_name: str) -> Dict[str, Any]:
        """
        Create a streamlined CSV row matching the original batch_samples format.
        
        Parameters
        ----------
        sample_id : str
            Sample ID
        sample_data : Dict[str, Any]
            Extracted sample data with curation and normalization results
        sample_type : str
            Sample type (primary_sample, cell_line, unknown)
        batch_name : str
            Batch name
            
        Returns
        -------
        Dict[str, Any]
            CSV row data matching original format
        """
        # Initialize row with all columns from original format
        row = {
            # Core metadata
            "sample_id": sample_id,
            "sample_type": sample_type,
            "batch_num": batch_name,
            # Direct extraction fields
            "organism": "",
            "series_id": "",
            # Target fields - final candidates
            "disease_final_candidate": "",
            "tissue_final_candidate": "",
            "organ_final_candidate": "",
            "cell_line_final_candidate": "",
            "cell_type_final_candidate": "",
            "developmental_stage_final_candidate": "",
            "ethnicity_final_candidate": "",
            "gender_final_candidate": "",
            "age_final_candidate": "",
            "assay_type_final_candidate": "",
            "treatment_final_candidate": "",
            # Normalized fields
            "disease_normalized_term": "",
            "disease_normalized_id": "",
            "tissue_normalized_term": "",
            "tissue_normalized_id": "",
            "organ_normalized_term": "",
            "organ_normalized_id": "",
            # Metadata fields
            "sandbox_id": batch_name,
            "pubmed_id": "",
            "platform_id": "",
            "instrument": "",
        }
        
        # Populate direct fields
        direct_fields = sample_data.get("direct_fields", {})
        row["organism"] = direct_fields.get("organism", "")
        row["series_id"] = direct_fields.get("series_id", "")
        row["pubmed_id"] = direct_fields.get("pubmed_id", "")
        row["platform_id"] = direct_fields.get("platform_id", "")
        row["instrument"] = direct_fields.get("instrument", "")
        
        # Populate curated fields
        curated_fields = sample_data.get("curated_fields", {})
        for field_name, field_data in curated_fields.items():
            final_candidate_key = f"{field_name}_final_candidate"
            if final_candidate_key in row:
                row[final_candidate_key] = field_data.get("final_candidate", "")
        
        # Populate normalized fields
        normalized_fields = sample_data.get("normalized_fields", {})
        for field_name, field_data in normalized_fields.items():
            term_key = f"{field_name}_normalized_term"
            id_key = f"{field_name}_normalized_id"
            if term_key in row:
                row[term_key] = field_data.get("normalized_term", "")
            if id_key in row:
                row[id_key] = field_data.get("normalized_id", "")
        
        return row
    
    def _write_csv_sync(self, file_path: Path, data: list):
        """Synchronous CSV writing helper for executor usage."""
        import csv
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, 
                fieldnames=data[0].keys(), 
                quoting=csv.QUOTE_ALL,
                escapechar="\\",
                lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(data)

    def create_comprehensive_csv_row(self, sample_id: str, sample_data: Dict[str, Any], sample_type: str, batch_name: str) -> Dict[str, Any]:
        """
        Create a comprehensive CSV row with detailed curation and normalization information.
        Groups all information for each target field together for better analysis.
        
        Parameters
        ----------
        sample_id : str
            Sample ID
        sample_data : Dict[str, Any]
            Extracted sample data with curation and normalization results
        sample_type : str
            Sample type (primary_sample, cell_line, unknown)
        batch_name : str
            Batch name
            
        Returns
        -------
        Dict[str, Any]
            Comprehensive CSV row data with detailed information grouped by field
        """
        # Define all possible fields and their order
        all_curated_fields = ["disease", "tissue", "organ", "cell_line", "cell_type", "developmental_stage", 
                             "ethnicity", "gender", "age", "assay_type", "treatment"]
        all_normalized_fields = ["disease", "tissue", "organ"]
        
        # Start with basic metadata
        row = {
            "sample_id": sample_id,
            "sample_type": sample_type,
            "batch_num": batch_name,
            "organism": "",
            "series_id": "",
            "sandbox_id": batch_name,
            "pubmed_id": "",
            "platform_id": "",
            "instrument": "",
        }
        
        # Populate direct fields
        direct_fields = sample_data.get("direct_fields", {})
        row["organism"] = direct_fields.get("organism", "")
        row["series_id"] = direct_fields.get("series_id", "")
        row["pubmed_id"] = direct_fields.get("pubmed_id", "")
        row["platform_id"] = direct_fields.get("platform_id", "")
        row["instrument"] = direct_fields.get("instrument", "")
        
        # For each target field, group all related information together
        for field_name in all_curated_fields:
            # Initialize all fields for this target field
            row[f"{field_name}_final_candidate"] = ""
            row[f"{field_name}_confidence"] = ""
            row[f"{field_name}_context"] = ""
            row[f"{field_name}_rationale"] = ""
            
            # Add normalized fields if this field supports normalization
            if field_name in all_normalized_fields:
                row[f"{field_name}_normalized_term"] = ""
                row[f"{field_name}_normalized_id"] = ""
                row[f"{field_name}_normalization_confidence"] = ""
                row[f"{field_name}_prenormalized"] = ""
                row[f"{field_name}_normalization_notes"] = ""
                row[f"{field_name}_ontology"] = ""
        
        # Populate actual curated fields
        curated_fields = sample_data.get("curated_fields", {})
        for field_name, field_data in curated_fields.items():
            if field_name in all_curated_fields:
                row[f"{field_name}_final_candidate"] = field_data.get("final_candidate", "")
                row[f"{field_name}_confidence"] = field_data.get("confidence", "")
                row[f"{field_name}_context"] = field_data.get("context", "")
                row[f"{field_name}_rationale"] = field_data.get("rationale", "")
        
        # Populate actual normalized fields
        normalized_fields = sample_data.get("normalized_fields", {})
        for field_name, field_data in normalized_fields.items():
            if field_name in all_normalized_fields:
                row[f"{field_name}_normalized_term"] = field_data.get("normalized_term", "")
                row[f"{field_name}_normalized_id"] = field_data.get("normalized_id", "")
                row[f"{field_name}_normalization_confidence"] = field_data.get("normalization_confidence", "")
                row[f"{field_name}_prenormalized"] = field_data.get("prenormalized", "")
                row[f"{field_name}_normalization_notes"] = field_data.get("normalization_notes", "")
                row[f"{field_name}_ontology"] = field_data.get("ontology", "")
        
        return row

    async def consolidate_output_files(
        self,
        data_intake_output: LinkerOutput,
        preprocessing_output: Dict[str, Any],
        conditional_output: Dict[str, Any],
    ):
        """
        Consolidate outputs into final parquet/CSV files maintaining backward compatibility.
        
        This method now uses async parallelization for improved performance:
        - Parallel batch result extraction
        - Concurrent file I/O operations
        - Parallelized row creation for large datasets
        
        Parameters
        ----------
        data_intake_output : LinkerOutput
            Output from data intake stage
        preprocessing_output : Dict[str, Any]
            Output from preprocessing stage
        conditional_output : Dict[str, Any]
            Output from conditional processing stage
        """
        logger.info(f"📊 Consolidating output files into {self.output_format} format")
        
        try:
            # Create workflow summary
            workflow_summary = {
                "workflow_type": "efficient_batch_samples",
                "total_execution_time_seconds": (
                    data_intake_output.execution_time_seconds + 
                    preprocessing_output.get("execution_time_seconds", 0) +
                    conditional_output.get("execution_time_seconds", 0)
                ),
                "stages": {
                    "data_intake": {
                        "success": data_intake_output.success,
                        "execution_time_seconds": data_intake_output.execution_time_seconds,
                        "samples_processed": len(data_intake_output.sample_ids_requested),
                    },
                    "preprocessing": {
                        "success": preprocessing_output["success"],
                        "execution_time_seconds": preprocessing_output.get("execution_time_seconds", 0),
                        "sample_type_distribution": preprocessing_output.get("statistics", {}).get("sample_type_distribution", {}),
                    },
                    "conditional_processing": {
                        "success": conditional_output["success"],
                        "execution_time_seconds": conditional_output.get("execution_time_seconds", 0),
                        "batches_processed": conditional_output.get("statistics", {}).get("total_batches", 0),
                        "samples_processed": conditional_output.get("statistics", {}).get("total_samples", 0),
                    }
                },
                "configuration": self.batch_config,
                "timestamp": datetime.now().isoformat(),
            }
            
            # Save workflow summary
            workflow_summary_file = self.batch_dir / "workflow_summary.json"
            with open(workflow_summary_file, "w") as f:
                json.dump(workflow_summary, f, indent=2)
            
            # Save sample metadata mapping for compatibility
            sample_metadata = {
                "sample_type_mapping": preprocessing_output.get("sample_type_mapping", {}),
                "batch_configuration": self.batch_config,
                "processing_statistics": {
                    "data_intake": workflow_summary["stages"]["data_intake"],
                    "preprocessing": workflow_summary["stages"]["preprocessing"],
                    "conditional_processing": workflow_summary["stages"]["conditional_processing"],
                },
                "timestamp": datetime.now().isoformat(),
            }
            
            sample_metadata_file = self.batch_dir / "sample_metadata.json"
            with open(sample_metadata_file, "w") as f:
                json.dump(sample_metadata, f, indent=2)
            
            # Create placeholder output files for backward compatibility
            # Note: The actual data is in the individual batch directories
            # These files provide summary statistics and metadata
            
            # Precompute direct fields (organism, pubmed_id, platform_id, instrument_model, series_id)
            # using the original extractor for parity with legacy workflow
            all_sample_ids = []
            for batch_result in conditional_output.get("batch_results", []):
                if batch_result.get("success"):
                    all_sample_ids.extend(batch_result.get("batch_samples", []))
            direct_fields_map = extract_direct_fields_from_data_intake(
                data_intake_output=data_intake_output,
                sample_ids=list(dict.fromkeys(all_sample_ids))  # preserve order, remove dups
            ) if all_sample_ids else {}

            if self.output_format == "parquet":
                # Extract detailed results from each batch directory (matching original format)
                streamlined_data = []
                comprehensive_data = []
                
                for batch_result in conditional_output.get("batch_results", []):
                    if batch_result["success"]:
                        batch_name = batch_result["batch_name"] 
                        sample_type = batch_result["sample_type"]
                        batch_dir = Path(batch_result["batch_directory"])
                        
                        # Extract detailed curation and normalization results
                        sample_results = await self.extract_sample_results_from_batch(batch_dir)
                        
                        for sample_id in batch_result["batch_samples"]:
                            if sample_id in sample_results:
                                sample_data = sample_results[sample_id]
                                
                                # Create streamlined row matching original batch_samples format
                                streamlined_row = self.create_streamlined_csv_row(sample_id, sample_data, sample_type, batch_name)
                                # Override with authoritative direct fields from data intake for parity
                                df = direct_fields_map.get(sample_id, {})
                                if df:
                                    streamlined_row["organism"] = df.get("organism", {}).get("value", streamlined_row.get("organism", ""))
                                    streamlined_row["series_id"] = df.get("series_id", {}).get("value", streamlined_row.get("series_id", ""))
                                    streamlined_row["pubmed_id"] = df.get("pubmed_id", {}).get("value", streamlined_row.get("pubmed_id", ""))
                                    streamlined_row["platform_id"] = df.get("platform_id", {}).get("value", streamlined_row.get("platform_id", ""))
                                    streamlined_row["instrument"] = df.get("instrument_model", {}).get("value", streamlined_row.get("instrument", ""))
                                streamlined_data.append(streamlined_row)
                                
                                # Create comprehensive row with detailed information
                                comprehensive_row = self.create_comprehensive_csv_row(sample_id, sample_data, sample_type, batch_name)
                                # Override with authoritative direct fields from data intake for parity
                                if df:
                                    comprehensive_row["organism"] = df.get("organism", {}).get("value", comprehensive_row.get("organism", ""))
                                    comprehensive_row["series_id"] = df.get("series_id", {}).get("value", comprehensive_row.get("series_id", ""))
                                    comprehensive_row["pubmed_id"] = df.get("pubmed_id", {}).get("value", comprehensive_row.get("pubmed_id", ""))
                                    comprehensive_row["platform_id"] = df.get("platform_id", {}).get("value", comprehensive_row.get("platform_id", ""))
                                    comprehensive_row["instrument"] = df.get("instrument_model", {}).get("value", comprehensive_row.get("instrument", ""))
                                comprehensive_data.append(comprehensive_row)
                
                if streamlined_data:
                    # Save streamlined parquet file
                    streamlined_df = pd.DataFrame(streamlined_data)
                    streamlined_file = self.batch_dir / "batch_results.parquet"
                    streamlined_df.to_parquet(streamlined_file, index=False)
                    
                    # Save comprehensive parquet file with detailed information
                    comprehensive_df = pd.DataFrame(comprehensive_data)
                    comprehensive_file = self.batch_dir / "comprehensive_batch_results.parquet"
                    comprehensive_df.to_parquet(comprehensive_file, index=False)
                    
                    logger.info(f"✅ Saved parquet files: {streamlined_file} and {comprehensive_file}")
                
            elif self.output_format == "csv":
                # 🚀 PARALLELIZATION IMPROVEMENT: Process all batch results concurrently
                streamlined_data = []
                comprehensive_data = []
                
                async def process_batch_result_csv(batch_result):
                    """Process a single batch result and extract all sample data for CSV."""
                    if not batch_result["success"]:
                        return [], []  # Return empty lists for failed batches
                    
                    batch_name = batch_result["batch_name"] 
                    sample_type = batch_result["sample_type"]
                    batch_dir = Path(batch_result["batch_directory"])
                    
                    # Extract detailed curation and normalization results
                    sample_results = await self.extract_sample_results_from_batch(batch_dir)
                    
                    batch_streamlined_data = []
                    batch_comprehensive_data = []
                    
                    # Process all samples in this batch
                    for sample_id in batch_result["batch_samples"]:
                        if sample_id in sample_results:
                            sample_data = sample_results[sample_id]
                            
                            # Create streamlined row matching original batch_samples format
                            streamlined_row = self.create_streamlined_csv_row(sample_id, sample_data, sample_type, batch_name)
                            # Override with authoritative direct fields from data intake for parity
                            df = direct_fields_map.get(sample_id, {})
                            if df:
                                streamlined_row["organism"] = df.get("organism", {}).get("value", streamlined_row.get("organism", ""))
                                streamlined_row["series_id"] = df.get("series_id", {}).get("value", streamlined_row.get("series_id", ""))
                                streamlined_row["pubmed_id"] = df.get("pubmed_id", {}).get("value", streamlined_row.get("pubmed_id", ""))
                                streamlined_row["platform_id"] = df.get("platform_id", {}).get("value", streamlined_row.get("platform_id", ""))
                                streamlined_row["instrument"] = df.get("instrument_model", {}).get("value", streamlined_row.get("instrument", ""))
                            batch_streamlined_data.append(streamlined_row)
                            
                            # Create comprehensive row with detailed information
                            comprehensive_row = self.create_comprehensive_csv_row(sample_id, sample_data, sample_type, batch_name)
                            # Override with authoritative direct fields from data intake for parity
                            if df:
                                comprehensive_row["organism"] = df.get("organism", {}).get("value", comprehensive_row.get("organism", ""))
                                comprehensive_row["series_id"] = df.get("series_id", {}).get("value", comprehensive_row.get("series_id", ""))
                                comprehensive_row["pubmed_id"] = df.get("pubmed_id", {}).get("value", comprehensive_row.get("pubmed_id", ""))
                                comprehensive_row["platform_id"] = df.get("platform_id", {}).get("value", comprehensive_row.get("platform_id", ""))
                                comprehensive_row["instrument"] = df.get("instrument_model", {}).get("value", comprehensive_row.get("instrument", ""))
                            batch_comprehensive_data.append(comprehensive_row)
                    
                    return batch_streamlined_data, batch_comprehensive_data
                
                # Process all batch results concurrently
                batch_tasks = [
                    process_batch_result_csv(batch_result) 
                    for batch_result in conditional_output.get("batch_results", [])
                ]
                
                if batch_tasks:
                    batch_processing_results = await asyncio.gather(*batch_tasks)
                    
                    # Flatten results from all batches
                    for batch_streamlined, batch_comprehensive in batch_processing_results:
                        streamlined_data.extend(batch_streamlined)
                        comprehensive_data.extend(batch_comprehensive)
                
                # 🚀 PARALLELIZATION IMPROVEMENT: Concurrent file writing
                async def write_csv_file(file_path: Path, data: list, file_type: str):
                    """Write CSV file asynchronously."""
                    if not data:
                        return None
                    
                    # Use synchronous writer with async executor
                    await asyncio.get_event_loop().run_in_executor(
                        None,  # Use default executor
                        lambda: self._write_csv_sync(file_path, data)
                    )
                    return str(file_path)
                
                # Write both CSV files concurrently at the end
                write_tasks = []
                if streamlined_data:
                    write_tasks.append(write_csv_file(
                        self.batch_dir / "batch_results.csv", 
                        streamlined_data, 
                        "streamlined"
                    ))
                if comprehensive_data:
                    write_tasks.append(write_csv_file(
                        self.batch_dir / "comprehensive_batch_results.csv", 
                        comprehensive_data, 
                        "comprehensive"
                    ))
                
                if write_tasks:
                    completed_files = await asyncio.gather(*write_tasks)
                    files_saved = [f for f in completed_files if f is not None]
                    
                    if files_saved:
                        logger.info(f"✅ Saved CSV files: {' and '.join(files_saved)}")
            
            logger.info(f"📁 All results saved to: {self.batch_dir}")
            
        except Exception as e:
            logger.error(f"❌ Error consolidating output files: {e}")
            raise

    async def run_complete_workflow(self, arbitrator_test_mode: bool = False) -> Dict[str, Any]:
        """
        Run the complete efficient batch samples workflow.
        
        Returns
        -------
        Dict[str, Any]
            Complete workflow results
        """
        start_time = time.time()
        
        logger.info("🚀 Starting efficient batch samples workflow")
        logger.info("📋 Configuration: %s", self.batch_config)
        
        try:
            # Load samples
            samples = self.load_samples()
            
            # 🚀 ADVANCED PIPELINE PARALLELIZATION: Overlapping stage preparation
            logger.info("🔧 Using advanced pipeline parallelization for maximum performance")
            
            # 🔧 PERFORMANCE MONITORING: Track parallelization effectiveness
            perf_monitor = {
                "pipeline_overlaps": 0,
                "concurrent_tasks": 0,
                "parallel_operations": [],
                "stage_timings": {}
            }
            
            # Stage 1: Data Intake with concurrent resource preparation
            logger.info("🚀 Stage 1: Running data intake with pipeline preparation")
            
            # Start data intake and begin preparing downstream resources concurrently
            data_intake_task = asyncio.create_task(self.run_data_intake_stage(samples))
            
            # While data intake runs, prepare preprocessing resources
            async def prepare_preprocessing_resources():
                """Prepare model providers and other resources for preprocessing while data intake runs."""
                try:
                    # Pre-create model providers to save time later
                    self._preprocessing_providers = self._create_sample_type_model_providers()
                    
                    # Pre-create output directories
                    preprocessing_dir = self.batch_dir / "preprocessing"
                    preprocessing_dir.mkdir(exist_ok=True)
                    
                    conditional_dir = self.batch_dir / "conditional_processing"
                    conditional_dir.mkdir(exist_ok=True)
                    
                    logger.info("🔧 Preprocessing resources prepared concurrently")
                    return True
                except Exception as e:
                    logger.warning(f"⚠️ Resource preparation failed: {e}")
                    return False
            
            # Start resource preparation concurrently with data intake
            prep_task = asyncio.create_task(prepare_preprocessing_resources())
            perf_monitor["pipeline_overlaps"] += 1
            perf_monitor["concurrent_tasks"] += 1
            
            # Wait for data intake to complete
            data_intake_output = await data_intake_task
            
            # Ensure resource preparation is complete
            prep_success = await prep_task
            if prep_success:
                logger.info("🔧 Pipeline resource preparation successful")
            
            # Get successfully processed samples from data intake
            successful_samples = data_intake_output.sample_ids_requested
            if hasattr(data_intake_output, 'sample_ids_for_curation') and data_intake_output.sample_ids_for_curation:
                successful_samples = data_intake_output.sample_ids_for_curation
            
            if len(successful_samples) != len(samples):
                failed_count = len(samples) - len(successful_samples)
                logger.warning(f"⚠️ {failed_count} samples failed during data intake and will be excluded from processing")
            
            # Stage 2: Preprocessing with pre-prepared resources
            logger.info("🚀 Stage 2: Running preprocessing with prepared resources")
            preprocessing_task = asyncio.create_task(
                self.run_preprocessing_stage(data_intake_output, successful_samples)
            )
            
            # While preprocessing runs, prepare conditional processing resources
            async def prepare_conditional_resources(data_intake_out, preprocessing_out=None):
                """Prepare conditional processing resources while preprocessing runs."""
                try:
                    # Pre-create model providers for conditional processing
                    if not hasattr(self, '_conditional_providers'):
                        _, curation_provider, _ = self._create_sample_type_model_providers()
                        self._conditional_providers = curation_provider
                    
                    logger.info("🔧 Conditional processing resources prepared concurrently")
                    return True
                except Exception as e:
                    logger.warning(f"⚠️ Conditional resource preparation failed: {e}")
                    return False
            
            # Start conditional resource preparation
            conditional_prep_task = asyncio.create_task(
                prepare_conditional_resources(data_intake_output)
            )
            perf_monitor["pipeline_overlaps"] += 1
            perf_monitor["concurrent_tasks"] += 1
            
            # Wait for preprocessing to complete
            preprocessing_output = await preprocessing_task
            
            # Ensure conditional resources are ready
            conditional_prep_success = await conditional_prep_task
            if conditional_prep_success:
                logger.info("🔧 Conditional processing resources ready")
            
            # Stage 3: Conditional Processing with pre-prepared resources
            logger.info("🚀 Stage 3: Running conditional processing with prepared resources")
            conditional_task = asyncio.create_task(
                self.run_conditional_processing_stage(
                    preprocessing_output, data_intake_output, 
                    arbitrator_test_mode=arbitrator_test_mode
                )
            )
            
            # While conditional processing runs, prepare consolidation resources
            async def prepare_consolidation_resources():
                """Prepare consolidation resources while conditional processing runs."""
                try:
                    # Pre-calculate direct fields that don't depend on conditional results
                    if hasattr(data_intake_output, 'sample_ids_requested'):
                        # Pre-validate output directories and prepare file paths
                        output_paths = [
                            self.batch_dir / "batch_results.csv",
                            self.batch_dir / "comprehensive_batch_results.csv"
                        ]
                        # Ensure parent directories exist
                        for path in output_paths:
                            path.parent.mkdir(parents=True, exist_ok=True)
                        
                        logger.info("🔧 Consolidation resources prepared concurrently")
                    return True
                except Exception as e:
                    logger.warning(f"⚠️ Consolidation preparation failed: {e}")
                    return False
            
            # Start consolidation preparation
            consolidation_prep_task = asyncio.create_task(prepare_consolidation_resources())
            perf_monitor["pipeline_overlaps"] += 1
            perf_monitor["concurrent_tasks"] += 1
            
            # Wait for conditional processing
            conditional_output = await conditional_task
            
            # Ensure consolidation is ready
            consolidation_prep_success = await consolidation_prep_task
            if consolidation_prep_success:
                logger.info("🔧 Consolidation resources ready")
            
            # Consolidation with parallel processing (already implemented)
            logger.info("🚀 Stage 4: Running consolidation with parallel processing")
            await self.consolidate_output_files(
                data_intake_output, preprocessing_output, conditional_output
            )
            
            total_execution_time = time.time() - start_time
            
            # 📊 PERFORMANCE REPORTING: Log parallelization effectiveness
            logger.info("🔧 Performance Summary:")
            logger.info(f"   Pipeline overlaps used: {perf_monitor['pipeline_overlaps']}")
            logger.info(f"   Concurrent tasks launched: {perf_monitor['concurrent_tasks']}")
            logger.info(f"   Total parallelization optimizations: {len(perf_monitor['parallel_operations'])}")
            
            # Create final workflow result
            workflow_result = {
                "success": True,
                "message": "Efficient batch samples workflow completed successfully",
                "total_execution_time_seconds": total_execution_time,
                "samples_requested": len(samples),
                "batch_directory": str(self.batch_dir),
                "configuration": self.batch_config,
                "stage_results": {
                    "data_intake": {
                        "success": data_intake_output.success,
                        "samples_processed": len(data_intake_output.sample_ids_requested),
                    },
                    "preprocessing": {
                        "success": preprocessing_output["success"],
                        "sample_type_distribution": preprocessing_output.get("statistics", {}).get("sample_type_distribution", {}),
                        "total_batches": preprocessing_output.get("statistics", {}).get("total_batches", 0),
                    },
                    "conditional_processing": {
                        "success": conditional_output["success"],
                        "successful_batches": conditional_output.get("statistics", {}).get("successful_batches", 0),
                        "failed_batches": conditional_output.get("statistics", {}).get("failed_batches", 0),
                        "successful_samples": conditional_output.get("statistics", {}).get("successful_samples", 0),
                    }
                },
                "timestamp": datetime.now().isoformat(),
            }
            
            logger.info("✅ Efficient batch samples workflow completed successfully in %.2f seconds", total_execution_time)
            logger.info("📊 Final results: %s", workflow_result['stage_results'])
            
            return workflow_result
            
        except Exception as e:
            total_execution_time = time.time() - start_time
            logger.error(f"❌ Efficient batch samples workflow failed: {str(e)}")
            
            return {
                "success": False,
                "message": f"Efficient batch samples workflow failed: {str(e)}",
                "total_execution_time_seconds": total_execution_time,
                "error": str(e),
                "batch_directory": str(self.batch_dir),
                "configuration": self.batch_config,
                "timestamp": datetime.now().isoformat(),
            }
    
async def run_efficient_batch_samples_workflow(
    output_dir: str = "batch",
    sample_count: int = 100,
    batch_size: int = 5,
    samples_file: str = "archs4_samples/archs4_gsm_ids.txt",
    model_provider: ModelProvider = None,
    max_tokens: int = None,
    target_fields: list = None,
    sample_type_filter: str = None,
    batch_name: str = None,
    output_format: str = "parquet",
    max_workers: int | None = None,
    enable_profiling: bool = False,
    conditional_mode: str = "eval",
    arbitrator_test_mode: bool = False,
    max_iterations: int = 2,
) -> Dict[str, Any]:
    """
    Run the efficient batch samples workflow.
    
    This is the main entry point for the efficient batch samples processing
    that maintains backward compatibility with the original batch_samples interface.
    
    Parameters
    ----------
    output_dir : str
        Directory to save batch results (default: "batch")
    sample_count : int
        Number of samples to process (default: 100)
    batch_size : int
        Number of samples per batch (default: 5)
    samples_file : str
        Path to archs4_gsm_ids.txt file containing GSM IDs (default: "archs4_samples/archs4_gsm_ids.txt")
    model_provider : ModelProvider, optional
        Model provider for LLM requests
    max_tokens : int, optional
        Maximum tokens for LLM responses
    target_fields : list, optional
        List of target fields to process
    sample_type_filter : str, optional
        Filter to process only specific sample type
    batch_name : str, optional
        Custom name for the batch directory
    output_format : str
        Output format: 'parquet' (default) or 'csv'
        
    Returns
    -------
    Dict[str, Any]
        Workflow execution results
    """
    processor = EfficientBatchSamplesProcessor(
        output_dir=output_dir,
        sample_count=sample_count,
        batch_size=batch_size,
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
    processor.conditional_mode = conditional_mode
    processor.max_iterations = max_iterations
    
    return await processor.run_complete_workflow(arbitrator_test_mode=arbitrator_test_mode)


if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(
        description="Efficient batch samples workflow using three-stage architecture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python batch_samples_efficient.py --sample-count 50 --batch-size 5

  # With custom batch name and sample type filter
  python batch_samples_efficient.py --sample-count 100 --batch-name test --sample-type-filter primary_sample

  # With specific target fields
  python batch_samples_efficient.py --sample-count 25 --target-fields disease,tissue,organ

  # CSV output format
  python batch_samples_efficient.py --sample-count 50 --output-format csv
        """,
    )

    parser.add_argument("--output-dir", default="batch", help="Output directory (default: batch)")
    parser.add_argument("--sample-count", type=int, default=100, help="Number of samples to process (default: 100)")
    parser.add_argument("--batch-size", type=int, default=5, help="Batch size (default: 5)")
    parser.add_argument("--samples-file", default="archs4_samples/archs4_gsm_ids.txt", help="Samples file path (default: archs4_samples/archs4_gsm_ids.txt)")
    parser.add_argument("--max-tokens", type=int, help="Maximum tokens for LLM responses")
    parser.add_argument("--target-fields", help="Comma-separated list of target fields")
    parser.add_argument("--sample-type-filter", choices=["primary_sample", "cell_line", "unknown"], help="Filter by sample type")
    parser.add_argument("--batch-name", help="Custom batch name")
    parser.add_argument("--output-format", choices=["parquet", "csv"], default="parquet", help="Output format (default: parquet)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--conditional-mode", 
        choices=["classic", "eval"], 
        default="eval",
        help="Conditional processing mode (default: eval)"
    )

    args = parser.parse_args()

    # Set up logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse target fields
    target_fields = None
    if args.target_fields:
        target_fields = [f.strip() for f in args.target_fields.split(",")]

    # Run workflow
    async def main():
        result = await run_efficient_batch_samples_workflow(
            output_dir=args.output_dir,
            sample_count=args.sample_count,
            batch_size=args.batch_size,
            samples_file=args.samples_file,
            max_tokens=args.max_tokens,
            target_fields=target_fields,
            sample_type_filter=args.sample_type_filter,
            batch_name=args.batch_name,
            output_format=args.output_format,
            conditional_mode=args.conditional_mode,
        )

        print(f"\nWorkflow Result: {'✅ Success' if result['success'] else '❌ Failed'}")
        print(f"Message: {result['message']}")
        print(f"Total Execution Time: {result['total_execution_time_seconds']:.2f} seconds")
        
        if result['success']:
            print(f"Batch Directory: {result['batch_directory']}")
            print(f"Stage Results: {json.dumps(result['stage_results'], indent=2)}")

        return 0 if result['success'] else 1

    # Run the async main function
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
