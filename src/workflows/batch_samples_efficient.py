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
import csv
import json
import pandas as pd
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging
from tqdm import tqdm

from dotenv import load_dotenv
from agents import ModelProvider

# Import the three workflows
from src.workflows.data_intake_sql import run_data_intake_sql_workflow
from src.workflows.preprocessing import run_preprocessing_workflow
from src.workflows.conditional_processing import run_conditional_processing_workflow

from src.models import LinkerOutput
from src.models.common import KeyValue

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
            "disease", "tissue", "organ", "cell_line", "developmental_stage",
            "ethnicity", "gender", "age", "organism", "pubmed_id", "platform_id", "instrument"
        ]
        self.sample_type_filter = sample_type_filter
        self.batch_name = batch_name
        self.output_format = output_format
        
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
            logger.info(f"🕒 Using timestamp-based batch name: batch_{timestamp}")
        
        self.batch_dir.mkdir(parents=True, exist_ok=True)
        
        # Create unified discovery directory structure
        self.discovery_dir = self.batch_dir / "discovery"
        self.discovery_dir.mkdir(parents=True, exist_ok=True)

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
            except:
                sample_type_provider = self.base_model_provider
        
        # Use Gemini Pro for conditional curation (higher quality)
        curation_provider = None
        if self.base_model_provider:
            try:
                curation_provider = type(self.base_model_provider)(default_model="google/gemini-2.5-pro")
            except:
                curation_provider = self.base_model_provider
        
        # Use Gemini Flash for normalization (faster, cost-effective)
        normalization_provider = None
        if self.base_model_provider:
            try:
                normalization_provider = type(self.base_model_provider)(default_model="google/gemini-2.5-flash")
            except:
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
            )
            
            if not data_intake_result.success:
                raise RuntimeError(f"Data intake failed: {data_intake_result.message}")
            
            stage_duration = time.time() - stage_start_time
            logger.info(f"✅ Stage 1 completed in {stage_duration:.2f} seconds")
            
            # Save data intake output for auditability
            data_intake_output_file = self.batch_dir / "data_intake_stage_output.json"
            with open(data_intake_output_file, "w") as f:
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
        logger.info(f"🚀 Stage 2: Running preprocessing (sample type curation and batching)")
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
            )
            
            if not preprocessing_result["success"]:
                raise RuntimeError(f"Preprocessing failed: {preprocessing_result['message']}")
            
            stage_duration = time.time() - stage_start_time
            logger.info(f"✅ Stage 2 completed in {stage_duration:.2f} seconds")
            
            # Save preprocessing output for auditability
            preprocessing_output_file = self.batch_dir / "preprocessing_stage_output.json"
            with open(preprocessing_output_file, "w") as f:
                json.dump(preprocessing_result, f, indent=2)
            
            return preprocessing_result
            
        except Exception as e:
            logger.error(f"❌ Stage 2 (Preprocessing) failed: {str(e)}")
            raise

    async def run_conditional_processing_stage(
        self, 
        preprocessing_output: Dict[str, Any], 
        data_intake_output: LinkerOutput
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
        logger.info(f"🚀 Stage 3: Running conditional processing (curation and normalization)")
        stage_start_time = time.time()
        
        try:
            # Extract sample type batches
            sample_type_batches = preprocessing_output["sample_type_batches"]
            
            # Create model provider for conditional processing (Gemini Pro/Flash hybrid)
            _, curation_provider, _ = self._create_sample_type_model_providers()
            
            # Run conditional processing workflow
            conditional_result = await run_conditional_processing_workflow(
                sample_type_batches=sample_type_batches,
                data_intake_output=data_intake_output,
                session_directory=str(self.batch_dir),
                target_fields=self.target_fields,
                model_provider=curation_provider,  # Will be specialized internally
                max_tokens=self.max_tokens,
            )
            
            if not conditional_result["success"]:
                logger.warning(f"⚠️ Conditional processing completed with errors: {conditional_result['message']}")
            
            stage_duration = time.time() - stage_start_time
            logger.info(f"✅ Stage 3 completed in {stage_duration:.2f} seconds")
            
            # Save conditional processing output for auditability
            conditional_output_file = self.batch_dir / "conditional_processing_stage_output.json"
            with open(conditional_output_file, "w") as f:
                json.dump(conditional_result, f, indent=2)
            
            return conditional_result
            
        except Exception as e:
            logger.error(f"❌ Stage 3 (Conditional Processing) failed: {str(e)}")
            raise

    def extract_sample_results_from_batch(self, batch_dir: Path) -> Dict[str, Dict[str, Any]]:
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
                
                # Extract direct fields from data intake
                if "curation_packages" in data_intake:
                    for pkg in data_intake["curation_packages"]:
                        if pkg["sample_id"] == sample_id:
                            sample_data["direct_fields"] = {
                                "organism": pkg.get("organism", ""),
                                "series_id": pkg.get("series_id", ""),
                                "pubmed_id": pkg.get("pubmed_id", ""),
                                "platform_id": pkg.get("platform_id", ""),
                                "instrument": pkg.get("instrument", "")
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
                                    if "final_candidate" in curator_result:
                                        sample_data["curated_fields"][field_name] = {
                                            "final_candidate": curator_result["final_candidate"],
                                            "confidence": curator_result.get("final_confidence", ""),
                                            "context": curator_result.get("context", ""),
                                            "rationale": curator_result.get("rationale", "")
                                        }
                                    break
                    except Exception as e:
                        logger.warning(f"Error reading curator file {curator_file}: {e}")
                
                # Also extract from field directories (alternative structure)
                for field_dir in batch_dir.iterdir():
                    if field_dir.is_dir() and field_dir.name in [
                        "disease", "tissue", "organ", "cell_line", "developmental_stage",
                        "ethnicity", "gender", "age", "assay_type", "treatment"
                    ]:
                        field_name = field_dir.name
                        curator_file = field_dir / f"curator_output_primary_sample.json"
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
                                            if "final_candidate" in curation_result:
                                                sample_data["curated_fields"][field_name] = {
                                                    "final_candidate": curation_result["final_candidate"],
                                                    "confidence": curation_result.get("final_confidence", ""),
                                                    "context": curation_result.get("context", ""),
                                                    "rationale": curation_result.get("rationale", "")
                                                }
                                            break
                            except Exception as e:
                                logger.warning(f"Error reading curator file {curator_file}: {e}")
                
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
                                    for sample_result in field_data["sample_results"]:
                                        if sample_result.get("sample_id") == sample_id:
                                            result = sample_result.get("result", {})
                                            if "final_normalized_term" in result:
                                                sample_data["normalized_fields"][field_name] = {
                                                    "normalized_term": result["final_normalized_term"],
                                                    "normalized_id": result.get("final_normalized_id", ""),
                                                    "ontology": result.get("final_ontology", "")
                                                }
                                            break
                    except Exception as e:
                        logger.warning(f"Error reading batch targets file {batch_targets_file}: {e}")
                
                batch_results[sample_id] = sample_data
                
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
            "treatment": ""  # Additional treatment field
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

    def consolidate_output_files(
        self,
        data_intake_output: LinkerOutput,
        preprocessing_output: Dict[str, Any],
        conditional_output: Dict[str, Any],
    ):
        """
        Consolidate outputs into final parquet/CSV files maintaining backward compatibility.
        
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
            
            if self.output_format == "parquet":
                # Extract detailed results from each batch directory (matching original format)
                detailed_data = []
                
                for batch_result in conditional_output.get("batch_results", []):
                    if batch_result["success"]:
                        batch_name = batch_result["batch_name"] 
                        sample_type = batch_result["sample_type"]
                        batch_dir = Path(batch_result["batch_directory"])
                        
                        # Extract detailed curation and normalization results
                        sample_results = self.extract_sample_results_from_batch(batch_dir)
                        
                        for sample_id in batch_result["batch_samples"]:
                            if sample_id in sample_results:
                                sample_data = sample_results[sample_id]
                                # Create row matching original batch_samples format
                                row = self.create_streamlined_csv_row(sample_id, sample_data, sample_type, batch_name)
                                detailed_data.append(row)
                
                if detailed_data:
                    df = pd.DataFrame(detailed_data)
                    
                    # Save streamlined results (matching original format)
                    streamlined_file = self.batch_dir / "batch_results.parquet"
                    df.to_parquet(streamlined_file, index=False)
                    
                    # Save comprehensive results (same as streamlined for now)
                    comprehensive_file = self.batch_dir / "comprehensive_batch_results.parquet"
                    df.to_parquet(comprehensive_file, index=False)
                    
                    logger.info(f"✅ Saved parquet files: {streamlined_file} and {comprehensive_file}")
                
            elif self.output_format == "csv":
                # Extract detailed results from each batch directory (matching original format)
                detailed_data = []
                
                for batch_result in conditional_output.get("batch_results", []):
                    if batch_result["success"]:
                        batch_name = batch_result["batch_name"] 
                        sample_type = batch_result["sample_type"]
                        batch_dir = Path(batch_result["batch_directory"])
                        
                        # Extract detailed curation and normalization results
                        sample_results = self.extract_sample_results_from_batch(batch_dir)
                        
                        for sample_id in batch_result["batch_samples"]:
                            if sample_id in sample_results:
                                sample_data = sample_results[sample_id]
                                # Create row matching original batch_samples format
                                row = self.create_streamlined_csv_row(sample_id, sample_data, sample_type, batch_name)
                                detailed_data.append(row)
                
                if detailed_data:
                    # Save CSV files with detailed results
                    streamlined_file = self.batch_dir / "batch_results.csv"
                    with open(streamlined_file, "w", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=detailed_data[0].keys())
                        writer.writeheader()
                        writer.writerows(detailed_data)
                    
                    # Copy as comprehensive results
                    comprehensive_file = self.batch_dir / "comprehensive_batch_results.csv"
                    with open(comprehensive_file, "w", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=detailed_data[0].keys())
                        writer.writeheader()
                        writer.writerows(detailed_data)
                    
                    logger.info(f"✅ Saved CSV files: {streamlined_file} and {comprehensive_file}")
            
            logger.info(f"📁 All results saved to: {self.batch_dir}")
            
        except Exception as e:
            logger.error(f"❌ Error consolidating output files: {e}")
            raise

    async def run_complete_workflow(self) -> Dict[str, Any]:
        """
        Run the complete efficient batch samples workflow.
        
        Returns
        -------
        Dict[str, Any]
            Complete workflow results
        """
        start_time = time.time()
        
        logger.info(f"🚀 Starting efficient batch samples workflow")
        logger.info(f"📋 Configuration: {self.batch_config}")
        
        try:
            # Load samples
            samples = self.load_samples()
            
            # Stage 1: Data Intake
            data_intake_output = await self.run_data_intake_stage(samples)
            
            # Get successfully processed samples from data intake
            successful_samples = data_intake_output.sample_ids_requested
            if hasattr(data_intake_output, 'sample_ids_for_curation') and data_intake_output.sample_ids_for_curation:
                successful_samples = data_intake_output.sample_ids_for_curation
            
            logger.info(f"📋 Data intake completed: {len(successful_samples)}/{len(samples)} samples processed successfully")
            if len(successful_samples) != len(samples):
                failed_count = len(samples) - len(successful_samples)
                logger.warning(f"⚠️ {failed_count} samples failed during data intake and will be excluded from processing")
            
            # Stage 2: Preprocessing (using only successful samples)
            preprocessing_output = await self.run_preprocessing_stage(data_intake_output, successful_samples)
            
            # Stage 3: Conditional Processing
            conditional_output = await self.run_conditional_processing_stage(
                preprocessing_output, data_intake_output
            )
            
            # Consolidate output files
            self.consolidate_output_files(
                data_intake_output, preprocessing_output, conditional_output
            )
            
            total_execution_time = time.time() - start_time
            
            # Create final workflow result
            workflow_result = {
                "success": True,
                "message": f"Efficient batch samples workflow completed successfully",
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
            
            logger.info(f"✅ Efficient batch samples workflow completed successfully in {total_execution_time:.2f} seconds")
            logger.info(f"📊 Final results: {workflow_result['stage_results']}")
            
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
    )
    
    return await processor.run_complete_workflow()


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
