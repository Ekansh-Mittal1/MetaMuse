"""
Batch samples workflow for processing multiple GEO samples with comprehensive metadata extraction.

This script randomly selects 100 unique GSM samples from Age.txt and processes them in batches of 5
using the batch_targets workflow. It extracts all target metadata fields and organizes results
in a comprehensive CSV format with detailed curation and normalization data.

Output Structure:
batch/
├── batch_results.csv                    # Comprehensive results CSV
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
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import logging
from tqdm import tqdm

from dotenv import load_dotenv
from agents import ModelProvider

from src.workflows.batch_targets import run_batch_targets_workflow

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
        max_tokens: int = 65536,
        target_fields: list = None,
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
        """
        self.output_dir = Path(output_dir)
        self.sample_count = sample_count
        self.batch_size = batch_size
        self.age_file = age_file
        self.model_provider = model_provider
        self.max_tokens = max_tokens
        self.target_fields = target_fields

        # Create timestamped output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.batch_dir = self.output_dir / f"batch_{timestamp}"
        self.batch_dir.mkdir(parents=True, exist_ok=True)

        # Initialize tracking
        self.sample_tracking = {}
        self.failed_samples = []
        self.processed_samples = []

        # Set up logging to file
        log_handler = logging.FileHandler(self.batch_dir / "processing_log.txt")
        log_handler.setLevel(logging.INFO)
        log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        log_handler.setFormatter(log_formatter)
        logger.addHandler(log_handler)

        logger.info("Initialized BatchSamplesProcessor")
        logger.info(f"Output directory: {self.batch_dir}")
        logger.info(f"Sample count: {sample_count}, Batch size: {batch_size}")
        logger.info(f"Max tokens: {self.max_tokens}")
        logger.info(f"Model provider configured: {'Yes' if model_provider else 'No'}")
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
        logger.info(f"Loading samples from {self.age_file}")

        try:
            with open(self.age_file, "r") as f:
                lines = f.readlines()

            # Extract valid GSM IDs
            gsm_samples = []
            for line in lines:
                line = line.strip()
                if line.startswith("GSM") and line[3:].isdigit():
                    gsm_samples.append(line)

            logger.info(
                f"Loaded {len(gsm_samples)} valid GSM samples from {self.age_file}"
            )
            return gsm_samples

        except FileNotFoundError:
            logger.error(f"Age file not found: {self.age_file}")
            raise
        except Exception as e:
            logger.error(f"Error loading age file: {e}")
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

        logger.info(f"Selected {len(selected)} samples for processing")

        # Save selected samples
        with open(self.batch_dir / "selected_samples.json", "w") as f:
            json.dump(selected, f, indent=2)

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

    async def process_batch(
        self, batch_samples: List[str], batch_num: int
    ) -> Dict[str, Any]:
        """
        Process a single batch of samples using batch_targets workflow.

        Parameters
        ----------
        batch_samples : List[str]
            List of GSM samples in this batch
        batch_num : int
            Batch number for tracking

        Returns
        -------
        Dict[str, Any]
            Batch processing results
        """
        logger.info(f"Processing batch {batch_num + 1} with samples: {batch_samples}")

        batch_start_time = time.time()
        input_text = " ".join(batch_samples)

        try:
            # Run batch targets workflow with model provider
            result = await run_batch_targets_workflow(
                input_text=input_text,
                session_id=None,  # Let it generate unique session ID
                sandbox_dir="sandbox",
                model_provider=self.model_provider,
                max_tokens=self.max_tokens,
                max_turns=100,
                enable_parallel_execution=True,
                target_fields=self.target_fields,
            )

            batch_end_time = time.time()
            processing_time = batch_end_time - batch_start_time

            # Extract sandbox session ID from result
            sandbox_id = result.get("session_id", "unknown")

            # Track samples in this batch
            for sample in batch_samples:
                self.sample_tracking[sample] = {
                    "batch_num": batch_num + 1,
                    "sandbox_id": sandbox_id,
                    "status": "completed" if result.get("success", False) else "failed",
                    "processing_time": processing_time
                    / len(batch_samples),  # Average per sample
                    "timestamp": datetime.now().isoformat(),
                }

            if result.get("success", False):
                logger.info(
                    f"Batch {batch_num + 1} completed successfully in {processing_time:.2f}s"
                )
                self.processed_samples.extend(batch_samples)
            else:
                logger.error(
                    f"Batch {batch_num + 1} failed: {result.get('message', 'Unknown error')}"
                )
                self.failed_samples.extend(batch_samples)

            return result

        except Exception as e:
            logger.error(f"Error processing batch {batch_num + 1}: {e}")
            # Mark all samples in batch as failed
            for sample in batch_samples:
                self.sample_tracking[sample] = {
                    "batch_num": batch_num + 1,
                    "sandbox_id": "unknown",
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                }
            self.failed_samples.extend(batch_samples)
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
        logger.info(
            f"Starting metadata extraction for {sample_id} from sandbox {sandbox_id}"
        )
        try:
            # Create sample subdirectory
            sample_dir = self.batch_dir / sample_id
            sample_dir.mkdir(exist_ok=True)

            # Path to sandbox directory
            sandbox_path = Path("sandbox") / sandbox_id

            if not sandbox_path.exists():
                logger.warning(
                    f"Sandbox directory not found for {sample_id}: {sandbox_path}"
                )
                return

            # Load series-sample mapping to find the correct series directory
            mapping_file = sandbox_path / "series_sample_mapping.json"
            if not mapping_file.exists():
                logger.warning(f"No series_sample_mapping.json found in {sandbox_path}")
                return

            with open(mapping_file, "r") as f:
                mapping_data = json.load(f)

            # Find the correct series for this sample
            reverse_mapping = mapping_data.get("reverse_mapping", {})
            series_id = reverse_mapping.get(sample_id)

            if not series_id:
                logger.warning(f"No series found for sample {sample_id} in mapping")
                return

            # Find the correct series directory
            series_dir = sandbox_path / series_id
            if not series_dir.exists():
                logger.warning(
                    f"Series directory not found for {sample_id}: {series_dir}"
                )
                return

            logger.info(f"Found series directory for {sample_id}: {series_dir}")

            # Extract series metadata
            series_file = series_dir / f"{series_dir.name}_metadata.json"
            if series_file.exists():
                with open(series_file, "r") as f:
                    series_data = json.load(f)
                with open(sample_dir / "series_metadata.json", "w") as f:
                    json.dump(series_data, f, indent=2)

            # Extract sample metadata
            sample_file = series_dir / f"{sample_id}_metadata.json"
            logger.info(f"Looking for sample metadata file: {sample_file}")
            if sample_file.exists():
                logger.info(f"Found sample metadata file for {sample_id}")
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

            logger.info(f"Extracted raw metadata for {sample_id}")

        except Exception as e:
            logger.error(f"Error extracting raw metadata for {sample_id}: {e}")

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
            sandbox_path = Path("sandbox") / sandbox_id
            source_file = sandbox_path / "batch_targets_output.json"

            # Create batch_outputs subdirectory
            batch_outputs_dir = self.batch_dir / "batch_outputs"
            batch_outputs_dir.mkdir(exist_ok=True)

            # Create target file with batch number
            target_file = (
                batch_outputs_dir / f"batch_{batch_num:02d}_targets_output.json"
            )

            if not source_file.exists():
                logger.warning(f"No batch_targets_output.json found in {sandbox_path}")
                return

            # Copy the file
            import shutil

            shutil.copy2(source_file, target_file)
            logger.info(
                f"Copied batch_targets_output.json to batch_outputs/batch_{batch_num:02d}_targets_output.json"
            )

        except Exception as e:
            logger.error(f"Error copying batch_targets_output.json: {e}")

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
            sandbox_path = Path("sandbox") / sandbox_id
            source_file = sandbox_path / "data_intake_output.json"

            # Create batch_outputs subdirectory
            batch_outputs_dir = self.batch_dir / "batch_outputs"
            batch_outputs_dir.mkdir(exist_ok=True)

            # Create target file with batch number
            target_file = (
                batch_outputs_dir / f"batch_{batch_num:02d}_data_intake_output.json"
            )

            if not source_file.exists():
                logger.warning(f"No data_intake_output.json found in {sandbox_path}")
                return

            # Copy the file
            import shutil

            shutil.copy2(source_file, target_file)
            logger.info(
                f"Copied data_intake_output.json to batch_outputs/batch_{batch_num:02d}_data_intake_output.json"
            )

        except Exception as e:
            logger.error(f"Error copying data_intake_output.json: {e}")

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
            sandbox_path = Path("sandbox") / sandbox_id
            results_file = sandbox_path / "batch_targets_output.json"

            if not results_file.exists():
                logger.warning(f"No batch_targets_output.json found in {sandbox_path}")
                return {}

            with open(results_file, "r") as f:
                batch_data = json.load(f)

            # Check for the new structure first (sample_results)
            if "sample_results" in batch_data:
                logger.info(f"Using new batch_targets structure for {sandbox_id}")
                sample_results = batch_data["sample_results"]

                # Enhance with actual normalization data from individual output files
                self._enhance_with_normalization_data(sandbox_path, sample_results)

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
        Generate comprehensive CSV with all curated and normalized data.
        """
        logger.info("Generating comprehensive results CSV")

        csv_file = self.batch_dir / "batch_results.csv"

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
            # Processing metadata
            "sources_processed",
            "reconciliation_needed",
            "reconciliation_reason",
            "total_candidates_found",
            "processing_timestamp",
        ]

        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()

            # Process each tracked sample
            for sample_id, tracking_info in self.sample_tracking.items():
                row = self._create_csv_row(sample_id, tracking_info, columns)
                writer.writerow(row)

        logger.info(f"Generated comprehensive CSV: {csv_file}")

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
        # Initialize row with empty values
        row = {col: "" for col in columns}

        # Basic sample info
        row["sample_id"] = sample_id
        row["sandbox_id"] = tracking_info.get("sandbox_id", "")
        row["batch_num"] = tracking_info.get("batch_num", "")
        row["processing_status"] = tracking_info.get("status", "")
        row["processing_time"] = tracking_info.get("processing_time", "")
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
            row[f"{field}_confidence"] = best_candidate.get("confidence", "")
            row[f"{field}_context"] = best_candidate.get("context", "")
            row[f"{field}_rationale"] = best_candidate.get("rationale", "")
            row[f"{field}_prenormalized"] = best_candidate.get("prenormalized", "")

        # Add metadata
        row["sources_processed"] = ", ".join(
            field_curation.get("sources_processed", [])
        )
        row["reconciliation_needed"] = field_curation.get("reconciliation_needed", "")
        row["reconciliation_reason"] = field_curation.get("reconciliation_reason", "")
        row["total_candidates_found"] = field_curation.get("candidate_count", "")

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
        Save sample tracking and failed samples data.
        """
        # Save sample tracking
        with open(self.batch_dir / "sample_tracking.json", "w") as f:
            json.dump(self.sample_tracking, f, indent=2)

        # Save failed samples
        if self.failed_samples:
            with open(self.batch_dir / "failed_samples.json", "w") as f:
                json.dump(self.failed_samples, f, indent=2)

        # Save processing summary
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

        with open(self.batch_dir / "processing_summary.json", "w") as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Processing summary: {summary}")

    async def run(self) -> None:
        """
        Run the complete batch samples processing workflow.
        """
        start_time = time.time()
        logger.info("Starting batch samples processing workflow")

        try:
            # Step 1: Load and select samples
            all_samples = self.load_age_samples()
            selected_samples = self.select_random_samples(all_samples)

            # Step 2: Create batches
            batches = self.create_batches(selected_samples)

            # Step 3: Process batches
            with tqdm(
                total=len(batches), desc="Processing batches", unit="batch"
            ) as pbar:
                for batch_num, batch_samples in enumerate(batches):
                    batch_result = await self.process_batch(batch_samples, batch_num)

                    # Extract raw metadata for successful samples
                    if batch_result.get("success", False):
                        sandbox_id = batch_result.get("session_id", "unknown")
                        for sample_id in batch_samples:
                            self.extract_raw_metadata(sample_id, sandbox_id)

                        # Copy batch_targets_output.json to batch_outputs subdirectory
                        self.copy_batch_targets_output(sandbox_id, batch_num + 1)

                        # Copy data_intake_output.json to batch_outputs subdirectory
                        self.copy_data_intake_output(sandbox_id, batch_num + 1)

                    pbar.update(1)
                    pbar.set_postfix(
                        {"Completed": batch_num + 1, "Total": len(batches)}
                    )

                    # Save progress periodically
                    if (batch_num + 1) % 5 == 0:
                        self.save_tracking_data()

            # Step 4: Generate outputs
            self.generate_comprehensive_csv()
            self.save_tracking_data()

            end_time = time.time()
            total_time = end_time - start_time

            logger.info(f"Batch processing completed in {total_time:.2f} seconds")
            logger.info(
                f"Successfully processed: {len(self.processed_samples)} samples"
            )
            logger.info(f"Failed samples: {len(self.failed_samples)}")
            logger.info(f"Output directory: {self.batch_dir}")

        except Exception as e:
            logger.error(f"Error in batch processing workflow: {e}")
            raise


async def run_batch_samples_workflow(
    sample_count: int = 100,
    batch_size: int = 5,
    output_dir: str = "batch",
    age_file: str = "Age.txt",
    model_provider: ModelProvider = None,
    max_tokens: int = 65536,
    target_fields: list = None,
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
