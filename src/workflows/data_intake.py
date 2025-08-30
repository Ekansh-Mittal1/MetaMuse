"""
Data intake workflow for GEO metadata extraction and linking.

This module provides a data intake workflow that replicates the functionality
of the IngestionAgent and LinkerAgent without using the agents SDK. It directly
calls the underlying tools in a predefined sequence.
"""

import json
import os
import re
import sys
import argparse
from pathlib import Path
from typing import Dict, List
from dotenv import load_dotenv
try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable

# Add the project root to Python path when running this file directly
if __name__ == "__main__":
    # Get the project root directory (two levels up from this file)
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

# Import new Pydantic models
from src.models import WorkflowResult
from src.models.agent_outputs import LinkerOutput, create_successful_linker_output
from src.models.metadata_models import (
    CleanedSeriesMetadata,
    CleanedSampleMetadata,
    CleanedAbstractMetadata,
)
from src.models.curation_models import CurationDataPackage

# Import the tool implementations
from src.tools.ingestion_tools import (
    extract_gsm_metadata_impl,
    extract_gse_metadata_impl,
    extract_paper_abstract_impl,
    extract_pubmed_id_from_gse_metadata_impl,
    extract_series_id_from_gsm_metadata_impl,
    create_series_sample_mapping_impl,
    validate_geo_inputs_impl,
)

from src.tools.linker_tools import (
    load_mapping_file_impl,
    find_sample_directory_impl,
    clean_metadata_files_impl,
    package_linked_data_impl,
    create_curation_data_package_impl,
)

# Load environment variables
load_dotenv()


class DataIntakeWorkflow:
    """
    Data intake workflow that replicates IngestionAgent and LinkerAgent functionality.

    This workflow directly calls the underlying tools in a predefined sequence
    without using the agents SDK, providing the same results with deterministic behavior.
    """

    def __init__(self, session_id: str, sandbox_dir: str = "sandbox", create_series_directories: bool = True):
        """
        Initialize the data intake workflow.

        Parameters
        ----------
        session_id : str
            The unique session identifier
        sandbox_dir : str
            Base sandbox directory
        create_series_directories : bool
            Whether to create GSE* series directories during processing
        """
        import time

        self.session_id = session_id
        self.sandbox_dir = sandbox_dir
        self.create_series_directories = create_series_directories
        
        # For unified discovery structure, use sandbox_dir directly
        if session_id == "discovery":
            self.session_dir = Path(sandbox_dir)
        else:
            self.session_dir = Path(sandbox_dir) / session_id
            self.session_dir.mkdir(parents=True, exist_ok=True)
        self._start_time = time.time()

        # Validate required environment variables
        required_env_vars = {
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
            print("   NCBI_EMAIL=your_email@example.com")
            print("   NCBI_API_KEY=your_ncbi_api_key  # Optional but recommended")
            print("=" * 60)
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

        # Get environment variables
        self.email = os.getenv("NCBI_EMAIL")
        self.api_key = os.getenv("NCBI_API_KEY")

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

    def _load_cleaned_metadata(self, sample_ids: List[str]) -> Dict:
        """
        Load cleaned metadata files for the given sample IDs.

        Parameters
        ----------
        sample_ids : List[str]
            List of sample IDs to load metadata for

        Returns
        -------
        Dict
            Dictionary containing cleaned metadata organized by type
        """
        cleaned_series_metadata = {}
        cleaned_sample_metadata = {}
        cleaned_abstract_metadata = {}

        for sample_id in sample_ids:
            try:
                # Find the sample directory
                sample_dir_result = find_sample_directory_impl(
                    sample_id, str(self.session_dir)
                )
                if isinstance(sample_dir_result, str):
                    sample_dir_data = json.loads(sample_dir_result)
                else:
                    sample_dir_data = sample_dir_result

                if not sample_dir_data.get("success", False):
                    print(
                        f"⚠️  Could not find directory for {sample_id}: {sample_dir_data.get('message', 'Unknown error')}"
                    )
                    continue

                # The directory is in the data field
                sample_dir = Path(sample_dir_data["data"]["directory"])

                # Look for cleaned metadata files
                cleaned_files = list(sample_dir.glob("*_cleaned_*.json"))

                for cleaned_file in cleaned_files:
                    try:
                        with open(cleaned_file, "r", encoding="utf-8") as f:
                            metadata_data = json.load(f)

                        # Determine the type based on filename
                        if "series" in cleaned_file.name.lower():
                            if "series_id" in metadata_data:
                                series_id = metadata_data["series_id"]
                                cleaned_series_metadata[series_id] = (
                                    CleanedSeriesMetadata(**metadata_data)
                                )
                        elif "sample" in cleaned_file.name.lower():
                            if "sample_id" in metadata_data:
                                sample_id_from_file = metadata_data["sample_id"]
                                cleaned_sample_metadata[sample_id_from_file] = (
                                    CleanedSampleMetadata(**metadata_data)
                                )
                        elif (
                            "abstract" in cleaned_file.name.lower()
                            or "pmid" in cleaned_file.name.lower()
                        ):
                            if "pmid" in metadata_data:
                                pmid = metadata_data["pmid"]
                                cleaned_abstract_metadata[pmid] = (
                                    CleanedAbstractMetadata(**metadata_data)
                                )

                    except Exception as e:
                        print(
                            f"⚠️  Error loading cleaned metadata file {cleaned_file}: {e}"
                        )
                        continue

            except Exception as e:
                print(f"⚠️  Error processing sample {sample_id}: {e}")
                continue

        return {
            "cleaned_series_metadata": cleaned_series_metadata
            if cleaned_series_metadata
            else None,
            "cleaned_sample_metadata": cleaned_sample_metadata
            if cleaned_sample_metadata
            else None,
            "cleaned_abstract_metadata": cleaned_abstract_metadata
            if cleaned_abstract_metadata
            else None,
        }

    def _create_curation_packages(
        self,
        sample_ids: List[str],
        cleaned_metadata: Dict,
        fields_to_remove: List[str] = None,
    ) -> List[CurationDataPackage]:
        """
        Create CurationDataPackage objects for the given sample IDs.

        Parameters
        ----------
        sample_ids : List[str]
            List of sample IDs to create packages for
        cleaned_metadata : Dict
            Dictionary containing cleaned metadata organized by type
        fields_to_remove : List[str], optional
            Fields that were removed during cleaning

        Returns
        -------
        List[CurationDataPackage]
            List of CurationDataPackage objects
        """
        curation_packages = []

        for sample_id in sample_ids:
            try:
                # Use the implementation from the tool
                result = create_curation_data_package_impl(
                    sample_id, str(self.session_dir), fields_to_remove
                )

                if isinstance(result, str):
                    result_data = json.loads(result)
                else:
                    result_data = result

                if result_data.get("success", False) and "data" in result_data:
                    data_field = result_data["data"]
                    if "curation_package" in data_field:
                        package_data = data_field["curation_package"]
                        curation_package = CurationDataPackage(**package_data)
                        curation_packages.append(curation_package)
                        # Created CurationDataPackage successfully
                    else:
                        print(
                            f"⚠️  Failed to create CurationDataPackage for {sample_id}: No curation_package in data field"
                        )
                else:
                    print(
                        f"⚠️  Failed to create CurationDataPackage for {sample_id}: {result_data.get('message', 'Unknown error')}"
                    )

            except Exception as e:
                print(f"⚠️  Error creating CurationDataPackage for {sample_id}: {e}")
                continue

        return curation_packages

    def _parse_geo_ids(self, input_text: str) -> Dict[str, List[str]]:
        """
        Parse GEO IDs from input text.

        Parameters
        ----------
        input_text : str
            Input text containing GEO IDs

        Returns
        -------
        Dict[str, List[str]]
            Dictionary with 'gsm_ids', 'gse_ids', and 'pmid_ids' lists
        """
        # Extract GSM IDs (GSM followed by numbers)
        gsm_ids = re.findall(r"GSM\d+", input_text.upper())

        # Extract GSE IDs (GSE followed by numbers)
        gse_ids = re.findall(r"GSE\d+", input_text.upper())

        # Extract PMID IDs (PMID followed by numbers)
        pmid_ids = re.findall(r"PMID(\d+)", input_text.upper())
        # Convert to integers
        pmid_ids = [int(pmid) for pmid in pmid_ids]

        return {"gsm_ids": gsm_ids, "gse_ids": gse_ids, "pmid_ids": pmid_ids}

    def _validate_inputs(self, geo_ids: Dict[str, List[str]]) -> WorkflowResult:
        """
        Validate the parsed GEO IDs.

        Parameters
        ----------
        geo_ids : Dict[str, List[str]]
            Dictionary containing parsed GEO IDs

        Returns
        -------
        WorkflowResult
            Validation result
        """
        try:
            # Validate each type of ID
            for gsm_id in geo_ids["gsm_ids"]:
                result = validate_geo_inputs_impl(
                    gsm_id=gsm_id, email=self.email, api_key=self.api_key
                )
                result_data = json.loads(result)
                if result_data["validation_status"] == "failed":
                    return WorkflowResult(
                        success=False,
                        message=f"Validation failed for GSM ID {gsm_id}",
                        errors=result_data["errors"],
                    )

            for gse_id in geo_ids["gse_ids"]:
                result = validate_geo_inputs_impl(
                    gse_id=gse_id, email=self.email, api_key=self.api_key
                )
                result_data = json.loads(result)
                if result_data["validation_status"] == "failed":
                    return WorkflowResult(
                        success=False,
                        message=f"Validation failed for GSE ID {gse_id}",
                        errors=result_data["errors"],
                    )

            for pmid in geo_ids["pmid_ids"]:
                result = validate_geo_inputs_impl(
                    pmid=pmid, email=self.email, api_key=self.api_key
                )
                result_data = json.loads(result)
                if result_data["validation_status"] == "failed":
                    return WorkflowResult(
                        success=False,
                        message=f"Validation failed for PMID {pmid}",
                        errors=result_data["errors"],
                    )

            return WorkflowResult(
                success=True, message="All inputs validated successfully", data=geo_ids
            )

        except Exception as e:
            return WorkflowResult(
                success=False, message=f"Validation error: {str(e)}", errors=[str(e)]
            )

    def _extract_gsm_workflow(self, gsm_id: str) -> WorkflowResult:
        """
        Execute the 6-step GSM workflow.

        Parameters
        ----------
        gsm_id : str
            GSM ID to process

        Returns
        -------
        WorkflowResult
            Workflow execution result
        """
        try:
            files_created = []
            workflow_data = {"gsm_id": gsm_id}

            # Starting GSM workflow

            # Step 1: Extract GSM metadata

            gsm_file = extract_gsm_metadata_impl(
                gsm_id, str(self.session_dir), self.email, self.api_key, self.create_series_directories
            )
            files_created.append(gsm_file)
            workflow_data["gsm_metadata_file"] = gsm_file

            # Step 2: Extract Series ID from GSM metadata

            series_result = extract_series_id_from_gsm_metadata_impl(
                gsm_file, str(self.session_dir)
            )
            series_data = json.loads(series_result)
            series_id = series_data.get("series_id")
            if not series_id:
                return WorkflowResult(
                    success=False,
                    message=f"No series ID found for {gsm_id}",
                    errors=[f"Series ID extraction failed for {gsm_id}"],
                )
            workflow_data["series_id"] = series_id

            # Step 3: Extract GSE metadata

            gse_file = extract_gse_metadata_impl(
                series_id, str(self.session_dir), self.email, self.api_key, self.create_series_directories
            )
            files_created.append(gse_file)
            workflow_data["gse_metadata_file"] = gse_file

            # Step 4: Extract PubMed ID from GSE metadata

            pmid_result = extract_pubmed_id_from_gse_metadata_impl(
                gse_file, str(self.session_dir)
            )
            pmid_data = json.loads(pmid_result)
            pmid = pmid_data.get("pubmed_id")
            if pmid:
                workflow_data["pmid"] = pmid

                # Step 5: Extract paper abstract

                try:
                    paper_file = extract_paper_abstract_impl(
                        pmid, str(self.session_dir), self.email, self.api_key, gse_file, self.create_series_directories
                    )
                    files_created.append(paper_file)
                    workflow_data["paper_metadata_file"] = paper_file
                except Exception as e:
                    print(
                        f"⚠️  Warning: Failed to extract paper abstract for PMID {pmid}: {e}"
                    )
                    print("⚠️  Continuing workflow without paper abstract...")
                    workflow_data["paper_extraction_error"] = str(e)


            # Step 6: Create series-sample mapping

            mapping_file = create_series_sample_mapping_impl(str(self.session_dir))
            files_created.append(mapping_file)
            workflow_data["mapping_file"] = mapping_file

            return WorkflowResult(
                success=True,
                message=f"GSM workflow completed successfully for {gsm_id}",
                data=workflow_data,
                files_created=files_created,
            )

        except Exception as e:
            return WorkflowResult(
                success=False,
                message=f"GSM workflow failed for {gsm_id}: {str(e)}",
                errors=[str(e)],
            )

    def _extract_gse_workflow(self, gse_id: str) -> WorkflowResult:
        """
        Execute the 4-step GSE workflow.

        Parameters
        ----------
        gse_id : str
            GSE ID to process

        Returns
        -------
        WorkflowResult
            Workflow execution result
        """
        try:
            files_created = []
            workflow_data = {"gse_id": gse_id}

            # Starting GSE workflow

            # Step 1: Extract GSE metadata

            gse_file = extract_gse_metadata_impl(
                gse_id, str(self.session_dir), self.email, self.api_key
            )
            files_created.append(gse_file)
            workflow_data["gse_metadata_file"] = gse_file

            # Step 2: Extract PubMed ID from GSE metadata

            pmid_result = extract_pubmed_id_from_gse_metadata_impl(
                gse_file, str(self.session_dir)
            )
            pmid_data = json.loads(pmid_result)
            pmid = pmid_data.get("pubmed_id")
            if pmid:
                workflow_data["pmid"] = pmid

                # Step 3: Extract paper abstract

                try:
                    paper_file = extract_paper_abstract_impl(
                        pmid, str(self.session_dir), self.email, self.api_key, gse_file, self.create_series_directories
                    )
                    files_created.append(paper_file)
                    workflow_data["paper_metadata_file"] = paper_file
                except Exception as e:
                    print(
                        f"⚠️  Warning: Failed to extract paper abstract for PMID {pmid}: {e}"
                    )
                    print("⚠️  Continuing workflow without paper abstract...")
                    workflow_data["paper_extraction_error"] = str(e)
            else:
                pass

            # Step 4: Create series-sample mapping

            mapping_file = create_series_sample_mapping_impl(str(self.session_dir))
            files_created.append(mapping_file)
            workflow_data["mapping_file"] = mapping_file

            return WorkflowResult(
                success=True,
                message=f"GSE workflow completed successfully for {gse_id}",
                data=workflow_data,
                files_created=files_created,
            )

        except Exception as e:
            return WorkflowResult(
                success=False,
                message=f"GSE workflow failed for {gse_id}: {str(e)}",
                errors=[str(e)],
            )

    def _extract_pmid_workflow(self, pmid: int) -> WorkflowResult:
        """
        Execute the PMID workflow.

        Parameters
        ----------
        pmid : int
            PMID to process

        Returns
        -------
        WorkflowResult
            Workflow execution result
        """
        try:
            files_created = []
            workflow_data = {"pmid": pmid}

            # Starting PMID workflow

            # Extract paper abstract

            try:
                paper_file = extract_paper_abstract_impl(
                    pmid, str(self.session_dir), self.email, self.api_key, None, self.create_series_directories
                )
                files_created.append(paper_file)
                workflow_data["paper_metadata_file"] = paper_file
            except Exception as e:
                print(
                    f"⚠️  Warning: Failed to extract paper abstract for PMID {pmid}: {e}"
                )
                return WorkflowResult(
                    success=False,
                    message=f"PMID workflow failed for {pmid}: {str(e)}",
                    errors=[str(e)],
                )

            return WorkflowResult(
                success=True,
                message=f"PMID workflow completed successfully for {pmid}",
                data=workflow_data,
                files_created=files_created,
            )

        except Exception as e:
            return WorkflowResult(
                success=False,
                message=f"PMID workflow failed for {pmid}: {str(e)}",
                errors=[str(e)],
            )

    def _link_sample_data(
        self, sample_id: str, fields_to_remove: List[str] = None
    ) -> WorkflowResult:
        """
        Execute the linker workflow for a single sample.

        Parameters
        ----------
        sample_id : str
            Sample ID to process
        fields_to_remove : List[str], optional
            Fields to remove during cleaning

        Returns
        -------
        WorkflowResult
            Linker workflow result
        """
        try:
            # Starting linker workflow

            # Step 1: Load mapping file

            mapping_result = load_mapping_file_impl(str(self.session_dir))
            if not mapping_result["success"]:
                return WorkflowResult(
                    success=False,
                    message=f"Failed to load mapping file: {mapping_result['message']}",
                    errors=[mapping_result["message"]],
                )

            # Step 2: Find sample directory

            dir_result = find_sample_directory_impl(sample_id, str(self.session_dir))
            if not dir_result["success"]:
                return WorkflowResult(
                    success=False,
                    message=f"Failed to find directory for {sample_id}: {dir_result['message']}",
                    errors=[dir_result["message"]],
                )

            # Step 3: Clean metadata files

            clean_result = clean_metadata_files_impl(
                sample_id, str(self.session_dir), fields_to_remove
            )
            if not clean_result["success"]:
                return WorkflowResult(
                    success=False,
                    message=f"Failed to clean metadata for {sample_id}: {clean_result['message']}",
                    errors=[clean_result["message"]],
                )

            # Step 4: Package linked data

            package_result = package_linked_data_impl(
                sample_id, str(self.session_dir), fields_to_remove
            )
            if not package_result["success"]:
                return WorkflowResult(
                    success=False,
                    message=f"Failed to package data for {sample_id}: {package_result['message']}",
                    errors=[package_result["message"]],
                )

            return WorkflowResult(
                success=True,
                message=f"Linker workflow completed successfully for {sample_id}",
                data={
                    "sample_id": sample_id,
                    "cleaned_files": clean_result.get("files_created", []),
                    "packaged_data": package_result.get("data", {}),
                },
                files_created=clean_result.get("files_created", [])
                + package_result.get("files_created", []),
            )

        except Exception as e:
            return WorkflowResult(
                success=False,
                message=f"Linker workflow failed for {sample_id}: {str(e)}",
                errors=[str(e)],
            )

    def run_ingestion_workflow(self, input_text: str) -> WorkflowResult:
        """
        Run the complete ingestion workflow.

        Parameters
        ----------
        input_text : str
            Input text containing GEO IDs

        Returns
        -------
        WorkflowResult
            Complete ingestion workflow result
        """
        try:
            # Starting ingestion workflow

            # Parse GEO IDs
            geo_ids = self._parse_geo_ids(input_text)

            # Validate inputs
            validation_result = self._validate_inputs(geo_ids)
            if not validation_result.success:
                return validation_result

            all_files_created = []
            all_workflow_data = []
            all_sample_ids = []

            # Process GSM IDs
            for gsm_id in tqdm(geo_ids["gsm_ids"], desc="Data Intake - GSM", unit="gsm"):
                result = self._extract_gsm_workflow(gsm_id)
                if not result.success:
                    return result
                all_files_created.extend(result.files_created or [])
                all_workflow_data.append(result.data)
                all_sample_ids.append(gsm_id)

            # Process GSE IDs
            for gse_id in tqdm(geo_ids["gse_ids"], desc="Data Intake - GSE", unit="gse"):
                result = self._extract_gse_workflow(gse_id)
                if not result.success:
                    return result
                all_files_created.extend(result.files_created or [])
                all_workflow_data.append(result.data)
                # Extract sample IDs from GSE if available
                if result.data and "series_id" in result.data:
                    # Try to get sample IDs from the mapping file
                    try:
                        mapping_result = load_mapping_file_impl(str(self.session_dir))
                        if mapping_result["success"] and "data" in mapping_result:
                            mapping_data = mapping_result["data"]
                            series_id = result.data["series_id"]
                            if (
                                "mapping" in mapping_data
                                and series_id in mapping_data["mapping"]
                            ):
                                series_sample_ids = mapping_data["mapping"][series_id][
                                    "sample_ids"
                                ]
                                all_sample_ids.extend(series_sample_ids)
                    except Exception as e:
                        print(
                            f"Warning: Could not extract sample IDs from {gse_id}: {e}"
                        )

            # Process PMID IDs
            for pmid in tqdm(geo_ids["pmid_ids"], desc="Data Intake - PMID", unit="pmid"):
                result = self._extract_pmid_workflow(pmid)
                if not result.success:
                    return result
                all_files_created.extend(result.files_created or [])
                all_workflow_data.append(result.data)

            return WorkflowResult(
                success=True,
                message=f"Ingestion workflow completed successfully. Processed {len(geo_ids['gsm_ids'])} GSM, {len(geo_ids['gse_ids'])} GSE, {len(geo_ids['pmid_ids'])} PMID",
                data={
                    "geo_ids": geo_ids,
                    "workflow_data": all_workflow_data,
                    "sample_ids": all_sample_ids,
                    "session_dir": str(self.session_dir),
                },
                files_created=all_files_created,
            )

        except Exception as e:
            return WorkflowResult(
                success=False,
                message=f"Ingestion workflow failed: {str(e)}",
                errors=[str(e)],
            )

    def run_linker_workflow(
        self, sample_ids: List[str], fields_to_remove: List[str] = None
    ) -> WorkflowResult:
        """
        Run the complete linker workflow for multiple samples.

        Parameters
        ----------
        sample_ids : List[str]
            List of sample IDs to process
        fields_to_remove : List[str], optional
            Fields to remove during cleaning

        Returns
        -------
        WorkflowResult
            Complete linker workflow result
        """
        try:
            all_results = []
            all_files_created = []

            for sample_id in tqdm(sample_ids, desc="Data Intake - Linking samples", unit="sample"):
                result = self._link_sample_data(sample_id, fields_to_remove)
                if not result.success:
                    return result
                all_results.append(result.data)
                all_files_created.extend(result.files_created or [])

            return WorkflowResult(
                success=True,
                message=f"Linker workflow completed successfully for {len(sample_ids)} samples",
                data={
                    "sample_results": all_results,
                    "sample_ids": sample_ids,
                    "session_dir": str(self.session_dir),
                },
                files_created=all_files_created,
            )

        except Exception as e:
            return WorkflowResult(
                success=False,
                message=f"Linker workflow failed: {str(e)}",
                errors=[str(e)],
            )

    def run_complete_workflow(
        self, input_text: str, fields_to_remove: List[str] = None
    ) -> LinkerOutput:
        """
        Run the complete workflow (ingestion + linking).

        Parameters
        ----------
        input_text : str
            Input text containing GEO IDs
        fields_to_remove : List[str], optional
            Fields to remove during cleaning

        Returns
        -------
        LinkerOutput
            Complete workflow result as LinkerOutput object
        """
        try:
            # Starting complete workflow

            # Parse GEO IDs
            geo_ids = self._parse_geo_ids(input_text)

            # Validate inputs
            validation_result = self._validate_inputs(geo_ids)
            if not validation_result.success:
                return validation_result

            all_files_created = []
            all_workflow_data = []
            all_sample_ids = []

            # Process GSM IDs
            for gsm_id in tqdm(geo_ids["gsm_ids"], desc="Data Intake - GSM", unit="gsm"):
                result = self._extract_gsm_workflow(gsm_id)
                if not result.success:
                    return result
                all_files_created.extend(result.files_created or [])
                all_workflow_data.append(result.data)
                all_sample_ids.append(gsm_id)

            # Process GSE IDs
            for gse_id in tqdm(geo_ids["gse_ids"], desc="Data Intake - GSE", unit="gse"):
                result = self._extract_gse_workflow(gse_id)
                if not result.success:
                    return result
                all_files_created.extend(result.files_created or [])
                all_workflow_data.append(result.data)
                # Extract sample IDs from GSE if available
                if result.data and "series_id" in result.data:
                    # Try to get sample IDs from the mapping file
                    try:
                        mapping_result = load_mapping_file_impl(str(self.session_dir))
                        if mapping_result["success"] and "data" in mapping_result:
                            mapping_data = mapping_result["data"]
                            series_id = result.data["series_id"]
                            if (
                                "mapping" in mapping_data
                                and series_id in mapping_data["mapping"]
                            ):
                                series_sample_ids = mapping_data["mapping"][series_id][
                                    "sample_ids"
                                ]
                                all_sample_ids.extend(series_sample_ids)
                    except Exception as e:
                        print(
                            f"Warning: Could not extract sample IDs from {gse_id}: {e}"
                        )

            # Process PMID IDs
            for pmid in tqdm(geo_ids["pmid_ids"], desc="Data Intake - PMID", unit="pmid"):
                result = self._extract_pmid_workflow(pmid)
                if not result.success:
                    return result
                all_files_created.extend(result.files_created or [])
                all_workflow_data.append(result.data)

            # Ensure we use the same field removal list as the full_pipeline
            if fields_to_remove is None:
                fields_to_remove = [
                    # GSE and GSM fields to remove from attributes
                    "status",
                    "submission_date",
                    "last_update_date",
                    "contributor",
                    # Contact fields
                    "contact_name",
                    "contact_email",
                    "contact_laboratory",
                    "contact_department",
                    "contact_institute",
                    "contact_address",
                    "contact_city",
                    "contact_state",
                    "contact_zip/postal_code",
                    "contact_country",
                    "contact_phone",
                    "contact_fax",
                    # Protocol and processing fields
                    # PMID fields to remove
                    "authors",
                    "journal",
                    "publication_date",
                    "keywords",
                    "mesh_terms",
                ]

            # Run linker workflow
            linker_result = self.run_linker_workflow(all_sample_ids, fields_to_remove)
            if not linker_result.success:
                return linker_result

            # Combine results
            all_files_created = (all_files_created) + (
                linker_result.files_created or []
            )

            # Load cleaned metadata
            cleaned_metadata = self._load_cleaned_metadata(all_sample_ids)

            # Create CurationDataPackages for CuratorAgent handoff
            curation_packages = self._create_curation_packages(
                all_sample_ids, cleaned_metadata, fields_to_remove
            )

            # Create LinkerOutput object
            import time

            execution_time = time.time() - self._start_time

            return create_successful_linker_output(
                sample_ids=all_sample_ids,
                session_dir=str(self.session_dir),
                execution_time=execution_time,
                successfully_linked=all_sample_ids,
                sample_ids_for_curation=all_sample_ids,
                recommended_curation_fields=["Disease", "Tissue", "Age", "Organ"],
                fields_removed_during_cleaning=fields_to_remove or [],
                files_created=all_files_created,
                warnings=[],
                cleaned_series_metadata=cleaned_metadata["cleaned_series_metadata"],
                cleaned_sample_metadata=cleaned_metadata["cleaned_sample_metadata"],
                cleaned_abstract_metadata=cleaned_metadata["cleaned_abstract_metadata"],
                curation_packages=curation_packages,
            )

        except Exception as e:
            import time

            execution_time = time.time() - self._start_time

            return LinkerOutput(
                success=False,
                message=f"Complete workflow failed: {str(e)}",
                execution_time_seconds=execution_time,
                sample_ids_requested=[],
                session_directory=str(self.session_dir),
                files_created=[],
                successfully_linked=[],
                failed_linking=[],
                warnings=[str(e)],
                sample_ids_for_curation=[],
                recommended_curation_fields=[],
                fields_removed_during_cleaning=[],
                linked_data=None,
                cleaned_metadata_files=None,
                cleaned_series_metadata=None,
                cleaned_sample_metadata=None,
                cleaned_abstract_metadata=None,
            )


def run_data_intake_workflow(
    input_text: str,
    session_id: str = None,
    sandbox_dir: str = "sandbox",
    fields_to_remove: List[str] = None,
    workflow_type: str = "complete",
    create_series_directories: bool = True,
) -> LinkerOutput:
    """
    Run the data intake workflow.

    Parameters
    ----------
    input_text : str
        Input text containing GEO IDs
    session_id : str, optional
        Session ID (generated if not provided)
    sandbox_dir : str
        Base sandbox directory
    fields_to_remove : List[str], optional
        Fields to remove during cleaning
    workflow_type : str
        Type of workflow: "ingestion", "linker", or "complete"

    Returns
    -------
    LinkerOutput
        Workflow execution result as LinkerOutput object
    """
    import uuid

    if session_id is None:
        # Generate session ID with pipeline prefix
        pipeline_prefixes = {
            "ingestion": "di_ing",
            "linker": "di_link",
            "complete": "di",
        }

        prefix = pipeline_prefixes.get(workflow_type, "di_unknown")
        session_id = f"{prefix}_{str(uuid.uuid4())}"

    workflow = DataIntakeWorkflow(session_id, sandbox_dir, create_series_directories=create_series_directories)

    if workflow_type == "ingestion":
        return workflow.run_ingestion_workflow(input_text)
    elif workflow_type == "linker":
        # For linker workflow, input_text should be a list of sample IDs
        sample_ids = [s.strip() for s in input_text.split(",") if s.strip()]
        return workflow.run_linker_workflow(sample_ids, fields_to_remove)
    elif workflow_type == "complete":
        return workflow.run_complete_workflow(input_text, fields_to_remove)
    else:
        return LinkerOutput(
            success=False,
            message=f"Invalid workflow type: {workflow_type}",
            execution_time_seconds=0.0,
            sample_ids_requested=[],
            session_directory=str(Path(sandbox_dir) if session_id == "discovery" else Path(sandbox_dir) / session_id) if session_id else "",
            files_created=[],
            successfully_linked=[],
            failed_linking=[],
            warnings=[
                f"Invalid workflow type: {workflow_type}. Supported types: ingestion, linker, complete"
            ],
            sample_ids_for_curation=[],
            recommended_curation_fields=[],
            fields_removed_during_cleaning=[],
            linked_data=None,
            cleaned_metadata_files=None,
        )


def print_result(result: WorkflowResult):
    """Print the workflow result in a formatted way."""
    print("\n" + "=" * 60)
    print("WORKFLOW RESULT")
    print("=" * 60)
    print(f"Success: {result.success}")
    print(f"Message: {result.message}")

    if result.errors:
        print("\nErrors:")
        for error in result.errors:
            print(f"  - {error}")

    if result.data:
        print("\nData Summary:")
        if "geo_ids" in result.data:
            geo_ids = result.data["geo_ids"]
            print(f"  - GSM IDs: {geo_ids.get('gsm_ids', [])}")
            print(f"  - GSE IDs: {geo_ids.get('gse_ids', [])}")
            print(f"  - PMID IDs: {geo_ids.get('pmid_ids', [])}")

        if "sample_ids" in result.data:
            print(f"  - Sample IDs: {result.data['sample_ids']}")

        if "session_dir" in result.data:
            print(f"  - Session Directory: {result.data['session_dir']}")

    if result.files_created:
        print(f"\nFiles Created ({len(result.files_created)}):")
        for file_path in result.files_created:
            print(f"  - {file_path}")

    print("=" * 60)


def main():
    """Command-line interface for the data intake workflow."""
    parser = argparse.ArgumentParser(
        description="Data intake workflow for GEO metadata extraction and linking",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Complete workflow (ingestion + linking)
  python data_intake.py --input "Extract metadata for GSM1000981, GSE41588" --type complete

  # Ingestion only
  python data_intake.py --input "Extract metadata for GSM1000981" --type ingestion

  # Linker only (requires existing session)
  python data_intake.py --input "GSM1000981, GSM1098372" --type linker --session existing-session

  # With custom fields to remove
  python data_intake.py --input "Extract metadata for GSM1000981" --type complete --remove-fields status submission_date last_update_date

  # With custom session and sandbox
  python data_intake.py --input "Extract metadata for GSE41588" --type complete --session my-session --sandbox custom-sandbox
        """,
    )

    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Input text containing GEO IDs (GSM, GSE, PMID) or comma-separated sample IDs for linker workflow",
    )

    parser.add_argument(
        "--type",
        "-t",
        choices=["ingestion", "linker", "complete"],
        default="complete",
        help="Type of workflow to run (default: complete)",
    )

    parser.add_argument(
        "--session", "-s", help="Session ID (generated automatically if not provided)"
    )

    parser.add_argument(
        "--sandbox",
        "-b",
        default="sandbox",
        help="Base sandbox directory (default: sandbox)",
    )

    parser.add_argument(
        "--remove-fields",
        "-r",
        nargs="+",
        help="Fields to remove during cleaning (default: status, submission_date, last_update_date, etc.)",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )

    parser.add_argument("--json", action="store_true", help="Output result as JSON")

    args = parser.parse_args()

    # Validate required environment variables early
    required_env_vars = {
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

    # Set up logging
    if args.verbose:
        # Data Intake Workflow
        print(f"Input: {args.input}")
        print(f"Type: {args.type}")
        print(f"Session: {args.session or 'auto-generated'}")
        print(f"Sandbox: {args.sandbox}")
        if args.remove_fields:
            print(f"Remove fields: {args.remove_fields}")
        print()

    # Run the workflow
    try:
        result = run_data_intake_workflow(
            input_text=args.input,
            session_id=args.session,
            sandbox_dir=args.sandbox,
            fields_to_remove=args.remove_fields,
            workflow_type=args.type,
        )

        # Output result
        if args.json:
            # Output as JSON
            output_data = {
                "success": result.success,
                "message": result.message,
                "data": result.data,
                "files_created": result.files_created,
                "errors": result.errors,
            }
            print(json.dumps(output_data, indent=2))
        else:
            # Output as formatted text
            print_result(result)

        # Exit with appropriate code
        sys.exit(0 if result.success else 1)

    except KeyboardInterrupt:
        print("\n❌ Workflow interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
