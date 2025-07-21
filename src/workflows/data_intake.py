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
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

# Add the project root to Python path when running this file directly
if __name__ == "__main__":
    # Get the project root directory (two levels up from this file)
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

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
)

# Load environment variables
load_dotenv()


@dataclass
class WorkflowResult:
    """Result structure for workflow operations."""

    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    files_created: Optional[List[str]] = None
    errors: Optional[List[str]] = None


class DataIntakeWorkflow:
    """
    Data intake workflow that replicates IngestionAgent and LinkerAgent functionality.

    This workflow directly calls the underlying tools in a predefined sequence
    without using the agents SDK, providing the same results with deterministic behavior.
    """

    def __init__(self, session_id: str, sandbox_dir: str = "sandbox"):
        """
        Initialize the data intake workflow.

        Parameters
        ----------
        session_id : str
            The unique session identifier
        sandbox_dir : str
            Base sandbox directory
        """
        self.session_id = session_id
        self.sandbox_dir = sandbox_dir
        self.session_dir = Path(sandbox_dir) / session_id
        self.session_dir.mkdir(parents=True, exist_ok=True)

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

            print(f"🔧 Starting GSM workflow for {gsm_id}")

            # Step 1: Extract GSM metadata
            print(f"Step 1: Extracting GSM metadata for {gsm_id}")
            gsm_file = extract_gsm_metadata_impl(
                gsm_id, str(self.session_dir), self.email, self.api_key
            )
            files_created.append(gsm_file)
            workflow_data["gsm_metadata_file"] = gsm_file

            # Step 2: Extract Series ID from GSM metadata
            print(f"Step 2: Extracting Series ID from {gsm_id} metadata")
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
            print(f"Step 3: Extracting GSE metadata for {series_id}")
            gse_file = extract_gse_metadata_impl(
                series_id, str(self.session_dir), self.email, self.api_key
            )
            files_created.append(gse_file)
            workflow_data["gse_metadata_file"] = gse_file

            # Step 4: Extract PubMed ID from GSE metadata
            print(f"Step 4: Extracting PubMed ID from {series_id} metadata")
            pmid_result = extract_pubmed_id_from_gse_metadata_impl(
                gse_file, str(self.session_dir)
            )
            pmid_data = json.loads(pmid_result)
            pmid = pmid_data.get("pubmed_id")
            if pmid:
                workflow_data["pmid"] = pmid

                # Step 5: Extract paper abstract
                print(f"Step 5: Extracting paper abstract for PMID {pmid}")
                try:
                    paper_file = extract_paper_abstract_impl(
                        pmid, str(self.session_dir), self.email, self.api_key, gse_file
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
                print(
                    f"Step 5: No PMID found for {series_id}, skipping paper extraction"
                )

            # Step 6: Create series-sample mapping
            print("Step 6: Creating series-sample mapping")
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

            print(f"🔧 Starting GSE workflow for {gse_id}")

            # Step 1: Extract GSE metadata
            print(f"Step 1: Extracting GSE metadata for {gse_id}")
            gse_file = extract_gse_metadata_impl(
                gse_id, str(self.session_dir), self.email, self.api_key
            )
            files_created.append(gse_file)
            workflow_data["gse_metadata_file"] = gse_file

            # Step 2: Extract PubMed ID from GSE metadata
            print(f"Step 2: Extracting PubMed ID from {gse_id} metadata")
            pmid_result = extract_pubmed_id_from_gse_metadata_impl(
                gse_file, str(self.session_dir)
            )
            pmid_data = json.loads(pmid_result)
            pmid = pmid_data.get("pubmed_id")
            if pmid:
                workflow_data["pmid"] = pmid

                # Step 3: Extract paper abstract
                print(f"Step 3: Extracting paper abstract for PMID {pmid}")
                try:
                    paper_file = extract_paper_abstract_impl(
                        pmid, str(self.session_dir), self.email, self.api_key, gse_file
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
                print(f"Step 3: No PMID found for {gse_id}, skipping paper extraction")

            # Step 4: Create series-sample mapping
            print("Step 4: Creating series-sample mapping")
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

            print(f"🔧 Starting PMID workflow for {pmid}")

            # Extract paper abstract
            print(f"Step 1: Extracting paper abstract for PMID {pmid}")
            try:
                paper_file = extract_paper_abstract_impl(
                    pmid, str(self.session_dir), self.email, self.api_key
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
            print(f"🔧 Starting linker workflow for {sample_id}")

            # Step 1: Load mapping file
            print("Step 1: Loading mapping file")
            mapping_result = load_mapping_file_impl(str(self.session_dir))
            if not mapping_result["success"]:
                return WorkflowResult(
                    success=False,
                    message=f"Failed to load mapping file: {mapping_result['message']}",
                    errors=[mapping_result["message"]],
                )

            # Step 2: Find sample directory
            print(f"Step 2: Finding directory for {sample_id}")
            dir_result = find_sample_directory_impl(sample_id, str(self.session_dir))
            if not dir_result["success"]:
                return WorkflowResult(
                    success=False,
                    message=f"Failed to find directory for {sample_id}: {dir_result['message']}",
                    errors=[dir_result["message"]],
                )

            # Step 3: Clean metadata files
            print(f"Step 3: Cleaning metadata files for {sample_id}")
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
            print(f"Step 4: Packaging linked data for {sample_id}")
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
            print(f"🔧 Starting ingestion workflow for: {input_text}")

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
            for gsm_id in geo_ids["gsm_ids"]:
                result = self._extract_gsm_workflow(gsm_id)
                if not result.success:
                    return result
                all_files_created.extend(result.files_created or [])
                all_workflow_data.append(result.data)
                all_sample_ids.append(gsm_id)

            # Process GSE IDs
            for gse_id in geo_ids["gse_ids"]:
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
            for pmid in geo_ids["pmid_ids"]:
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
            print(
                f"🔧 Starting linker workflow for {len(sample_ids)} samples: {sample_ids}"
            )

            all_results = []
            all_files_created = []

            for sample_id in sample_ids:
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
    ) -> WorkflowResult:
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
        WorkflowResult
            Complete workflow result
        """
        try:
            print(f"🔧 Starting complete workflow for: {input_text}")

            # Run ingestion workflow
            ingestion_result = self.run_ingestion_workflow(input_text)
            if not ingestion_result.success:
                return ingestion_result

            # Extract sample IDs from ingestion result
            sample_ids = ingestion_result.data.get("sample_ids", [])
            if not sample_ids:
                return WorkflowResult(
                    success=False,
                    message="No sample IDs found after ingestion workflow",
                    errors=["No sample IDs extracted from ingestion workflow"],
                )

            # Run linker workflow
            linker_result = self.run_linker_workflow(sample_ids, fields_to_remove)
            if not linker_result.success:
                return linker_result

            # Combine results
            all_files_created = (ingestion_result.files_created or []) + (
                linker_result.files_created or []
            )

            return WorkflowResult(
                success=True,
                message=f"Complete workflow completed successfully. Processed {len(sample_ids)} samples",
                data={
                    "ingestion_result": ingestion_result.data,
                    "linker_result": linker_result.data,
                    "sample_ids": sample_ids,
                    "session_dir": str(self.session_dir),
                },
                files_created=all_files_created,
            )

        except Exception as e:
            return WorkflowResult(
                success=False,
                message=f"Complete workflow failed: {str(e)}",
                errors=[str(e)],
            )


def run_data_intake_workflow(
    input_text: str,
    session_id: str = None,
    sandbox_dir: str = "sandbox",
    fields_to_remove: List[str] = None,
    workflow_type: str = "complete",
) -> WorkflowResult:
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
    WorkflowResult
        Workflow execution result
    """
    import uuid

    if session_id is None:
        session_id = str(uuid.uuid4())

    workflow = DataIntakeWorkflow(session_id, sandbox_dir)

    if workflow_type == "ingestion":
        return workflow.run_ingestion_workflow(input_text)
    elif workflow_type == "linker":
        # For linker workflow, input_text should be a list of sample IDs
        sample_ids = [s.strip() for s in input_text.split(",") if s.strip()]
        return workflow.run_linker_workflow(sample_ids, fields_to_remove)
    elif workflow_type == "complete":
        return workflow.run_complete_workflow(input_text, fields_to_remove)
    else:
        return WorkflowResult(
            success=False,
            message=f"Invalid workflow type: {workflow_type}",
            errors=["Supported types: ingestion, linker, complete"],
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
        print("🔧 Data Intake Workflow")
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
