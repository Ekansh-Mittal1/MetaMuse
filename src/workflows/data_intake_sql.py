"""
SQLite-based data intake workflow for GEO metadata extraction and linking.

This module provides a data intake workflow that replicates the functionality
of the IngestionAgent and LinkerAgent using the local GEOmetadb SQLite database
instead of ENTREZ API calls. It directly calls the underlying SQLite tools
in a predefined sequence for much faster performance.
"""

import json
import os
import re
import sys
import argparse
from pathlib import Path
from typing import Dict, List
from dotenv import load_dotenv

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

# Import the SQLite-based tool implementations
from src.tools.sqlite_ingestion_tools import (
    extract_gsm_metadata_sqlite_impl,
    extract_gse_metadata_sqlite_impl,
    extract_paper_abstract_sqlite_impl,
    extract_pubmed_id_from_gse_metadata_sqlite_impl,
    extract_series_id_from_gsm_metadata_sqlite_impl,
    create_series_sample_mapping_sqlite_impl,
    validate_geo_inputs_sqlite_impl,
    search_geo_sqlite_impl,
    get_database_info_sqlite_impl,
    download_geometadb_impl,
)

from src.tools.linker_tools import (
    load_mapping_file_impl,
    find_sample_directory_impl,
    clean_metadata_files_impl,
    package_linked_data_impl,
    create_curation_data_package_impl,
)

# Import SQLite manager for database operations
from src.tools.sqlite_manager import GEOmetadbManager, get_geometadb_manager

# Load environment variables
load_dotenv()


def _get_series_subdirectory(session_dir: str, series_id: str, create_directory: bool = True) -> Path:
    """
    Get or create a subdirectory for a specific series ID within the session directory.

    Parameters
    ----------
    session_dir : str
        The session directory path
    series_id : str
        The series ID (e.g., "GSE41588")
    create_directory : bool
        Whether to create the directory if it doesn't exist

    Returns
    -------
    Path
        Path to the series subdirectory
    """
    series_dir = Path(session_dir) / series_id
    if create_directory:
        series_dir.mkdir(parents=True, exist_ok=True)
    return series_dir


class DataIntakeSQLWorkflow:
    """
    SQLite-based data intake workflow that replicates IngestionAgent and LinkerAgent functionality.

    This workflow directly calls the underlying SQLite tools in a predefined sequence
    without using the agents SDK, providing the same results with deterministic behavior
    and much faster performance due to local database queries.
    """

    def __init__(self, session_id: str, sandbox_dir: str = "sandbox", 
                 create_series_directories: bool = True, db_path: str = "GEOmetadb.sqlite"):
        """
        Initialize the SQLite-based data intake workflow.

        Parameters
        ----------
        session_id : str
            The unique session identifier
        sandbox_dir : str
            Base sandbox directory
        create_series_directories : bool
            Whether to create GSE* series directories during processing
        db_path : str
            Path to the GEOmetadb SQLite database
        """
        import time

        self.session_id = session_id
        self.sandbox_dir = sandbox_dir
        self.create_series_directories = create_series_directories
        self.db_path = db_path
        
        # For unified discovery structure, use sandbox_dir directly
        if session_id == "discovery":
            self.session_dir = Path(sandbox_dir)
        else:
            self.session_dir = Path(sandbox_dir) / session_id
            self.session_dir.mkdir(parents=True, exist_ok=True)
        self._start_time = time.time()

        # Check if database exists and is accessible
        self._check_database()

    def _check_database(self):
        """Check if the SQLite database exists and is accessible."""
        try:
            if not Path(self.db_path).exists():
                print(f"⚠️  Database not found at {self.db_path}")
                print("📥 Attempting to download GEOmetadb database...")
                
                success = download_geometadb_impl(self.db_path, force=False)
                if "successfully downloaded" in success:
                    print("✅ Database downloaded successfully")
                else:
                    print(f"❌ Failed to download database: {success}")
                    raise ValueError("Database not available")
            
            # Test database connection
            with get_geometadb_manager(self.db_path) as manager:
                info = manager.get_database_info()
                if "error" in info:
                    raise ValueError(f"Database connection failed: {info['error']}")
                
                print(f"✅ Connected to GEOmetadb database")
                print(f"   Size: {info.get('file_size_mb', 'Unknown')} MB")
                print(f"   Tables: {len(info.get('tables', []))}")
                print(f"   GSE records: {info.get('row_counts', {}).get('gse', 'Unknown')}")
                print(f"   GSM records: {info.get('row_counts', {}).get('gsm', 'Unknown')}")
                
        except Exception as e:
            print(f"❌ Database initialization failed: {e}")
            print("🔍 Full traceback:")
            import traceback
            traceback.print_exc()
            raise ValueError(f"Database initialization failed: {e}")

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
        Validate the parsed GEO IDs using SQLite-based validation.

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
            # Validate each type of ID using SQLite tools
            for gsm_id in geo_ids["gsm_ids"]:
                result = validate_geo_inputs_sqlite_impl(gsm_id=gsm_id)
                result_data = json.loads(result)
                if not result_data["valid"]:
                    return WorkflowResult(
                        success=False,
                        message=f"Validation failed for GSM ID {gsm_id}",
                        errors=result_data["errors"],
                    )

            for gse_id in geo_ids["gse_ids"]:
                result = validate_geo_inputs_sqlite_impl(gse_id=gse_id)
                result_data = json.loads(result)
                if not result_data["valid"]:
                    return WorkflowResult(
                        success=False,
                        message=f"Validation failed for GSE ID {gse_id}",
                        errors=result_data["errors"],
                    )

            for pmid in geo_ids["pmid_ids"]:
                result = validate_geo_inputs_sqlite_impl(pmid=str(pmid))
                result_data = json.loads(result)
                if not result_data["valid"]:
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
        Execute the 6-step GSM workflow using SQLite database.

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

            print(f"🔍 Starting GSM workflow for {gsm_id}")

            # Step 1: Extract GSM metadata from SQLite
            gsm_file = extract_gsm_metadata_sqlite_impl(
                gsm_id, str(self.session_dir), self.db_path
            )
            files_created.append(gsm_file)
            workflow_data["gsm_metadata_file"] = gsm_file
            print(f"✅ GSM metadata extracted: {gsm_file}")
            
            # If create_series_directories is True, move the GSM file to a series subdirectory
            if self.create_series_directories:
                try:
                    # Read the GSM metadata to get the series ID
                    with open(gsm_file, 'r') as f:
                        gsm_metadata = json.load(f)
                    
                    # Handle both old and new GSM metadata structures
                    series_ids = gsm_metadata.get('series', [])
                    if not series_ids and 'attributes' in gsm_metadata:
                        # New structure has series_id in attributes
                        series_id = gsm_metadata['attributes'].get('series_id')
                        if series_id:
                            series_ids = [series_id]
                    
                    if series_ids:
                        # Use the first series ID for directory organization
                        primary_series_id = series_ids[0]
                        series_dir = _get_series_subdirectory(str(self.session_dir), primary_series_id, True)
                        
                        # Move the GSM file to the series directory
                        new_gsm_file = series_dir / f"{gsm_id}_metadata.json"
                        import shutil
                        shutil.move(gsm_file, new_gsm_file)
                        
                        # Update the file path
                        gsm_file = str(new_gsm_file)
                        workflow_data["gsm_metadata_file"] = gsm_file
                        files_created[-1] = gsm_file  # Update the last added file
                        
                        print(f"✅ Moved GSM metadata to series directory: {gsm_file}")
                except Exception as e:
                    print(f"⚠️  Warning: Failed to move GSM file to series directory: {e}")
                    print("⚠️  Continuing with original file location...")

            # Step 2: Extract Series ID from GSM metadata
            series_result = extract_series_id_from_gsm_metadata_sqlite_impl(gsm_file)
            series_data = json.loads(series_result)
            if not series_data.get("success", False):
                return WorkflowResult(
                    success=False,
                    message=f"Series ID extraction failed for {gsm_id}",
                    errors=[series_data.get("message", "Unknown error")],
                )
            series_id = series_data.get("series_id")
            if not series_id:
                return WorkflowResult(
                    success=False,
                    message=f"No series ID found for {gsm_id}",
                    errors=[f"Series ID extraction failed for {gsm_id}"],
                )
            workflow_data["series_id"] = series_id
            print(f"✅ Series ID extracted: {series_id}")

            # Step 3: Extract GSE metadata from SQLite
            gse_file = extract_gse_metadata_sqlite_impl(
                series_id, str(self.session_dir), self.db_path
            )
            files_created.append(gse_file)
            workflow_data["gse_metadata_file"] = gse_file
            print(f"✅ GSE metadata extracted: {gse_file}")
            
            # If create_series_directories is True, move the GSE file to a series subdirectory
            if self.create_series_directories:
                try:
                    # Create the series directory and move the GSE file there
                    series_dir = _get_series_subdirectory(str(self.session_dir), series_id, True)
                    
                    # Move the GSE file to the series directory
                    new_gse_file = series_dir / f"{series_id}_metadata.json"
                    import shutil
                    shutil.move(gse_file, new_gse_file)
                    
                    # Update the file path
                    gse_file = str(new_gse_file)
                    workflow_data["gse_metadata_file"] = gse_file
                    files_created[-1] = gse_file  # Update the last added file
                    
                    print(f"✅ Moved GSE metadata to series directory: {gse_file}")
                except Exception as e:
                    print(f"⚠️  Warning: Failed to move GSE file to series directory: {e}")
                    print("⚠️  Continuing with original file location...")

            # Step 4: Extract PubMed ID from GSE metadata
            pmid_result = extract_pubmed_id_from_gse_metadata_sqlite_impl(gse_file)
            pmid_data = json.loads(pmid_result)
            if not pmid_data.get("success", False):
                print(f"⚠️  Warning: Failed to extract PubMed ID: {pmid_data.get('message', 'Unknown error')}")
                print("⚠️  Continuing workflow without PubMed ID...")
                workflow_data["pmid_extraction_error"] = pmid_data.get("message", "Unknown error")
            else:
                pmid = pmid_data.get("pubmed_id")
                if pmid:
                    workflow_data["pmid"] = pmid
                    print(f"✅ PubMed ID extracted: {pmid}")

                # Step 5: Extract paper abstract from SQLite
                try:
                    paper_file = extract_paper_abstract_sqlite_impl(
                        pmid, str(self.session_dir), self.db_path
                    )
                    files_created.append(paper_file)
                    workflow_data["paper_metadata_file"] = paper_file
                    print(f"✅ Paper abstract extracted: {paper_file}")
                    
                    # If create_series_directories is True, move the paper file to a series subdirectory
                    if self.create_series_directories:
                        try:
                            # Move the paper file to the series directory
                            new_paper_file = series_dir / f"PMID_{pmid}_metadata.json"
                            import shutil
                            shutil.move(paper_file, new_paper_file)
                            
                            # Update the file path
                            paper_file = str(new_paper_file)
                            workflow_data["paper_metadata_file"] = paper_file
                            files_created[-1] = paper_file  # Update the last added file
                            
                            print(f"✅ Moved paper metadata to series directory: {paper_file}")
                        except Exception as e:
                            print(f"⚠️  Warning: Failed to move paper file to series directory: {e}")
                            print("⚠️  Continuing with original file location...")
                            
                except Exception as e:
                    print(
                        f"⚠️  Warning: Failed to extract paper abstract for PMID {pmid}: {e}"
                    )
                    print("⚠️  Continuing workflow without paper abstract...")
                    workflow_data["paper_extraction_error"] = str(e)

            # Step 6: Create series-sample mapping using SQLite
            mapping_file = create_series_sample_mapping_sqlite_impl(str(self.session_dir), self.db_path)
            files_created.append(mapping_file)
            workflow_data["mapping_file"] = mapping_file
            print(f"✅ Series-sample mapping created: {mapping_file}")

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
        Execute the 4-step GSE workflow using SQLite database.

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

            print(f"🔍 Starting GSE workflow for {gse_id}")

            # Step 1: Extract GSE metadata from SQLite
            gse_file = extract_gse_metadata_sqlite_impl(
                gse_id, str(self.session_dir), self.db_path
            )
            files_created.append(gse_file)
            workflow_data["gse_metadata_file"] = gse_file
            print(f"✅ GSE metadata extracted: {gse_file}")
            
            # If create_series_directories is True, move the GSE file to a series subdirectory
            if self.create_series_directories:
                try:
                    # Create the series directory and move the GSE file there
                    series_dir = _get_series_subdirectory(str(self.session_dir), gse_id, True)
                    
                    # Move the GSE file to the series directory
                    new_gse_file = series_dir / f"{gse_id}_metadata.json"
                    import shutil
                    shutil.move(gse_file, new_gse_file)
                    
                    # Update the file path
                    gse_file = str(new_gse_file)
                    workflow_data["gse_metadata_file"] = gse_file
                    files_created[-1] = gse_file  # Update the last added file
                    
                    print(f"✅ Moved GSE metadata to series directory: {gse_file}")
                except Exception as e:
                    print(f"⚠️  Warning: Failed to move GSE file to series directory: {e}")
                    print("⚠️  Continuing with original file location...")

            # Step 2: Extract PubMed ID from GSE metadata
            pmid_result = extract_pubmed_id_from_gse_metadata_sqlite_impl(gse_file)
            pmid_data = json.loads(pmid_result)
            if not pmid_data.get("success", False):
                print(f"⚠️  Warning: Failed to extract PubMed ID: {pmid_data.get('message', 'Unknown error')}")
                print("⚠️  Continuing workflow without PubMed ID...")
                workflow_data["pmid_extraction_error"] = pmid_data.get("message", "Unknown error")
            else:
                pmid = pmid_data.get("pubmed_id")
                if pmid:
                    workflow_data["pmid"] = pmid
                    print(f"✅ PubMed ID extracted: {pmid}")

                    # Step 3: Extract paper abstract from SQLite
                    try:
                        paper_file = extract_paper_abstract_sqlite_impl(
                            pmid, str(self.session_dir), self.db_path
                        )
                        files_created.append(paper_file)
                        workflow_data["paper_metadata_file"] = paper_file
                        print(f"✅ Paper abstract extracted: {paper_file}")
                        
                        # If create_series_directories is True, move the paper file to a series subdirectory
                        if self.create_series_directories:
                            try:
                                # Move the paper file to the series directory
                                new_paper_file = series_dir / f"PMID_{pmid}_metadata.json"
                                import shutil
                                shutil.move(paper_file, new_paper_file)
                                
                                # Update the file path
                                paper_file = str(new_paper_file)
                                workflow_data["paper_metadata_file"] = paper_file
                                files_created[-1] = paper_file  # Update the last added file
                                
                                print(f"✅ Moved paper metadata to series directory: {paper_file}")
                            except Exception as e:
                                print(f"⚠️  Warning: Failed to move paper file to series directory: {e}")
                                print("⚠️  Continuing with original file location...")
                                
                    except Exception as e:
                        print(
                            f"⚠️  Warning: Failed to extract paper abstract for PMID {pmid}: {e}"
                        )
                        print("⚠️  Continuing workflow without paper abstract...")
                        workflow_data["paper_extraction_error"] = str(e)
                else:
                    print("ℹ️  No PubMed ID found in GSE metadata")

            # Step 4: Create series-sample mapping using SQLite
            mapping_file = create_series_sample_mapping_sqlite_impl(str(self.session_dir), self.db_path)
            files_created.append(mapping_file)
            workflow_data["mapping_file"] = mapping_file
            print(f"✅ Series-sample mapping created: {mapping_file}")

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
        Execute the PMID workflow using SQLite database.

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

            print(f"🔍 Starting PMID workflow for {pmid}")

            # Extract paper abstract from SQLite
            try:
                paper_file = extract_paper_abstract_sqlite_impl(
                    pmid, str(self.session_dir), self.db_path
                )
                files_created.append(paper_file)
                workflow_data["paper_metadata_file"] = paper_file
                print(f"✅ Paper abstract extracted: {paper_file}")
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
            print(f"🔗 Starting linker workflow for {sample_id}")

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

            print(f"✅ Linker workflow completed for {sample_id}")

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
        Run the complete ingestion workflow using SQLite database.

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
            print("🚀 Starting SQLite-based ingestion workflow...")

            # Parse GEO IDs
            geo_ids = self._parse_geo_ids(input_text)
            print(f"📋 Parsed IDs: {len(geo_ids['gsm_ids'])} GSM, {len(geo_ids['gse_ids'])} GSE, {len(geo_ids['pmid_ids'])} PMID")

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

            # Process PMID IDs
            for pmid in geo_ids["pmid_ids"]:
                result = self._extract_pmid_workflow(pmid)
                if not result.success:
                    return result
                all_files_created.extend(result.files_created or [])
                all_workflow_data.append(result.data)

            print(f"✅ Ingestion workflow completed successfully")

            return WorkflowResult(
                success=True,
                message=f"SQLite ingestion workflow completed successfully. Processed {len(geo_ids['gsm_ids'])} GSM, {len(geo_ids['gse_ids'])} GSE, {len(geo_ids['pmid_ids'])} PMID",
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
                message=f"SQLite ingestion workflow failed: {str(e)}",
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
            print(f"🔗 Starting SQLite-based linker workflow for {len(sample_ids)} samples...")
            
            all_results = []
            all_files_created = []

            for sample_id in sample_ids:
                result = self._link_sample_data(sample_id, fields_to_remove)
                if not result.success:
                    return result
                all_results.append(result.data)
                all_files_created.extend(result.files_created or [])

            print(f"✅ Linker workflow completed successfully")

            return WorkflowResult(
                success=True,
                message=f"SQLite linker workflow completed successfully for {len(sample_ids)} samples",
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
                message=f"SQLite linker workflow failed: {str(e)}",
                errors=[str(e)],
            )

    def run_complete_workflow(
        self, input_text: str, fields_to_remove: List[str] = None
    ) -> LinkerOutput:
        """
        Run the complete workflow (ingestion + linking) using SQLite database.

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
            print("🚀 Starting complete SQLite-based workflow...")

            # Parse GEO IDs
            geo_ids = self._parse_geo_ids(input_text)
            print(f"📋 Parsed IDs: {len(geo_ids['gsm_ids'])} GSM, {len(geo_ids['gse_ids'])} GSE, {len(geo_ids['pmid_ids'])} PMID")

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

            # Process PMID IDs
            for pmid in geo_ids["pmid_ids"]:
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

            print(f"✅ Complete SQLite workflow completed successfully in {execution_time:.2f} seconds")

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
                message=f"Complete SQLite workflow failed: {str(e)}",
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

    def search_geo(self, query: str, search_type: str = "all", limit: int = 100) -> Dict:
        """
        Search GEO database using local SQLite database.
        
        Parameters
        ----------
        query : str
            Search query string
        search_type : str
            Type of search: 'all', 'gse', 'gsm', 'gpl'
        limit : int
            Maximum number of results to return
            
        Returns
        -------
        Dict
            Search results
        """
        try:
            results = search_geo_sqlite_impl(query, search_type, limit, self.db_path)
            return json.loads(results)
        except Exception as e:
            return {"error": f"Search failed: {str(e)}"}

    def get_database_info(self) -> Dict:
        """
        Get information about the local GEOmetadb database.
        
        Returns
        -------
        Dict
            Database information
        """
        try:
            info = get_database_info_sqlite_impl(self.db_path)
            return json.loads(info)
        except Exception as e:
            return {"error": f"Failed to get database info: {str(e)}"}


def run_data_intake_sql_workflow(
    input_text: str,
    session_id: str = None,
    sandbox_dir: str = "sandbox",
    fields_to_remove: List[str] = None,
    workflow_type: str = "complete",
    create_series_directories: bool = True,
    db_path: str = "GEOmetadb.sqlite",
) -> LinkerOutput:
    """
    Run the SQLite-based data intake workflow.

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
    create_series_directories : bool
        Whether to create GSE* series directories during processing
    db_path : str
        Path to the GEOmetadb SQLite database

    Returns
    -------
    LinkerOutput
        Workflow execution result as LinkerOutput object
    """
    import uuid

    if session_id is None:
        # Generate session ID with pipeline prefix
        pipeline_prefixes = {
            "ingestion": "di_sql_ing",
            "linker": "di_sql_link",
            "complete": "di_sql",
        }

        prefix = pipeline_prefixes.get(workflow_type, "di_sql_unknown")
        session_id = f"{prefix}_{str(uuid.uuid4())}"

    workflow = DataIntakeSQLWorkflow(
        session_id, 
        sandbox_dir, 
        create_series_directories=create_series_directories,
        db_path=db_path
    )

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


def print_result(result):
    """Print the workflow result in a formatted way."""
    print("\n" + "=" * 60)
    print("SQLITE WORKFLOW RESULT")
    print("=" * 60)
    
    # Handle WorkflowResult objects
    if hasattr(result, 'success'):
        print(f"Success: {result.success}")
        print(f"Message: {result.message}")

        if hasattr(result, 'errors') and result.errors:
            print("\nErrors:")
            for error in result.errors:
                print(f"  - {error}")

        if hasattr(result, 'data') and result.data:
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

        if hasattr(result, 'files_created') and result.files_created:
            print(f"\nFiles Created ({len(result.files_created)}):")
            for file_path in result.files_created:
                print(f"  - {file_path}")
    
    # Handle LinkerOutput objects
    elif hasattr(result, 'samples'):
        print(f"Success: True")
        print(f"Message: Successfully linked {len(result.samples)} samples")
        
        if hasattr(result, 'session_directory'):
            print(f"\nSession Directory: {result.session_directory}")
        
        if hasattr(result, 'mapping_file'):
            print(f"Mapping File: {result.mapping_file}")
    
    else:
        print(f"Result: {result}")

    print("=" * 60)


def main():
    """Command-line interface for the SQLite-based data intake workflow."""
    parser = argparse.ArgumentParser(
        description="SQLite-based data intake workflow for GEO metadata extraction and linking",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Complete workflow (ingestion + linking)
  python data_intake_sql.py --input "Extract metadata for GSM1000981, GSE41588" --type complete

  # Ingestion only
  python data_intake_sql.py --input "Extract metadata for GSM1000981" --type ingestion

  # Linker only (requires existing session)
  python data_intake_sql.py --input "GSM1000981, GSM1098372" --type linker --session existing-session

  # With custom fields to remove
  python data_intake_sql.py --input "Extract metadata for GSM1000981" --type complete --remove-fields status submission_date last_update_date

  # With custom session and sandbox
  python data_intake_sql.py --input "Extract metadata for GSE41588" --type complete --session my-session --sandbox custom-sandbox

  # With custom database path
  python data_intake_sql.py --input "Extract metadata for GSM1000981" --type complete --db-path /path/to/custom/GEOmetadb.sqlite
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
        "--db-path",
        default="GEOmetadb.sqlite",
        help="Path to the GEOmetadb SQLite database (default: GEOmetadb.sqlite)",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )

    parser.add_argument("--json", action="store_true", help="Output result as JSON")

    args = parser.parse_args()

    # Set up logging
    if args.verbose:
        print("🚀 SQLite-based Data Intake Workflow")
        print(f"Input: {args.input}")
        print(f"Type: {args.type}")
        print(f"Session: {args.session or 'auto-generated'}")
        print(f"Sandbox: {args.sandbox}")
        print(f"Database: {args.db_path}")
        if args.remove_fields:
            print(f"Remove fields: {args.remove_fields}")
        print()

    # Run the workflow
    try:
        result = run_data_intake_sql_workflow(
            input_text=args.input,
            session_id=args.session,
            sandbox_dir=args.sandbox,
            fields_to_remove=args.remove_fields,
            workflow_type=args.type,
            db_path=args.db_path,
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
