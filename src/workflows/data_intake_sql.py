"""
SQLite-based data intake workflow for GEO metadata extraction and linking.

This module provides a data intake workflow that replicates the functionality
of the IngestionAgent and LinkerAgent using the local GEOmetadb SQLite database
instead of ENTREZ API calls. It directly calls the underlying SQLite tools
in a predefined sequence for much faster performance.
"""

import json
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
from src.tools.sqlite_manager import get_geometadb_manager

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
                 create_series_directories: bool = True, db_path: str = "data/GEOmetadb.sqlite",
                 enable_profiling: bool = False, max_workers: int = None):
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
        self.enable_profiling = enable_profiling
        self.max_workers = max_workers
        
        # For unified discovery structure, use sandbox_dir directly
        if session_id == "discovery":
            self.session_dir = Path(sandbox_dir)
        else:
            self.session_dir = Path(sandbox_dir) / session_id
            self.session_dir.mkdir(parents=True, exist_ok=True)
        self._start_time = time.time()
        
        # New layout: data_intake directory with raw_data
        self.data_intake_dir = self.session_dir / "data_intake"
        self.data_intake_dir.mkdir(parents=True, exist_ok=True)
        self.data_intake_raw_dir = self.data_intake_dir / "raw_data"
        self.data_intake_raw_dir.mkdir(parents=True, exist_ok=True)
        
        # Profiling storage
        self._timings = {"phases": []}
        self._phase_start = None
        
        # Create and maintain a single database manager instance
        self._db_manager = None
        
        # Check if database exists and is accessible
        self._check_database()

    def _phase(self, name: str):
        import contextlib
        import time
        @contextlib.contextmanager
        def timer():
            start = time.time()
            try:
                yield
            finally:
                duration = time.time() - start
                if self.enable_profiling:
                    self._timings["phases"].append({"name": name, "seconds": duration})
        return timer()

    def _flush_profile(self, extra: Dict = None):
        if not self.enable_profiling:
            return
        try:
            import time
            total = time.time() - self._start_time
            self._timings["total_seconds"] = total
            if extra:
                self._timings.update(extra)
            out = self.session_dir / "data_intake" / "intake_profile.json"
            out.parent.mkdir(parents=True, exist_ok=True)
            with open(out, "w") as f:
                json.dump(self._timings, f, indent=2)
        except Exception as e:
            print(f"⚠️  Failed to write profiling report: {e}")

    def _get_db_manager(self):
        """Get or create a database manager instance."""
        if not hasattr(self, '_db_manager') or self._db_manager is None:
            from src.tools.sqlite_manager import GEOmetadbManager
            self._db_manager = GEOmetadbManager(self.db_path)
        return self._db_manager

    def _close_db_manager(self):
        """Close the database manager if it exists."""
        if self._db_manager is not None:
            try:
                self._db_manager.close()
            except Exception as e:
                print(f"⚠️  Warning: Failed to close database manager: {e}")
            finally:
                self._db_manager = None

    def _extract_gsm_metadata_optimized(self, gsm_id: str) -> str:
        """Extract GSM metadata using the shared database manager."""
        try:
            session_path = Path(str(self.session_dir))
            session_path.mkdir(parents=True, exist_ok=True)
            
            # Get metadata from shared database manager
            manager = self._get_db_manager()
            metadata = manager.get_gsm_metadata(gsm_id)
            
            if "error" in metadata:
                # Fall back to HTTP API
                return extract_gsm_metadata_sqlite_impl(gsm_id, str(self.session_dir), self.db_path)
            
            # Restructure metadata to match original workflow structure
            restructured_metadata = {
                "gsm_id": gsm_id,
                "status": "retrieved",
                "attributes": {}
            }
            
            # Move all database fields to attributes (except gsm_id, status, series)
            for key, value in metadata.items():
                if key not in ["gsm_id", "status", "series"]:
                    # Convert numeric fields to strings to match Pydantic model expectations
                    if key in ["channel_count", "data_row_count"] and value is not None:
                        restructured_metadata["attributes"][key] = str(value)
                    # Skip the raw 'gsm' field as it's redundant with gsm_id
                    elif key != "gsm":
                        restructured_metadata["attributes"][key] = value
            
            # Add series information to attributes
            if "series" in metadata:
                restructured_metadata["attributes"]["series_id"] = metadata["series"][0] if metadata["series"] else None
                restructured_metadata["attributes"]["all_series_ids"] = ", ".join(metadata["series"]) if metadata["series"] else None
            
            # Determine series and target directory
            series_id = None
            if "series" in metadata and metadata["series"]:
                series_id = metadata["series"][0]

            target_dir = session_path
            if series_id:
                target_dir = session_path / series_id
                target_dir.mkdir(parents=True, exist_ok=True)

            # Save metadata to file under series directory when available
            output_file = target_dir / f"{gsm_id}_metadata.json"
            with open(output_file, 'w') as f:
                json.dump(restructured_metadata, f, indent=2, default=str)
            
            return str(output_file)
            
        except Exception as e:
            # Fall back to original implementation
            return extract_gsm_metadata_sqlite_impl(gsm_id, str(self.session_dir), self.db_path)

    def _extract_gse_metadata_optimized(self, gse_id: str) -> str:
        """Extract GSE metadata using the shared database manager."""
        try:
            session_path = Path(str(self.session_dir))
            session_path.mkdir(parents=True, exist_ok=True)
            
            # Get metadata from shared database manager
            manager = self._get_db_manager()
            metadata = manager.get_gse_metadata(gse_id)
            
            if "error" in metadata:
                # Fall back to HTTP API
                return extract_gse_metadata_sqlite_impl(gse_id, str(self.session_dir), self.db_path)
            
            # Restructure metadata to match original workflow structure
            restructured_metadata = {
                "gse_id": gse_id,
                "status": "retrieved",
                "attributes": {}
            }
            
            # Move all database fields to attributes (except gse_id, status, samples, platforms, gse)
            for key, value in metadata.items():
                if key not in ["gse_id", "status", "samples", "platforms", "gse"]:
                    # Convert pubmed_id to string if it exists
                    if key == "pubmed_id" and value is not None:
                        restructured_metadata["attributes"][key] = str(value)
                    else:
                        restructured_metadata["attributes"][key] = value
            
            # Add samples and platforms to attributes
            if "samples" in metadata:
                restructured_metadata["attributes"]["sample_id"] = ", ".join(metadata["samples"]) if metadata["samples"] else None
            if "platforms" in metadata:
                restructured_metadata["attributes"]["platform_id"] = ", ".join(metadata["platforms"]) if metadata["platforms"] else None
            
            # Save metadata to file under series directory
            target_dir = session_path / gse_id
            target_dir.mkdir(parents=True, exist_ok=True)
            output_file = target_dir / f"{gse_id}_metadata.json"
            with open(output_file, 'w') as f:
                json.dump(restructured_metadata, f, indent=2, default=str)
            
            return str(output_file)
            
        except Exception as e:
            # Fall back to original implementation
            return extract_gse_metadata_sqlite_impl(gse_id, str(self.session_dir), self.db_path)

    def _batch_check_gsm_availability(self, gsm_ids: List[str]) -> Dict[str, bool]:
        """Check which GSMs are available in the local database."""
        try:
            manager = self._get_db_manager()
            available = {}
            
            # Batch query to check GSM availability
            if manager.connection:
                placeholders = ','.join(['?' for _ in gsm_ids])
                query = f"SELECT gsm FROM gsm WHERE gsm IN ({placeholders})"
                cursor = manager.connection.execute(query, gsm_ids)
                found_gsms = {row[0] for row in cursor.fetchall()}
                
                for gsm_id in gsm_ids:
                    available[gsm_id] = gsm_id in found_gsms
            else:
                # Fallback: assume none are available
                available = {gsm_id: False for gsm_id in gsm_ids}
                
            return available
        except Exception as e:
            print(f"⚠️  Batch GSM availability check failed: {e}")
            # Fallback: assume none are available
            return {gsm_id: False for gsm_id in gsm_ids}

    def _batch_check_gse_availability(self, gse_ids: List[str]) -> Dict[str, bool]:
        """Check which GSEs are available in the local database."""
        try:
            manager = self._get_db_manager()
            available = {}
            
            # Batch query to check GSE availability
            if manager.connection:
                placeholders = ','.join(['?' for _ in gse_ids])
                query = f"SELECT gse FROM gse WHERE gse IN ({placeholders})"
                cursor = manager.connection.execute(query, gse_ids)
                found_gses = {row[0] for row in cursor.fetchall()}
                
                for gse_id in gse_ids:
                    available[gse_id] = gse_id in found_gses
            else:
                # Fallback: assume none are available
                available = {gse_id: False for gse_id in gse_ids}
                
            return available
        except Exception as e:
            print(f"⚠️  Batch GSE availability check failed: {e}")
            # Fallback: assume none are available
            return {gse_id: False for gse_id in gse_ids}

    def _batch_extract_gsm_metadata(self, gsm_ids: List[str]) -> Dict[str, str]:
        """Extract metadata for multiple GSMs, using batch DB queries where possible."""
        self._ensure_data_intake_dirs()
        results = {}
        
        # Check which GSMs are available in the database
        with self._phase("batch_check_gsm_availability"):
            availability = self._batch_check_gsm_availability(gsm_ids)
        
        # Process GSMs that are available in the database
        db_available = [gsm_id for gsm_id, available in availability.items() if available]
        http_fallback = [gsm_id for gsm_id, available in availability.items() if not available]
        
        if db_available:
            print(f"🔍 Processing {len(db_available)} GSMs from local database")
            with self._phase("batch_extract_gsm_from_db"):
                for gsm_id in db_available:
                    try:
                        results[gsm_id] = self._extract_gsm_metadata_optimized(gsm_id)
                    except Exception as e:
                        print(f"⚠️  Failed to extract {gsm_id} from DB: {e}")
                        http_fallback.append(gsm_id)
        
        if http_fallback:
            print(f"🔍 Processing {len(http_fallback)} GSMs via HTTP API fallback")
            with self._phase("batch_extract_gsm_from_http"):
                for gsm_id in http_fallback:
                    try:
                        results[gsm_id] = extract_gsm_metadata_sqlite_impl(gsm_id, str(self.session_dir), self.db_path)
                    except Exception as e:
                        print(f"❌ Failed to extract {gsm_id} via HTTP: {e}")
        
        return results

    def _batch_extract_gse_metadata(self, gse_ids: List[str]) -> Dict[str, str]:
        """Extract metadata for multiple GSEs, using batch DB queries where possible."""
        self._ensure_data_intake_dirs()
        results = {}
        
        # Check which GSEs are available in the database  
        with self._phase("batch_check_gse_availability"):
            availability = self._batch_check_gse_availability(gse_ids)
        
        # Process GSEs that are available in the database
        db_available = [gse_id for gse_id, available in availability.items() if available]
        http_fallback = [gse_id for gse_id, available in availability.items() if not available]
        
        if db_available:
            print(f"🔍 Processing {len(db_available)} GSEs from local database")
            with self._phase("batch_extract_gse_from_db"):
                for gse_id in db_available:
                    try:
                        results[gse_id] = self._extract_gse_metadata_optimized(gse_id)
                    except Exception as e:
                        print(f"⚠️  Failed to extract {gse_id} from DB: {e}")
                        http_fallback.append(gse_id)
        
        if http_fallback:
            print(f"🔍 Processing {len(http_fallback)} GSEs via HTTP API fallback")
            with self._phase("batch_extract_gse_from_http"):
                for gse_id in http_fallback:
                    try:
                        results[gse_id] = extract_gse_metadata_sqlite_impl(gse_id, str(self.session_dir), self.db_path)
                    except Exception as e:
                        print(f"❌ Failed to extract {gse_id} via HTTP: {e}")
        
        return results

    def _ensure_data_intake_dirs(self):
        """Ensure data_intake and raw_data directories and attributes exist."""
        try:
            if not hasattr(self, "session_dir") or self.session_dir is None:
                if self.session_id == "discovery":
                    self.session_dir = Path(self.sandbox_dir)
                else:
                    self.session_dir = Path(self.sandbox_dir) / self.session_id
                    self.session_dir.mkdir(parents=True, exist_ok=True)
            
            if not hasattr(self, "data_intake_dir") or self.data_intake_dir is None:
                self.data_intake_dir = self.session_dir / "data_intake"
                self.data_intake_dir.mkdir(parents=True, exist_ok=True)
                
            if not hasattr(self, "data_intake_raw_dir") or self.data_intake_raw_dir is None:
                self.data_intake_raw_dir = self.data_intake_dir / "raw_data"
                self.data_intake_raw_dir.mkdir(parents=True, exist_ok=True)
                
        except Exception as e:
            print(f"⚠️  Failed to ensure data_intake directories: {e}")


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
                
                print("✅ Connected to GEOmetadb database")
                print(f"   Size: {info.get('file_size_mb', 'Unknown')} MB")
                print(f"   Tables: {len(info.get('tables', []))}")
                print(f"   GSE records: {info.get('row_counts', {}).get('gse', 'Unknown')}")
                print(f"   GSM records: {info.get('row_counts', {}).get('gsm', 'Unknown')}")
                
                # Check PubMed database availability
                try:
                    from src.tools.sqlite_ingestion_tools import get_pubmed_database_info_sqlite_impl
                    import json
                    
                    pubmed_info_json = get_pubmed_database_info_sqlite_impl()
                    pubmed_info = json.loads(pubmed_info_json)
                    
                    if "error" not in pubmed_info:
                        print("✅ Connected to PubMed database")
                        print(f"   Size: {pubmed_info.get('database_size_mb', 'Unknown')} MB")
                        print(f"   Articles: {pubmed_info.get('article_count', 'Unknown'):,}")
                        print(f"   Authors: {pubmed_info.get('author_count', 'Unknown'):,}")
                        print(f"   Year range: {pubmed_info.get('year_range', 'Unknown')}")
                    else:
                        print(f"⚠️  PubMed database not available: {pubmed_info.get('error', 'Unknown error')}")
                        
                except Exception as pubmed_e:
                    print(f"⚠️  Could not check PubMed database: {pubmed_e}")
                
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


            # Step 1: Extract GSM metadata from SQLite
            with self._phase("extract_gsm_metadata"):
                gsm_file = self._extract_gsm_metadata_optimized(gsm_id)
            files_created.append(gsm_file)
            workflow_data["gsm_metadata_file"] = gsm_file
            
            # Ensure directories exist (robust when called in different contexts)
            self._ensure_data_intake_dirs()

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
                        series_dir = _get_series_subdirectory(str(self.data_intake_dir), primary_series_id, True)
                        
                        # Move the GSM file to the series directory
                        new_gsm_file = series_dir / f"{gsm_id}_metadata.json"
                        import shutil
                        shutil.move(gsm_file, new_gsm_file)
                        
                        # Update the file path
                        gsm_file = str(new_gsm_file)
                        workflow_data["gsm_metadata_file"] = gsm_file
                        files_created[-1] = gsm_file  # Update the last added file
                        
                except Exception as e:
                    print(f"⚠️  Warning: Failed to move GSM file to series directory: {e}")
                    print("⚠️  Continuing with original file location...")

            # Step 2: Extract Series ID from GSM metadata
            with self._phase("extract_series_id_from_gsm"):
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

            # Step 3: Extract GSE metadata from SQLite
            with self._phase("extract_gse_metadata"):
                gse_file = self._extract_gse_metadata_optimized(series_id)
            
            # Check if the result is a file path or an error message
            if gse_file.startswith('{') and gse_file.endswith('}'):
                # This is a JSON error message, not a file path
                try:
                    error_data = json.loads(gse_file)
                    if not error_data.get("success", True):
                        error_msg = error_data.get("message", "Unknown error in GSE metadata extraction")
                        print(f"❌ GSE metadata extraction failed: {error_msg}")
                        
                        # Check if this is due to multiple GSE IDs
                        if error_data.get("source") == "multiple_gse_ids":
                            gse_ids = error_data.get("gse_ids", [])
                            print(f"⚠️  Multiple GSE IDs detected: {', '.join(gse_ids)}")
                            print("🔄 Attempting to process each GSE ID individually...")
                            
                            # Try to process the first GSE ID if available
                            if gse_ids:
                                first_gse_id = gse_ids[0]
                                print(f"🔄 Processing first GSE ID: {first_gse_id}")
                                gse_file = extract_gse_metadata_sqlite_impl(
                                    first_gse_id, str(self.session_dir), self.db_path
                                )
                                
                                # Check if this attempt succeeded
                                if gse_file.startswith('{') and gse_file.endswith('}'):
                                    second_error = json.loads(gse_file)
                                    if not second_error.get("success", True):
                                        print(f"❌ Failed to process individual GSE ID {first_gse_id}: {second_error.get('message', 'Unknown error')}")
                                        return WorkflowResult(
                                            success=False,
                                            message=f"Failed to extract GSE metadata: {error_msg}",
                                            errors=[error_msg],
                                            data=workflow_data
                                        )
                                else:
                                    print(f"✅ Successfully extracted metadata for individual GSE ID: {first_gse_id}")
                            else:
                                return WorkflowResult(
                                    success=False,
                                    message=f"Failed to extract GSE metadata: {error_msg}",
                                    errors=[error_msg],
                                    data=workflow_data
                                )
                        else:
                            return WorkflowResult(
                                success=False,
                                message=f"Failed to extract GSE metadata: {error_msg}",
                                errors=[error_msg],
                                data=workflow_data
                            )
                except json.JSONDecodeError:
                    # Not a valid JSON, treat as file path
                    pass
            
            files_created.append(gse_file)
            workflow_data["gse_metadata_file"] = gse_file
            
            # Ensure directories exist
            self._ensure_data_intake_dirs()

            # If create_series_directories is True, move the GSE file to a series subdirectory
            if self.create_series_directories:
                try:
                    # Create the series directory and move the GSE file there
                    series_dir = _get_series_subdirectory(str(self.data_intake_dir), series_id, True)
                    
                    # Move the GSE file to the series directory
                    new_gse_file = series_dir / f"{series_id}_metadata.json"
                    import shutil
                    shutil.move(gse_file, new_gse_file)
                    
                    # Update the file path
                    gse_file = str(new_gse_file)
                    workflow_data["gse_metadata_file"] = gse_file
                    files_created[-1] = gse_file  # Update the last added file
                    
                except Exception as e:
                    print(f"⚠️  Warning: Failed to move GSE file to series directory: {e}")
                    print("⚠️  Continuing with original file location...")

            # Step 4: Extract PubMed ID from GSE metadata
            with self._phase("extract_pubmed_id_from_gse"):
                pmid_result = extract_pubmed_id_from_gse_metadata_sqlite_impl(gse_file)
            pmid_data = json.loads(pmid_result)
            if not pmid_data.get("success", False):
                workflow_data["pmid_extraction_error"] = pmid_data.get("message", "Unknown error")
            else:
                pmid = pmid_data.get("pubmed_id")
                if pmid:
                    workflow_data["pmid"] = pmid

                # Step 5: Extract paper abstract from SQLite
                try:
                    with self._phase("extract_paper_abstract"):
                        paper_file = extract_paper_abstract_sqlite_impl(
                            pmid, str(self.session_dir), self.db_path
                        )
                    files_created.append(paper_file)
                    workflow_data["paper_metadata_file"] = paper_file
                    
                    # If create_series_directories is True, move the paper file to a series subdirectory
                    if self.create_series_directories:
                        try:
                            # Move the paper file to the series directory
                            series_dir = _get_series_subdirectory(str(self.data_intake_dir), series_id, True)
                            new_paper_file = series_dir / f"PMID_{pmid}_metadata.json"
                            import shutil
                            shutil.move(paper_file, new_paper_file)
                            
                            # Update the file path
                            paper_file = str(new_paper_file)
                            workflow_data["paper_metadata_file"] = paper_file
                            files_created[-1] = paper_file  # Update the last added file
                            
                        except Exception as e:
                            print(f"⚠️  Warning: Failed to move paper file to series directory: {e}")
                            print("⚠️  Continuing with original file location...")
                            
                except Exception as e:
                    print(
                        f"⚠️  Warning: Failed to extract paper abstract for PMID {pmid}: {e}"
                    )
                    print("⚠️  Continuing workflow without paper abstract...")
                    workflow_data["paper_extraction_error"] = str(e)

            # Note: Mapping generation deferred to end of workflow for efficiency

            # Step 6: Populate data_intake/raw_data/<GSM>/ structure
            try:
                gsm_raw_dir = self.data_intake_raw_dir / gsm_id
                gsm_raw_dir.mkdir(parents=True, exist_ok=True)

                # sample_metadata.json
                try:
                    with open(gsm_file, 'r', encoding='utf-8') as f:
                        gsm_metadata_json = json.load(f)
                    with open(gsm_raw_dir / 'sample_metadata.json', 'w', encoding='utf-8') as f:
                        json.dump(gsm_metadata_json, f, indent=2)
                except Exception as e:
                    print(f"⚠️  Failed writing sample_metadata.json for {gsm_id}: {e}")

                # series_metadata.json
                try:
                    with open(gse_file, 'r', encoding='utf-8') as f:
                        gse_metadata_json = json.load(f)
                    with open(gsm_raw_dir / 'series_metadata.json', 'w', encoding='utf-8') as f:
                        json.dump(gse_metadata_json, f, indent=2)
                except Exception as e:
                    print(f"⚠️  Failed writing series_metadata.json for {gsm_id}: {e}")

                # abstract_metadata.json: aggregate all PMID_* files under series directory
                try:
                    series_dir_path = self.data_intake_dir / series_id
                    pmid_files = sorted(series_dir_path.glob('PMID_*_metadata.json'))
                    pmid_entries = []
                    for pf in pmid_files:
                        try:
                            with open(pf, 'r', encoding='utf-8') as f:
                                pmid_entries.append(json.load(f))
                        except Exception as e:
                            print(f"⚠️  Failed reading {pf}: {e}")
                    abstract_payload = {"pmids": pmid_entries}
                    with open(gsm_raw_dir / 'abstract_metadata.json', 'w', encoding='utf-8') as f:
                        json.dump(abstract_payload, f, indent=2)
                except Exception as e:
                    print(f"⚠️  Failed writing abstract_metadata.json for {gsm_id}: {e}")
            except Exception as e:
                print(f"⚠️  Failed populating raw_data for {gsm_id}: {e}")

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
            with self._phase("extract_gse_metadata"):
                gse_file = self._extract_gse_metadata_optimized(gse_id)
            
            # Check if the result is a file path or an error message
            if gse_file.startswith('{') and gse_file.endswith('}'):
                # This is a JSON error message, not a file path
                try:
                    error_data = json.loads(gse_file)
                    if not error_data.get("success", True):
                        error_msg = error_data.get("message", "Unknown error in GSE metadata extraction")
                        print(f"❌ GSE metadata extraction failed: {error_msg}")
                        
                        # Check if this is due to multiple GSE IDs
                        if error_data.get("source") == "multiple_gse_ids":
                            gse_ids = error_data.get("gse_ids", [])
                            print(f"⚠️  Multiple GSE IDs detected: {', '.join(gse_ids)}")
                            print("🔄 Attempting to process each GSE ID individually...")
                            
                            # Try to process the first GSE ID if available
                            if gse_ids:
                                first_gse_id = gse_ids[0]
                                print(f"🔄 Processing first GSE ID: {first_gse_id}")
                                gse_file = extract_gse_metadata_sqlite_impl(
                                    first_gse_id, str(self.session_dir), self.db_path
                                )
                                
                                # Check if this attempt succeeded
                                if gse_file.startswith('{') and gse_file.endswith('}'):
                                    second_error = json.loads(gse_file)
                                    if not second_error.get("success", True):
                                        print(f"❌ Failed to process individual GSE ID {first_gse_id}: {second_error.get('message', 'Unknown error')}")
                                        return WorkflowResult(
                                            success=False,
                                            message=f"Failed to extract GSE metadata: {error_msg}",
                                            errors=[error_msg],
                                            data=workflow_data
                                        )
                                else:
                                    print(f"✅ Successfully extracted metadata for individual GSE ID: {first_gse_id}")
                            else:
                                return WorkflowResult(
                                    success=False,
                                    message=f"Failed to extract GSE metadata: {error_msg}",
                                    errors=[error_msg],
                                    data=workflow_data
                                )
                        else:
                            return WorkflowResult(
                                success=False,
                                message=f"Failed to extract GSE metadata: {error_msg}",
                                errors=[error_msg],
                                data=workflow_data
                            )
                except json.JSONDecodeError:
                    # Not a valid JSON, treat as file path
                    pass
            
            files_created.append(gse_file)
            workflow_data["gse_metadata_file"] = gse_file
            print(f"✅ GSE metadata extracted: {gse_file}")
            
            # If create_series_directories is True, move the GSE file to a series subdirectory
            if self.create_series_directories:
                try:
                    # Create the series directory and move the GSE file there
                    series_dir = _get_series_subdirectory(str(self.data_intake_dir), gse_id, True)
                    
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
            with self._phase("extract_pubmed_id_from_gse"):
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
                        with self._phase("extract_paper_abstract"):
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
                    # No PMID found, continue without paper abstract
                    pass

            # Note: Mapping generation deferred to end of workflow for efficiency

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
                with self._phase("extract_paper_abstract"):
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

            # Validate inputs
            validation_result = self._validate_inputs(geo_ids)
            if not validation_result.success:
                return validation_result

            all_files_created = []
            all_workflow_data = []
            all_sample_ids = []

            # Process GSM IDs using batch optimization
            if len(geo_ids["gsm_ids"]) > 0:
                print(f"🚀 Processing {len(geo_ids['gsm_ids'])} GSM IDs with batch optimization")
                gsm_results = self._batch_extract_gsm_metadata(geo_ids["gsm_ids"])
                
                for gsm_id in geo_ids["gsm_ids"]:
                    if gsm_id in gsm_results:
                        # Create workflow data for this GSM
                        workflow_data = {
                            "gsm_id": gsm_id,
                            "gsm_metadata_file": gsm_results[gsm_id]
                        }
                        all_files_created.append(gsm_results[gsm_id])
                        all_workflow_data.append(workflow_data)
                        all_sample_ids.append(gsm_id)
                    else:
                        print(f"❌ Failed to process GSM {gsm_id}")
                        return WorkflowResult(
                            success=False,
                            message=f"Failed to extract metadata for GSM {gsm_id}",
                            errors=[f"GSM {gsm_id} extraction failed"]
                        )

            # Process GSE IDs using batch optimization
            if len(geo_ids["gse_ids"]) > 0:
                print(f"🚀 Processing {len(geo_ids['gse_ids'])} GSE IDs with batch optimization")
                gse_results = self._batch_extract_gse_metadata(geo_ids["gse_ids"])
                
                for gse_id in geo_ids["gse_ids"]:
                    if gse_id in gse_results:
                        # Create workflow data for this GSE
                        workflow_data = {
                            "gse_id": gse_id,
                            "gse_metadata_file": gse_results[gse_id]
                        }
                        all_files_created.append(gse_results[gse_id])
                        all_workflow_data.append(workflow_data)
                    else:
                        print(f"❌ Failed to process GSE {gse_id}")
                        return WorkflowResult(
                            success=False,
                            message=f"Failed to extract metadata for GSE {gse_id}",
                            errors=[f"GSE {gse_id} extraction failed"]
                        )

            # Process PMID IDs
            if len(geo_ids["pmid_ids"]) > 0:
                for pmid in tqdm(geo_ids["pmid_ids"], desc="Data Intake SQL - PMID", unit="pmid"):
                    result = self._extract_pmid_workflow(pmid)
                    if not result.success:
                        return result
                    all_files_created.extend(result.files_created or [])
                    all_workflow_data.append(result.data)


            final = WorkflowResult(
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
            self._flush_profile({"entity": "ingestion"})
            return final

        except Exception as e:
            return WorkflowResult(
                success=False,
                message=f"SQLite ingestion workflow failed: {str(e)}",
                errors=[str(e)],
            )

    def run_linker_workflow(
        self, sample_ids: List[str], fields_to_remove: List[str] = None, max_workers: int = None
    ) -> WorkflowResult:
        """
        Run the complete linker workflow for multiple samples with optional parallelization.

        Parameters
        ----------
        sample_ids : List[str]
            List of sample IDs to process
        fields_to_remove : List[str], optional
            Fields to remove during cleaning
        max_workers : int, optional
            Maximum number of worker threads for parallel processing

        Returns
        -------
        WorkflowResult
            Complete linker workflow result
        """
        try:
            all_results = []
            all_files_created = []
            failed_samples = []

            if max_workers and max_workers > 1:
                # Parallel processing using ThreadPoolExecutor
                import concurrent.futures
                import threading
                
                results_lock = threading.Lock()
                
                def process_sample(sample_id):
                    result = self._link_sample_data(sample_id, fields_to_remove)
                    with results_lock:
                        if not result.success:
                            print(f"⚠️  Failed to process sample {sample_id}: {result.message}")
                            failed_samples.append(sample_id)
                            return None
                        return result
                
                print(f"🚀 Processing {len(sample_ids)} samples with {max_workers} workers")
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all tasks
                    future_to_sample = {
                        executor.submit(process_sample, sample_id): sample_id 
                        for sample_id in sample_ids
                    }
                    
                    # Process results with progress bar
                    with tqdm(total=len(sample_ids), desc="Data Intake SQL - Linking samples", unit="sample") as pbar:
                        for future in concurrent.futures.as_completed(future_to_sample):
                            sample_id = future_to_sample[future]
                            try:
                                result = future.result()
                                if result:
                                    all_results.append(result.data)
                                    all_files_created.extend(result.files_created or [])
                            except Exception as e:
                                print(f"⚠️  Exception processing sample {sample_id}: {e}")
                                with results_lock:
                                    failed_samples.append(sample_id)
                            finally:
                                pbar.update(1)
            else:
                # Sequential processing (original behavior)
                for sample_id in tqdm(sample_ids, desc="Data Intake SQL - Linking samples", unit="sample"):
                    result = self._link_sample_data(sample_id, fields_to_remove)
                    if not result.success:
                        print(f"⚠️  Failed to process sample {sample_id}: {result.message}")
                        failed_samples.append(sample_id)
                        continue  # Continue with other samples instead of failing entire workflow
                    all_results.append(result.data)
                    all_files_created.extend(result.files_created or [])


            successful_samples = len(sample_ids) - len(failed_samples)
            message = f"SQLite linker workflow completed: {successful_samples}/{len(sample_ids)} samples processed successfully"
            if failed_samples:
                message += f". Failed samples: {failed_samples}"
            
            final = WorkflowResult(
                success=True,
                message=message,
                data={
                    "sample_results": all_results,
                    "sample_ids": [sid for sid in sample_ids if sid not in failed_samples],  # Only include successful samples
                    "failed_samples": failed_samples,
                    "session_dir": str(self.session_dir),
                },
                files_created=all_files_created,
            )
            self._flush_profile({"entity": "linker", "requested_samples": len(sample_ids)})
            return final

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

            # Parse GEO IDs
            geo_ids = self._parse_geo_ids(input_text)

            # Validate inputs
            validation_result = self._validate_inputs(geo_ids)
            if not validation_result.success:
                return validation_result

            all_files_created = []
            all_workflow_data = []
            all_sample_ids = []

            # Process GSM IDs using batch optimization
            if len(geo_ids["gsm_ids"]) > 0:
                print(f"🚀 Processing {len(geo_ids['gsm_ids'])} GSM IDs with batch optimization")
                with self._phase("batch_ingest_gsms"):
                    gsm_results = self._batch_extract_gsm_metadata(geo_ids["gsm_ids"])
                
                # Collect series IDs from extracted GSM metadata to extract GSE metadata
                series_ids_to_extract = set()
                
                for gsm_id in geo_ids["gsm_ids"]:
                    if gsm_id in gsm_results:
                        # Create workflow data for this GSM
                        workflow_data = {
                            "gsm_id": gsm_id,
                            "gsm_metadata_file": gsm_results[gsm_id]
                        }
                        all_files_created.append(gsm_results[gsm_id])
                        all_workflow_data.append(workflow_data)
                        all_sample_ids.append(gsm_id)
                        
                        # Extract series ID from GSM metadata to queue for GSE extraction
                        try:
                            import json
                            with open(gsm_results[gsm_id]) as f:
                                gsm_data = json.load(f)
                            series_id = gsm_data.get("attributes", {}).get("series_id")
                            if series_id:
                                series_ids_to_extract.add(series_id)
                        except Exception as e:
                            print(f"⚠️  Could not extract series_id from {gsm_id}: {e}")
                    else:
                        print(f"❌ Failed to process GSM {gsm_id}")
                        return LinkerOutput(
                            success=False,
                            message=f"Failed to extract metadata for GSM {gsm_id}",
                            execution_time_seconds=0.0,
                            sample_ids_requested=[],
                            session_directory=str(self.session_dir),
                            files_created=[],
                            successfully_linked=[],
                            failed_linking=[gsm_id],
                            warnings=[f"GSM {gsm_id} extraction failed"],
                            sample_ids_for_curation=[],
                            recommended_curation_fields=[],
                            fields_removed_during_cleaning=[],
                            linked_data=None,
                            cleaned_metadata_files=None,
                            cleaned_series_metadata=None,
                            cleaned_sample_metadata=None,
                            cleaned_abstract_metadata=None,
                        )
                
                # Extract GSE metadata for all series found in GSM data
                if series_ids_to_extract:
                    print(f"🚀 Extracting GSE metadata for {len(series_ids_to_extract)} series: {', '.join(sorted(series_ids_to_extract))}")
                    with self._phase("batch_ingest_derived_gses"):
                        gse_results = self._batch_extract_gse_metadata(list(series_ids_to_extract))
                    
                    for gse_id in series_ids_to_extract:
                        if gse_id in gse_results:
                            all_files_created.append(gse_results[gse_id])
                            print(f"✅ Extracted GSE metadata: {gse_id}")
                        else:
                            print(f"⚠️  Failed to extract GSE metadata: {gse_id}")

            # Process GSE IDs using batch optimization
            if len(geo_ids["gse_ids"]) > 0:
                print(f"🚀 Processing {len(geo_ids['gse_ids'])} GSE IDs with batch optimization")
                with self._phase("batch_ingest_gses"):
                    gse_results = self._batch_extract_gse_metadata(geo_ids["gse_ids"])
                
                for gse_id in geo_ids["gse_ids"]:
                    if gse_id in gse_results:
                        # Create workflow data for this GSE
                        workflow_data = {
                            "gse_id": gse_id,
                            "gse_metadata_file": gse_results[gse_id]
                        }
                        all_files_created.append(gse_results[gse_id])
                        all_workflow_data.append(workflow_data)
                    else:
                        print(f"❌ Failed to process GSE {gse_id}")
                        return LinkerOutput(
                            success=False,
                            message=f"Failed to extract metadata for GSE {gse_id}",
                            execution_time_seconds=0.0,
                            sample_ids_requested=[],
                            session_directory=str(self.session_dir),
                            files_created=[],
                            successfully_linked=[],
                            failed_linking=[gse_id],
                            warnings=[f"GSE {gse_id} extraction failed"],
                            sample_ids_for_curation=[],
                            recommended_curation_fields=[],
                            fields_removed_during_cleaning=[],
                            linked_data=None,
                            cleaned_metadata_files=None,
                            cleaned_series_metadata=None,
                            cleaned_sample_metadata=None,
                            cleaned_abstract_metadata=None,
                        )

            # Process PMID IDs (keep sequential for now since they're typically fewer)
            for pmid in tqdm(geo_ids["pmid_ids"], desc="Data Intake SQL - PMID", unit="pmid"):
                with self._phase("ingest_pmid"):
                    result = self._extract_pmid_workflow(pmid)
                if not result.success:
                    return LinkerOutput(
                        success=False,
                        message=f"Failed to extract metadata for PMID {pmid}",
                        execution_time_seconds=0.0,
                        sample_ids_requested=[],
                        session_directory=str(self.session_dir),
                        files_created=[],
                        successfully_linked=[],
                        failed_linking=[str(pmid)],
                        warnings=[f"PMID {pmid} extraction failed"],
                        sample_ids_for_curation=[],
                        recommended_curation_fields=[],
                        fields_removed_during_cleaning=[],
                        linked_data=None,
                        cleaned_metadata_files=None,
                        cleaned_series_metadata=None,
                        cleaned_sample_metadata=None,
                        cleaned_abstract_metadata=None,
                    )
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

            # Generate series-sample mapping once for all samples
            with self._phase("create_series_sample_mapping"):
                # Create mapping by scanning the session directory (where metadata files are saved)
                create_series_sample_mapping_sqlite_impl(str(self.session_dir), self.db_path)
                mapping_path = self.session_dir / "series_sample_mapping.json"
                
                # Update mapping to include all ingested samples to fix missing reverse mappings
                try:
                    import json
                    with open(mapping_path, 'r') as f:
                        mapping_data = json.load(f)
                    
                    # Add all processed GSMs to the mapping based on their actual file locations
                    for gsm_id in all_sample_ids:
                        # Find which GSE directory contains this GSM
                        for gse_dir in self.session_dir.glob("GSE*"):
                            gsm_file = gse_dir / f"{gsm_id}_metadata.json"
                            if gsm_file.exists():
                                gse_id = gse_dir.name
                                # Add to mapping
                                if gse_id not in mapping_data["mapping"]:
                                    mapping_data["mapping"][gse_id] = []
                                if gsm_id not in mapping_data["mapping"][gse_id]:
                                    mapping_data["mapping"][gse_id].append(gsm_id)
                                # Add to reverse mapping
                                mapping_data["reverse_mapping"][gsm_id] = gse_id
                                break
                    
                    # Update totals
                    mapping_data["total_series"] = len(mapping_data["mapping"])
                    total_samples = sum(len(samples) for samples in mapping_data["mapping"].values())
                    mapping_data["total_samples"] = total_samples
                    
                    # Save updated mapping
                    with open(mapping_path, 'w') as f:
                        json.dump(mapping_data, f, indent=2)
                    
                    print(f"✅ Updated mapping with {len(all_sample_ids)} ingested samples")
                except Exception as e:
                    print(f"⚠️  Warning: Failed to update mapping with ingested samples: {e}")
                
                # Ensure a copy exists under data_intake for LinkerTools, which expects it there
                try:
                    self.data_intake_dir.mkdir(parents=True, exist_ok=True)
                    di_mapping_path = self.data_intake_dir / "series_sample_mapping.json"
                    import shutil
                    shutil.copyfile(mapping_path, di_mapping_path)
                    all_files_created.extend([str(mapping_path), str(di_mapping_path)])
                    print(f"✅ Series-sample mapping saved: {mapping_path}")
                    print(f"✅ Series-sample mapping copied to: {di_mapping_path}")
                except Exception as e:
                    print(f"⚠️  Warning: Failed to copy mapping into data_intake dir: {e}")
                    all_files_created.append(str(mapping_path))

            # Run linker workflow (after mapping is updated)
            # Use max_workers for parallel linking if available
            max_workers = getattr(self, 'max_workers', None)
            linker_result = self.run_linker_workflow(all_sample_ids, fields_to_remove, max_workers)
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

            print(f"✅ Complete SQLite workflow completed successfully in {execution_time:.2f} seconds\n\n")
            
            # Close database manager to clean up resources
            self._close_db_manager()

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
    db_path: str = "data/GEOmetadb.sqlite",
    enable_profiling: bool = False,
    max_workers: int = None,
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
        db_path=db_path,
        enable_profiling=enable_profiling,
        max_workers=max_workers,
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
        print("Success: True")
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
  python data_intake_sql.py --input "Extract metadata for GSM1000981" --type complete --db-path data/GEOmetadb.sqlite
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
        default="data/GEOmetadb.sqlite",
        help="Path to the GEOmetadb SQLite database (default: data/GEOmetadb.sqlite)",
    )

    parser.add_argument(
        "--profile",
        action="store_true",
        help="Enable per-step profiling and write intake_profile.json",
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
            enable_profiling=args.profile,
        )

        # Output result
        if args.json:
            # Output as JSON
            output_data = result.model_dump() if hasattr(result, "model_dump") else {
                "success": getattr(result, "success", None),
                "message": getattr(result, "message", None),
                "execution_time_seconds": getattr(result, "execution_time_seconds", None),
                "sample_ids_requested": getattr(result, "sample_ids_requested", None),
                "session_directory": getattr(result, "session_directory", None),
                "files_created": getattr(result, "files_created", None),
                "successfully_linked": getattr(result, "successfully_linked", None),
                "failed_linking": getattr(result, "failed_linking", None),
                "warnings": getattr(result, "warnings", None),
                "sample_ids_for_curation": getattr(result, "sample_ids_for_curation", None),
                "recommended_curation_fields": getattr(result, "recommended_curation_fields", None),
                "fields_removed_during_cleaning": getattr(result, "fields_removed_during_cleaning", None),
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