"""
LinkerAgent tools for processing and linking metadata files.

This module provides tools for the LinkerAgent to process metadata files
created by the IngestionAgent, including cleaning files, downloading series
matrix data, and extracting sample-specific information.
"""

import json
import traceback
from pathlib import Path
from typing import Dict, List, Any, Optional

# Import new Pydantic models
from src.models import (
    LinkerResult,
    SeriesSampleMapping,
    create_success_result,
    create_error_result,
    CurationDataPackage,
    CleanedSeriesMetadata,
    CleanedSampleMetadata,
    CleanedAbstractMetadata,
    GSMMetadata,
    GSEMetadata,
    PMIDMetadata,
)
from src.models.common import KeyValue


class LinkerTools:
    """
    Tools for processing and linking metadata files from IngestionAgent output.
    """

    def __init__(self, session_dir: str):
        """
        Initialize LinkerTools with session directory.

        Parameters
        ----------
        session_dir : str
            Path to the session directory containing IngestionAgent output
        """
        self.session_dir = Path(session_dir)
        # Mapping file now expected under preprocessing if session_dir points to batch root
        preprocessing_mapping = self.session_dir / "preprocessing" / "series_sample_mapping.json"
        default_mapping = self.session_dir / "series_sample_mapping.json"
        self.mapping_file = preprocessing_mapping if preprocessing_mapping.exists() else default_mapping

    def load_mapping_file(self) -> LinkerResult:
        """
        Load the series_sample_mapping.json file to understand directory structure.

        Returns
        -------
        LinkerResult
            Result containing validated SeriesSampleMapping object
        """
        try:
            if not self.mapping_file.exists():
                return create_error_result(
                    LinkerResult,
                    f"Mapping file not found: {self.mapping_file}",
                    session_id=getattr(self, "session_id", None),
                )

            with open(self.mapping_file, "r") as f:
                mapping_data = json.load(f)

            # Validate and convert to Pydantic model
            try:
                mapping_obj = SeriesSampleMapping(**mapping_data)
            except Exception as e:
                return create_error_result(
                    LinkerResult,
                    f"Invalid mapping file format: {str(e)}",
                    errors=[f"Validation error: {str(e)}"],
                    session_id=getattr(self, "session_id", None),
                )

            return create_success_result(
                LinkerResult,
                "Mapping file loaded successfully",
                data={"mapping": mapping_obj.model_dump()},  # Pass as dict
                session_id=getattr(self, "session_id", None),
            )

        except Exception as e:
            error_msg = f"Error loading mapping file: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ LINKER ERROR: {error_msg}")
            # Also print to stderr for better visibility
            import sys

            print(f"❌ LINKER ERROR: {error_msg}", file=sys.stderr)
            traceback.print_exc()
            raise

    def find_sample_directory(self, sample_id: str) -> LinkerResult:
        """
        Find the directory containing files for a specific sample ID.

        Parameters
        ----------
        sample_id : str
            The sample ID to find (e.g., GSM1000981)

        Returns
        -------
        LinkerResult
            Result containing directory path or error information
        """
        mapping_result = self.load_mapping_file()
        if not mapping_result.success:
            return mapping_result

        # Extract the SeriesSampleMapping object
        mapping_data = mapping_result.data["mapping"]
        mapping_obj = SeriesSampleMapping(**mapping_data)

        # Check reverse mapping first
        if sample_id in mapping_obj.reverse_mapping:
            series_id = mapping_obj.reverse_mapping[sample_id]
            series_dir = self.session_dir / series_id

            if series_dir.exists():
                return create_success_result(
                    LinkerResult,
                    f"Found directory for sample {sample_id}",
                    data={
                        "sample_id": sample_id,
                        "series_id": series_id,
                        "directory": str(series_dir),
                    },
                    session_id=getattr(self, "session_id", None),
                )

        # Fallback: scan for the GSM file in any GSE directory
        print(f"🔍 Sample {sample_id} not in mapping, scanning directories...")
        for gse_dir in self.session_dir.glob("GSE*"):
            if gse_dir.is_dir():
                gsm_file = gse_dir / f"{sample_id}_metadata.json"
                if gsm_file.exists():
                    print(f"✅ Found {sample_id} in {gse_dir.name} via directory scan")
                    return create_success_result(
                        LinkerResult,
                        f"Found directory for sample {sample_id} via fallback scan",
                        data={
                            "sample_id": sample_id,
                            "series_id": gse_dir.name,
                            "directory": str(gse_dir),
                        },
                        session_id=getattr(self, "session_id", None),
                    )

        return create_error_result(
            LinkerResult,
            f"Sample {sample_id} not found in mapping or directory scan",
            session_id=getattr(self, "session_id", None),
        )

    def clean_metadata_files(
        self, sample_id: str, fields_to_remove: List[str] = None
    ) -> LinkerResult:
        """
        Generate cleaned metadata models and save them to JSON files.

        Parameters
        ----------
        sample_id : str
            The sample ID to process
        fields_to_remove : List[str], optional
            List of fields to remove from metadata files

        Returns
        -------
        LinkerResult
            Result containing cleaned metadata models and file paths
        """
        try:
            if fields_to_remove is None or len(fields_to_remove) == 0:
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
                    "contact_zip_postal_code",
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

            dir_result = self.find_sample_directory(sample_id)
            if not dir_result.success:
                return dir_result

            series_dir = Path(dir_result.data["directory"])
            series_id = dir_result.data["series_id"]
            cleaned_dir = series_dir / "cleaned"
            cleaned_dir.mkdir(exist_ok=True)

            files_created = []
            cleaned_models = {}

            # Clean series metadata file
            series_metadata_file = series_dir / f"{series_id}_metadata.json"
            if series_metadata_file.exists():
                cleaned_series = self._create_cleaned_series_metadata(
                    series_metadata_file, fields_to_remove
                )
                if cleaned_series:
                    cleaned_series_file = (
                        cleaned_dir / f"{series_id}_metadata_cleaned.json"
                    )
                    with open(cleaned_series_file, "w") as f:
                        json.dump(cleaned_series.model_dump(), f, indent=2)
                    files_created.append(str(cleaned_series_file))
                    cleaned_models["series"] = cleaned_series.model_dump()

            # Clean sample metadata file (GSM file)
            sample_metadata_file = series_dir / f"{sample_id}_metadata.json"
            if sample_metadata_file.exists():
                cleaned_sample = self._create_cleaned_sample_metadata(
                    sample_metadata_file, fields_to_remove
                )
                if cleaned_sample:
                    cleaned_sample_file = (
                        cleaned_dir / f"{sample_id}_metadata_cleaned.json"
                    )
                    with open(cleaned_sample_file, "w") as f:
                        json.dump(cleaned_sample.model_dump(), f, indent=2)
                    files_created.append(str(cleaned_sample_file))
                    cleaned_models["sample"] = cleaned_sample.model_dump()

            # Clean abstract metadata file
            abstract_files = list(series_dir.glob("PMID_*_metadata.json"))
            if abstract_files:
                abstract_file = abstract_files[0]  # Take the first one
                cleaned_abstract = self._create_cleaned_abstract_metadata(
                    abstract_file, fields_to_remove
                )
                if cleaned_abstract:
                    cleaned_abstract_file = (
                        cleaned_dir / f"{abstract_file.stem}_cleaned.json"
                    )
                    with open(cleaned_abstract_file, "w") as f:
                        json.dump(cleaned_abstract.model_dump(), f, indent=2)
                    files_created.append(str(cleaned_abstract_file))
                    cleaned_models["abstract"] = cleaned_abstract.model_dump()

            return create_success_result(
                LinkerResult,
                f"Created {len(cleaned_models)} cleaned metadata models and saved {len(files_created)} files",
                data={"cleaned_models": cleaned_models},
                files_created=files_created,
                session_id=getattr(self, "session_id", None),
            )
        except Exception as e:
            error_msg = f"Error cleaning metadata files: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ LINKER ERROR: {error_msg}")
            # Also print to stderr for better visibility
            import sys

            print(f"❌ LINKER ERROR: {error_msg}", file=sys.stderr)
            traceback.print_exc()
            raise

    def _create_cleaned_series_metadata(
        self, metadata_file: Path, fields_to_remove: List[str]
    ) -> Optional[CleanedSeriesMetadata]:
        """
        Create a CleanedSeriesMetadata model from a GSE metadata file.

        Parameters
        ----------
        metadata_file : Path
            Path to the GSE metadata JSON file
        fields_to_remove : List[str]
            List of fields to remove from metadata

        Returns
        -------
        Optional[CleanedSeriesMetadata]
            Cleaned series metadata model or None if failed
        """
        try:
            with open(metadata_file, "r") as f:
                data = json.load(f)

            # Normalize field names before loading
            data = self._normalize_field_names(data)

            # Load as GSE metadata first
            gse_metadata = GSEMetadata(**data)

            # Extract series ID
            series_id = gse_metadata.gse_id

            # Convert attributes to dict and clean
            attributes_dict = gse_metadata.attributes.model_dump()
            cleaned_dict = self._remove_fields_from_dict(
                attributes_dict, fields_to_remove
            )

            # Convert to KeyValue pairs
            content = [
                KeyValue(key=k, value=str(v))
                for k, v in cleaned_dict.items()
                if v is not None
            ]

            return CleanedSeriesMetadata(
                series_id=series_id,
                content=content,
                source_type="series",
                original_file_path=str(metadata_file),
            )
        except Exception as e:
            print(f"❌ Error creating cleaned series metadata: {str(e)}")
            print(f"   File: {metadata_file}")
            print(f"   Error type: {type(e).__name__}")
            return None

    def _create_cleaned_sample_metadata(
        self, metadata_file: Path, fields_to_remove: List[str]
    ) -> Optional[CleanedSampleMetadata]:
        """
        Create a CleanedSampleMetadata model from a GSM metadata file.

        Parameters
        ----------
        metadata_file : Path
            Path to the GSM metadata JSON file
        fields_to_remove : List[str]
            List of fields to remove from metadata

        Returns
        -------
        Optional[CleanedSampleMetadata]
            Cleaned sample metadata model or None if failed
        """
        try:
            with open(metadata_file, "r") as f:
                data = json.load(f)

            # Normalize field names before loading
            data = self._normalize_field_names(data)

            # Load as GSM metadata first
            gsm_metadata = GSMMetadata(**data)

            # Extract sample ID
            sample_id = gsm_metadata.gsm_id

            # Convert attributes to dict and clean
            attributes_dict = gsm_metadata.attributes.model_dump()
            cleaned_dict = self._remove_fields_from_dict(
                attributes_dict, fields_to_remove
            )

            # Convert to KeyValue pairs
            content = [
                KeyValue(key=k, value=str(v))
                for k, v in cleaned_dict.items()
                if v is not None
            ]

            return CleanedSampleMetadata(
                sample_id=sample_id,
                content=content,
                source_type="sample",
                original_file_path=str(metadata_file),
            )
        except Exception as e:
            print(f"❌ Error creating cleaned sample metadata: {str(e)}")
            print(f"   File: {metadata_file}")
            print(f"   Error type: {type(e).__name__}")
            return None

    def _create_cleaned_abstract_metadata(
        self, metadata_file: Path, fields_to_remove: List[str]
    ) -> Optional[CleanedAbstractMetadata]:
        """
        Create a CleanedAbstractMetadata model from a PMID metadata file.

        Parameters
        ----------
        metadata_file : Path
            Path to the PMID metadata JSON file
        fields_to_remove : List[str]
            List of fields to remove from metadata

        Returns
        -------
        Optional[CleanedAbstractMetadata]
            Cleaned abstract metadata model or None if failed
        """
        try:
            with open(metadata_file, "r") as f:
                data = json.load(f)

            # Normalize field names before loading
            data = self._normalize_field_names(data)

            # Load as PMID metadata first
            pmid_metadata = PMIDMetadata(**data)

            # Extract PMID and convert to string
            pmid = str(pmid_metadata.pmid)

            # Convert to dict and clean
            metadata_dict = pmid_metadata.model_dump()
            cleaned_dict = self._remove_fields_from_dict(
                metadata_dict, fields_to_remove
            )

            # Convert to KeyValue pairs
            content = [
                KeyValue(key=k, value=str(v))
                for k, v in cleaned_dict.items()
                if v is not None
            ]

            return CleanedAbstractMetadata(
                pmid=pmid,
                content=content,
                source_type="abstract",
                original_file_path=str(metadata_file),
            )
        except Exception as e:
            print(f"❌ Error creating cleaned abstract metadata: {str(e)}")
            print(f"   File: {metadata_file}")
            print(f"   Error type: {type(e).__name__}")
            return None

    def _remove_fields_from_dict(
        self, data_dict: Dict[str, Any], fields_to_remove: List[str]
    ) -> Dict[str, Any]:
        """
        Remove specified fields from a dictionary.

        Parameters
        ----------
        data_dict : Dict[str, Any]
            Dictionary to clean
        fields_to_remove : List[str]
            List of fields to remove

        Returns
        -------
        Dict[str, Any]
            Cleaned dictionary
        """
        cleaned = data_dict.copy()
        for field in fields_to_remove:
            if field in cleaned:
                cleaned.pop(field)
        return cleaned

    def _remove_fields_recursive(self, data: Any, fields_to_remove: List[str]):
        """
        Recursively remove fields from a data structure.

        Parameters
        ----------
        data : Any
            Data structure to clean
        fields_to_remove : List[str]
            List of fields to remove
        """
        if isinstance(data, dict):
            # Remove fields from current level
            for field in fields_to_remove:
                if field in data:
                    data.pop(field)
            # Recursively process all values
            for value in data.values():
                self._remove_fields_recursive(value, fields_to_remove)
        elif isinstance(data, list):
            for item in data:
                self._remove_fields_recursive(item, fields_to_remove)

    def _normalize_field_names(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize field names to handle variations in data format.

        Parameters
        ----------
        data_dict : Dict[str, Any]
            Dictionary with potentially inconsistent field names

        Returns
        -------
        Dict[str, Any]
            Dictionary with normalized field names
        """
        normalized = data_dict.copy()

        # Handle contact_zip/postal_code field name variations
        if "contact_zip/postal_code" in normalized:
            normalized["contact_zip_postal_code"] = normalized.pop(
                "contact_zip/postal_code"
            )

        # Filter out problematic GSE fields that cause validation errors
        if "attributes" in normalized and isinstance(normalized["attributes"], dict):
            attrs = normalized["attributes"]
            problematic_fields = ["platform_organism", "sample_organism", "sample_taxid", "relation"]
            for field in problematic_fields:
                attrs.pop(field, None)

        return normalized

    def create_curation_data_package(
        self, sample_id: str, fields_to_remove: List[str] = None
    ) -> LinkerResult:
        """
        Create a CurationDataPackage with cleaned metadata from all sources.

        Parameters
        ----------
        sample_id : str
            The sample ID to process
        fields_to_remove : List[str], optional
            List of fields to remove from metadata files

        Returns
        -------
        LinkerResult
            Result containing CurationDataPackage object
        """
        try:
            # First find the sample directory
            directory_result = self.find_sample_directory(sample_id)
            if not directory_result.success:
                return directory_result

            # Get paths
            series_id = directory_result.data["series_id"]
            series_dir = self.session_dir / series_id

            # Set default fields to remove if not provided
            if fields_to_remove is None or len(fields_to_remove) == 0:
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

            # Create cleaned metadata models
            series_metadata = None
            sample_metadata = None
            abstract_metadata = None

            # Create cleaned series metadata
            series_metadata_file = series_dir / f"{series_id}_metadata.json"
            if series_metadata_file.exists():
                # Add sample_id to fields to remove specifically for series metadata
                series_fields_to_remove = fields_to_remove + ["sample_id"] if fields_to_remove else ["sample_id"]
                series_metadata = self._create_cleaned_series_metadata(
                    series_metadata_file, series_fields_to_remove
                )

            # Create cleaned sample metadata
            sample_metadata_file = series_dir / f"{sample_id}_metadata.json"
            if sample_metadata_file.exists():
                sample_metadata = self._create_cleaned_sample_metadata(
                    sample_metadata_file, fields_to_remove
                )

            # Create cleaned abstract metadata
            abstract_files = list(series_dir.glob("PMID_*_metadata.json"))
            if abstract_files:
                abstract_file = abstract_files[0]  # Take the first one
                abstract_metadata = self._create_cleaned_abstract_metadata(
                    abstract_file, fields_to_remove
                )

            # Create the curation data package
            curation_package = CurationDataPackage(
                sample_id=sample_id,
                series_id=series_id,
                series_metadata=series_metadata,
                sample_metadata=sample_metadata,
                abstract_metadata=abstract_metadata,
            )

            return create_success_result(
                LinkerResult,
                f"Created curation data package for {sample_id} with cleaned metadata",
                data={"curation_package": curation_package.model_dump()},
                files_created=[],  # No files created since we clean in memory
                session_id=getattr(self, "session_id", None),
            )

        except Exception as e:
            error_msg = f"Error creating curation data package for {sample_id}: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ LINKER ERROR: {error_msg}")
            return create_error_result(
                LinkerResult,
                f"Error creating curation data package: {str(e)}",
                errors=[str(e)],
                session_id=getattr(self, "session_id", None),
            )

    def process_multiple_samples(
        self, sample_ids: List[str], fields_to_remove: List[str] = None
    ) -> LinkerResult:
        """
        Process multiple sample IDs at once (clean and package for all samples).

        Parameters
        ----------
        sample_ids : List[str]
            List of sample IDs to process
        fields_to_remove : List[str], optional
            List of fields to remove from metadata files

        Returns
        -------
        LinkerResult
            Result containing processing summary for all samples
        """
        try:
            results = []
            all_files_created = []
            successful_samples = []
            failed_samples = []

            print(f"🔧 Processing {len(sample_ids)} samples: {sample_ids}")

            for sample_id in sample_ids:
                print(f"🔧 Processing sample: {sample_id}")

                # Process each sample individually
                result = self.package_linked_data(sample_id, fields_to_remove)

                if result.success:
                    successful_samples.append(sample_id)
                    if result.files_created:
                        all_files_created.extend(result.files_created)
                    results.append(
                        {
                            "sample_id": sample_id,
                            "success": True,
                            "message": result.message,
                            "files_created": result.files_created or [],
                        }
                    )
                else:
                    failed_samples.append(sample_id)
                    results.append(
                        {
                            "sample_id": sample_id,
                            "success": False,
                            "message": result.message,
                            "errors": result.errors or [],
                        }
                    )

            # Create summary
            summary = {
                "total_samples": len(sample_ids),
                "successful_samples": successful_samples,
                "failed_samples": failed_samples,
                "success_rate": len(successful_samples) / len(sample_ids)
                if sample_ids
                else 0,
                "individual_results": results,
            }

            if failed_samples:
                return create_error_result(
                    LinkerResult,
                    f"Processed {len(successful_samples)}/{len(sample_ids)} samples successfully. Failed: {failed_samples}",
                    errors=[f"Failed to process: {failed_samples}"],
                    data={"summary": summary},
                    files_created=all_files_created,
                    session_id=getattr(self, "session_id", None),
                )
            else:
                return create_success_result(
                    LinkerResult,
                    f"Successfully processed all {len(sample_ids)} samples",
                    data={"summary": summary},
                    files_created=all_files_created,
                    session_id=getattr(self, "session_id", None),
                )

        except Exception as e:
            error_msg = f"Error processing multiple samples: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ LINKER ERROR: {error_msg}")
            return create_error_result(
                LinkerResult,
                f"Error processing multiple samples: {str(e)}",
                errors=[str(e)],
                session_id=getattr(self, "session_id", None),
            )

    def package_linked_data(
        self, sample_id: str, fields_to_remove: List[str] = None
    ) -> LinkerResult:
        """
        Package all linked information for a sample into a comprehensive result.

        This method packages cleaned metadata files without requiring series matrix files.
        Series matrix functionality has been removed from agent access and is now legacy.

        Parameters
        ----------
        sample_id : str
            The sample ID to process
        fields_to_remove : List[str], optional
            List of fields to remove from metadata files

        Returns
        -------
        LinkerResult
            Result containing all packaged information
        """
        try:
            # Find sample directory
            dir_result = self.find_sample_directory(sample_id)
            if not dir_result.success:
                return dir_result

            # Set default fields to remove if not provided
            if fields_to_remove is None or len(fields_to_remove) == 0:
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

            # Load and clean sample metadata in memory
            series_dir = Path(dir_result.data["directory"])
            sample_metadata_file = series_dir / f"{sample_id}_metadata.json"
            sample_metadata = {}
            if sample_metadata_file.exists():
                with open(sample_metadata_file, "r") as f:
                    sample_metadata = json.load(f)
                # Clean the content in memory
                self._remove_fields_recursive(sample_metadata, fields_to_remove)

            # Package everything together (without series matrix data)
            packaged_data = {
                "sample_id": sample_id,
                "series_id": dir_result.data["series_id"],
                "directory": dir_result.data["directory"],
                "sample_metadata": sample_metadata,
                "processing_summary": {
                    "cleaned_in_memory": True,
                    "fields_removed": fields_to_remove,
                    "note": "Series matrix functionality has been removed from agent access",
                },
            }

            # Save packaged data
            packaged_file = series_dir / f"{sample_id}_linked_data.json"
            with open(packaged_file, "w") as f:
                json.dump(packaged_data, f, indent=2)

            return create_success_result(
                LinkerResult,
                f"Successfully packaged linked data for sample {sample_id} with cleaned metadata (series matrix functionality removed)",
                data={"packaged_data": packaged_data},
                files_created=[str(packaged_file)],
                session_id=getattr(self, "session_id", None),
            )

        except Exception as e:
            error_msg = f"Error packaging linked data: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ LINKER ERROR: {error_msg}")
            # Also print to stderr for better visibility
            import sys

            print(f"❌ LINKER ERROR: {error_msg}", file=sys.stderr)
            traceback.print_exc()
            raise


# Implementation functions for tool_utils.py
def load_mapping_file_impl(session_dir: str) -> Dict[str, Any]:
    """
    Load the series_sample_mapping.json file.

    Parameters
    ----------
    session_dir : str
        Path to the session directory

    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and data
    """
    tools = LinkerTools(session_dir)
    result = tools.load_mapping_file()
    return {"success": result.success, "message": result.message, "data": result.data}


def find_sample_directory_impl(sample_id: str, session_dir: str) -> Dict[str, Any]:
    """
    Find the directory containing files for a specific sample ID.

    Parameters
    ----------
    sample_id : str
        The sample ID to find
    session_dir : str
        Path to the session directory

    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and directory info
    """
    tools = LinkerTools(session_dir)
    result = tools.find_sample_directory(sample_id)
    return {"success": result.success, "message": result.message, "data": result.data}


def clean_metadata_files_impl(
    sample_id: str, session_dir: str, fields_to_remove: List[str] = None
) -> Dict[str, Any]:
    """
    Generate cleaned versions of metadata files by removing specified fields.

    Parameters
    ----------
    sample_id : str
        The sample ID to process
    session_dir : str
        Path to the session directory
    fields_to_remove : List[str], optional
        List of fields to remove from metadata files

    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and cleaned files info
    """
    try:
        tools = LinkerTools(session_dir)
        result = tools.clean_metadata_files(sample_id, fields_to_remove)

        return {
            "success": result.success,
            "message": result.message,
            "files_created": result.files_created,
        }
    except Exception as e:
        print(f"[CLEAN_IMPL] Exception in clean_metadata_files_impl: {str(e)}")
        print("[CLEAN_IMPL] Full traceback:")
        traceback.print_exc()
        # Re-raise the exception to preserve the traceback
        raise


def create_curation_data_package_impl(
    sample_id: str, session_dir: str, fields_to_remove: List[str] = None
) -> Dict[str, Any]:
    """
    Create a CurationDataPackage with cleaned metadata from all sources.

    Parameters
    ----------
    sample_id : str
        The sample ID to process
    session_dir : str
        Path to the session directory
    fields_to_remove : List[str], optional
        List of fields to remove from metadata files

    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and CurationDataPackage
    """
    tools = LinkerTools(session_dir)
    result = tools.create_curation_data_package(sample_id, fields_to_remove)
    return {
        "success": result.success,
        "message": result.message,
        "data": result.data,
        "files_created": result.files_created,
    }


def process_multiple_samples_impl(
    sample_ids: List[str], session_dir: str, fields_to_remove: List[str] = None
) -> Dict[str, Any]:
    """
    Process multiple sample IDs at once (clean and package for all samples).

    Parameters
    ----------
    sample_ids : List[str]
        List of sample IDs to process
    session_dir : str
        Path to the session directory
    fields_to_remove : List[str], optional
        List of fields to remove from metadata files

    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and processing summary
    """
    tools = LinkerTools(session_dir)
    result = tools.process_multiple_samples(sample_ids, fields_to_remove)
    return {
        "success": result.success,
        "message": result.message,
        "data": result.data,
        "files_created": result.files_created,
    }


def package_linked_data_impl(
    sample_id: str, session_dir: str, fields_to_remove: List[str] = None
) -> Dict[str, Any]:
    """
    Package all linked information for a sample into a comprehensive result.

    Parameters
    ----------
    sample_id : str
        The sample ID to process
    session_dir : str
        Path to the session directory
    fields_to_remove : List[str], optional
        List of fields to remove from metadata files

    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and packaged data
    """
    tools = LinkerTools(session_dir)
    result = tools.package_linked_data(sample_id, fields_to_remove)
    return {
        "success": result.success,
        "message": result.message,
        "data": result.data,
        "files_created": result.files_created,
    }
