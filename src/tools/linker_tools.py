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
    GSMMetadata,
    GSEMetadata,
    PMIDMetadata,
    LinkedData,
    SeriesSampleMapping,
    ModelSerializer,
    create_success_result,
    create_error_result
)


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
        self.mapping_file = self.session_dir / "series_sample_mapping.json"

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
                    session_id=getattr(self, 'session_id', None)
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
                    session_id=getattr(self, 'session_id', None)
                )

            return create_success_result(
                LinkerResult,
                "Mapping file loaded successfully",
                data={"mapping": mapping_obj},  # Pass the Pydantic object directly
                session_id=getattr(self, 'session_id', None)
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
        mapping_obj = mapping_result.data["mapping"]

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
                    session_id=getattr(self, 'session_id', None)
                )

        return create_error_result(
            LinkerResult,
            f"Sample {sample_id} not found in mapping",
            session_id=getattr(self, 'session_id', None)
        )

    def clean_metadata_files(
        self, sample_id: str, fields_to_remove: List[str] = None
    ) -> LinkerResult:
        """
        Generate cleaned versions of metadata files by removing specified fields.

        Parameters
        ----------
        sample_id : str
            The sample ID to process
        fields_to_remove : List[str], optional
            List of fields to remove from metadata files

        Returns
        -------
        LinkerResult
            Result containing paths to cleaned files
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

            # Clean series metadata file
            series_metadata_file = series_dir / f"{series_id}_metadata.json"
            if series_metadata_file.exists():
                cleaned_series_file = cleaned_dir / f"{series_id}_metadata_cleaned.json"
                self._clean_json_file(
                    series_metadata_file, cleaned_series_file, fields_to_remove
                )
                files_created.append(str(cleaned_series_file))

            # Clean sample metadata file (GSM file)
            sample_metadata_file = series_dir / f"{sample_id}_metadata.json"
            if sample_metadata_file.exists():
                cleaned_sample_file = cleaned_dir / f"{sample_id}_metadata_cleaned.json"
                self._clean_json_file(
                    sample_metadata_file, cleaned_sample_file, fields_to_remove
                )
                files_created.append(str(cleaned_sample_file))

            # Clean abstract metadata file
            abstract_files = list(series_dir.glob("PMID_*_metadata.json"))
            if abstract_files:
                abstract_file = abstract_files[0]  # Take the first one
                cleaned_abstract_file = (
                    cleaned_dir / f"{abstract_file.stem}_cleaned.json"
                )
                self._clean_json_file(
                    abstract_file, cleaned_abstract_file, fields_to_remove
                )
                files_created.append(str(cleaned_abstract_file))

            # Clean series matrix metadata file
            series_matrix_file = series_dir / f"{series_id}_series_matrix.json"
            if series_matrix_file.exists():
                cleaned_matrix_file = (
                    cleaned_dir / f"{series_id}_series_matrix_cleaned.json"
                )
                self._clean_json_file(
                    series_matrix_file, cleaned_matrix_file, fields_to_remove
                )
                files_created.append(str(cleaned_matrix_file))

            return LinkerResult(
                success=True,
                message=f"Cleaned {len(files_created)} metadata files",
                files_created=files_created,
            )
        except Exception as e:
            error_msg = f"Error cleaning metadata files: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ LINKER ERROR: {error_msg}")
            # Also print to stderr for better visibility
            import sys

            print(f"❌ LINKER ERROR: {error_msg}", file=sys.stderr)
            traceback.print_exc()
            raise

    def _clean_json_file(
        self, input_file: Path, output_file: Path, fields_to_remove: List[str]
    ):
        """
        Clean a JSON file by removing specified fields.

        Parameters
        ----------
        input_file : Path
            Path to input JSON file
        output_file : Path
            Path to output cleaned JSON file
        fields_to_remove : List[str]
            List of fields to remove
        """
        try:
            with open(input_file, "r") as f:
                data = json.load(f)

            # Remove specified fields recursively
            self._remove_fields_recursive(data, fields_to_remove)

            with open(output_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            error_msg = f"Error cleaning JSON file {input_file}: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ LINKER ERROR: {error_msg}")
            # Also print to stderr for better visibility
            import sys

            print(f"❌ LINKER ERROR: {error_msg}", file=sys.stderr)
            traceback.print_exc()
            raise

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
            for field in fields_to_remove:
                if field in data:
                    data.pop(field)
            for value in data.values():
                self._remove_fields_recursive(value, fields_to_remove)
        elif isinstance(data, list):
            for item in data:
                self._remove_fields_recursive(item, fields_to_remove)



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

            # Clean metadata files
            clean_result = self.clean_metadata_files(sample_id, fields_to_remove)
            if not clean_result.success:
                return clean_result

            # Load cleaned sample metadata
            series_dir = Path(dir_result.data["directory"])
            cleaned_dir = series_dir / "cleaned"
            cleaned_sample_metadata_file = (
                cleaned_dir / f"{sample_id}_metadata_cleaned.json"
            )
            sample_metadata = {}
            if cleaned_sample_metadata_file.exists():
                with open(cleaned_sample_metadata_file, "r") as f:
                    sample_metadata = json.load(f)
            else:
                # Fallback to original if cleaned doesn't exist
                sample_metadata_file = series_dir / f"{sample_id}_metadata.json"
                if sample_metadata_file.exists():
                    with open(sample_metadata_file, "r") as f:
                        sample_metadata = json.load(f)

            # Package everything together (without series matrix data)
            packaged_data = {
                "sample_id": sample_id,
                "series_id": dir_result.data["series_id"],
                "directory": dir_result.data["directory"],
                "cleaned_files": clean_result.files_created,
                "sample_metadata": sample_metadata,
                "processing_summary": {
                    "cleaned_files_count": len(clean_result.files_created),
                    "note": "Series matrix functionality has been removed from agent access",
                },
            }

            # Save packaged data
            packaged_file = series_dir / f"{sample_id}_linked_data.json"
            with open(packaged_file, "w") as f:
                json.dump(packaged_data, f, indent=2)

            return LinkerResult(
                success=True,
                message=f"Successfully packaged linked data for sample {sample_id} (series matrix functionality removed)",
                data=packaged_data,
                files_created=[str(packaged_file)],
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

        print(
            f"[CLEAN_IMPL] Result: success={result.success}, message={result.message}"
        )

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
