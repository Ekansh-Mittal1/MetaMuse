"""
LinkerAgent tools for processing and linking metadata files.

This module provides tools for the LinkerAgent to process metadata files
created by the IngestionAgent, including cleaning files, downloading series
matrix data, and extracting sample-specific information.
"""

import json
import gzip
import urllib.request
import urllib.error
import traceback
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class LinkerResult:
    """Result structure for LinkerAgent operations."""

    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    files_created: Optional[List[str]] = None


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
            Result containing mapping data or error information
        """
        try:
            if not self.mapping_file.exists():
                return LinkerResult(
                    success=False,
                    message=f"Mapping file not found: {self.mapping_file}",
                )

            with open(self.mapping_file, "r") as f:
                mapping_data = json.load(f)

            return LinkerResult(
                success=True,
                message="Mapping file loaded successfully",
                data=mapping_data,
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

        mapping_data = mapping_result.data

        # Check reverse mapping first
        if (
            "reverse_mapping" in mapping_data
            and sample_id in mapping_data["reverse_mapping"]
        ):
            series_id = mapping_data["reverse_mapping"][sample_id]
            series_dir = self.session_dir / series_id

            if series_dir.exists():
                return LinkerResult(
                    success=True,
                    message=f"Found directory for sample {sample_id}",
                    data={
                        "sample_id": sample_id,
                        "series_id": series_id,
                        "directory": str(series_dir),
                    },
                )

        return LinkerResult(
            success=False, message=f"Sample {sample_id} not found in mapping"
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

    def download_series_matrix(self, sample_id: str) -> LinkerResult:
        """
        Download the smallest series matrix file for a sample.

        Parameters
        ----------
        sample_id : str
            The sample ID to process

        Returns
        -------
        LinkerResult
            Result containing downloaded file path and metadata
        """
        dir_result = self.find_sample_directory(sample_id)
        if not dir_result.success:
            return dir_result

        series_dir = Path(dir_result.data["directory"])
        series_id = dir_result.data["series_id"]

        # Load series matrix metadata
        series_matrix_file = series_dir / f"{series_id}_series_matrix.json"
        if not series_matrix_file.exists():
            return LinkerResult(
                success=False,
                message=f"Series matrix metadata file not found: {series_matrix_file}",
            )

        with open(series_matrix_file, "r") as f:
            matrix_data = json.load(f)

        if "file_links" not in matrix_data or not matrix_data["file_links"]:
            return LinkerResult(
                success=False, message="No file links found in series matrix metadata"
            )

        # Find the smallest file (assume first one is smallest for now)
        file_url = matrix_data["file_links"][0]
        file_name = matrix_data["available_files"][0]

        # Download the file
        download_path = series_dir / file_name

        try:
            urllib.request.urlretrieve(file_url, download_path)

            return LinkerResult(
                success=True,
                message=f"Downloaded series matrix file: {file_name}",
                data={
                    "file_path": str(download_path),
                    "file_name": file_name,
                    "file_url": file_url,
                },
            )

        except Exception as e:
            error_msg = f"Error downloading file: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ LINKER ERROR: {error_msg}")
            # Also print to stderr for better visibility
            import sys

            print(f"❌ LINKER ERROR: {error_msg}", file=sys.stderr)
            traceback.print_exc()
            raise

    def extract_matrix_metadata(self, sample_id: str) -> LinkerResult:
        """
        Extract metadata from the top of a series matrix file (prefixed with !).

        Parameters
        ----------
        sample_id : str
            The sample ID to process

        Returns
        -------
        LinkerResult
            Result containing extracted metadata
        """
        # First download the file if not already downloaded
        download_result = self.download_series_matrix(sample_id)
        if not download_result.success:
            return download_result

        file_path = Path(download_result.data["file_path"])

        try:
            # Handle gzipped files
            if file_path.suffix == ".gz":
                with gzip.open(file_path, "rt") as f:
                    content = f.read()
            else:
                with open(file_path, "r") as f:
                    content = f.read()

            # Extract metadata lines (those starting with !)
            metadata_lines = []
            for line in content.split("\n"):
                if line.startswith("!"):
                    metadata_lines.append(line)
                elif line.strip() and not line.startswith("!"):
                    # Stop when we hit non-metadata content
                    break

            # Parse metadata into structured format
            metadata = {}
            for line in metadata_lines:
                if "=" in line:
                    # Remove the ! prefix and split on =
                    clean_line = line[1:].strip()
                    if "=" in clean_line:
                        key, value = clean_line.split("=", 1)
                        key = key.strip().strip('"')
                        value = value.strip().strip('"')
                        metadata[key] = value

            return LinkerResult(
                success=True,
                message=f"Extracted {len(metadata)} metadata fields from series matrix",
                data=metadata,
            )

        except Exception as e:
            error_msg = f"Error extracting metadata: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ LINKER ERROR: {error_msg}")
            # Also print to stderr for better visibility
            import sys

            print(f"❌ LINKER ERROR: {error_msg}", file=sys.stderr)
            traceback.print_exc()
            raise

    def extract_sample_metadata(self, sample_id: str) -> LinkerResult:
        """
        Extract metadata for a specific sample from the series matrix table.

        Parameters
        ----------
        sample_id : str
            The sample ID to extract data for

        Returns
        -------
        LinkerResult
            Result containing sample-specific metadata
        """
        # First download the file if not already downloaded
        download_result = self.download_series_matrix(sample_id)
        if not download_result.success:
            return download_result

        file_path = Path(download_result.data["file_path"])

        try:
            # Handle gzipped files
            if file_path.suffix == ".gz":
                with gzip.open(file_path, "rt") as f:
                    content = f.read()
            else:
                with open(file_path, "r") as f:
                    content = f.read()

            lines = content.split("\n")

            # Find the data section (after metadata lines)
            data_start = 0
            for i, line in enumerate(lines):
                if not line.startswith("!") and line.strip():
                    data_start = i
                    break

            if data_start == 0:
                return LinkerResult(
                    success=False,
                    message="Could not find data section in series matrix file",
                )

            # Parse the data section
            data_lines = lines[data_start:]
            if not data_lines:
                return LinkerResult(
                    success=False, message="No data lines found in series matrix file"
                )

            # First line should be headers
            headers = data_lines[0].split("\t")

            # Find the column for our sample
            sample_column = None
            for i, header in enumerate(headers):
                if sample_id in header:
                    sample_column = i
                    break

            if sample_column is None:
                return LinkerResult(
                    success=False,
                    message=f"Sample {sample_id} not found in series matrix columns",
                )

            # Extract data for this sample
            sample_data = {}
            for line in data_lines[1:]:  # Skip header line
                if line.strip():
                    values = line.split("\t")
                    if len(values) > sample_column:
                        # First column is usually the probe/gene ID
                        probe_id = values[0] if values else ""
                        sample_value = (
                            values[sample_column] if len(values) > sample_column else ""
                        )
                        if probe_id and sample_value:
                            sample_data[probe_id] = sample_value

            return LinkerResult(
                success=True,
                message=f"Extracted data for {len(sample_data)} probes/genes for sample {sample_id}",
                data={
                    "sample_id": sample_id,
                    "column_index": sample_column,
                    "data": sample_data,
                },
            )

        except Exception as e:
            error_msg = f"Error extracting sample metadata: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ LINKER ERROR: {error_msg}")
            # Also print to stderr for better visibility
            import sys

            print(f"❌ LINKER ERROR: {error_msg}", file=sys.stderr)
            traceback.print_exc()
            raise

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


def download_series_matrix_impl(sample_id: str, session_dir: str) -> Dict[str, Any]:
    """
    Download the smallest series matrix file for a sample.

    Parameters
    ----------
    sample_id : str
        The sample ID to process
    session_dir : str
        Path to the session directory

    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and download info
    """
    tools = LinkerTools(session_dir)
    result = tools.download_series_matrix(sample_id)
    return {"success": result.success, "message": result.message, "data": result.data}


def extract_matrix_metadata_impl(sample_id: str, session_dir: str) -> Dict[str, Any]:
    """
    Extract metadata from the top of a series matrix file (prefixed with !).

    Parameters
    ----------
    sample_id : str
        The sample ID to process
    session_dir : str
        Path to the session directory

    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and extracted metadata
    """
    tools = LinkerTools(session_dir)
    result = tools.extract_matrix_metadata(sample_id)
    return {"success": result.success, "message": result.message, "data": result.data}


def extract_sample_metadata_impl(sample_id: str, session_dir: str) -> Dict[str, Any]:
    """
    Extract metadata for a specific sample from the series matrix table.

    Parameters
    ----------
    sample_id : str
        The sample ID to extract data for
    session_dir : str
        Path to the session directory

    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and sample metadata
    """
    tools = LinkerTools(session_dir)
    result = tools.extract_sample_metadata(sample_id)
    return {"success": result.success, "message": result.message, "data": result.data}


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
