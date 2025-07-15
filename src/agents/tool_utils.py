"""
Tool utilities for agents to use GEO metadata extraction functionality.

This module exposes the GEO metadata extraction tools as function tools
that can be used by agents in the system. All logic is delegated to
the actual tool implementations in src.tools.ingestion_tools.
"""

from __future__ import annotations

from pathlib import Path
import os
import json

from agents import function_tool
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import the tool implementations
from src.tools.ingestion_tools import (
    extract_gsm_metadata_impl,
    extract_gse_metadata_impl,
    extract_series_matrix_metadata_impl,
    extract_paper_abstract_impl,
    extract_pubmed_id_from_gse_metadata_impl,
    extract_series_id_from_gsm_metadata_impl,
    validate_geo_inputs_impl,
    create_series_sample_mapping_impl
)

from src.tools.linker_tools import (
    load_mapping_file_impl,
    find_sample_directory_impl,
    clean_metadata_files_impl,
    download_series_matrix_impl,
    extract_matrix_metadata_impl,
    extract_sample_metadata_impl,
    package_linked_data_impl
)


def get_session_tools(session_dir: str | Path) -> list:
    """
    Creates a suite of GEO metadata extraction tools that are bound to a specific session directory.

    This approach avoids redefining tools within agent creation functions and
    provides a centralized, reusable way to create session-specific tools.

    Parameters
    ----------
    session_dir : str or Path
        The directory path for the session.

    Returns
    -------
    list
        A list of session-bound GEO metadata extraction tools.
    """
    session_dir = str(session_dir)
    
    # Get environment variables with defaults
    default_email = os.getenv("NCBI_EMAIL")
    default_api_key = os.getenv("NCBI_API_KEY")
    
    # Validate that we have required configuration
    if not default_email:
        print("Warning: NCBI_EMAIL environment variable is not set. "
              "Tools will use a default email for testing.")
        default_email = "test@example.com"
    
    # Warn if API key is not set (optional but recommended)
    if not default_api_key:
        print("Warning: NCBI_API_KEY environment variable is not set. "
              "This will limit API rate limits to 3 requests per second.")
        default_api_key = None

    @function_tool
    def extract_gsm_metadata(
        gsm_id: str,
        email: str = None,
        api_key: str = None
    ) -> str:
        """
        Extract metadata for a GEO Sample (GSM) record.
        
        This tool retrieves comprehensive metadata for a specific GEO sample,
        including sample characteristics, experimental protocols, and associated
        information.
        
        Parameters
        ----------
        gsm_id : str
            Gene Expression Omnibus sample ID (e.g., "GSM1019742").
        email : str, optional
            Email address for NCBI E-Utils identification.
            If not provided, uses session default.
        api_key : str, optional
            NCBI API key for higher rate limits.
            If not provided, uses session default.
        
        Returns
        -------
        str
            Path to the saved metadata file.
        """
        # Use session defaults if not provided
        if email is None:
            email = default_email
        if api_key is None:
            api_key = default_api_key
        
        return extract_gsm_metadata_impl(gsm_id, session_dir, email, api_key)

    @function_tool
    def extract_gse_metadata(
        gse_id: str,
        email: str = None,
        api_key: str = None
    ) -> str:
        """
        Extract metadata for a GEO Series (GSE) record.
        
        This tool retrieves comprehensive metadata for a specific GEO series,
        including series characteristics, experimental design, and associated
        information.
        
        Parameters
        ----------
        gse_id : str
            Gene Expression Omnibus series ID (e.g., "GSE41588").
        email : str, optional
            Email address for NCBI E-Utils identification.
            If not provided, uses session default.
        api_key : str, optional
            NCBI API key for higher rate limits.
            If not provided, uses session default.
        
        Returns
        -------
        str
            Path to the saved metadata file.
        """
        # Use session defaults if not provided
        if email is None:
            email = default_email
        if api_key is None:
            api_key = default_api_key
        
        return extract_gse_metadata_impl(gse_id, session_dir, email, api_key)

    @function_tool
    def extract_series_matrix_metadata(
        gse_id: str,
        email: str = None,
        api_key: str = None
    ) -> str:
        """
        Extract series matrix metadata and sample names for a GEO Series (GSE) record.
        
        This tool retrieves metadata and sample names from the series matrix file
        without downloading the full gene expression data.
        
        Parameters
        ----------
        gse_id : str
            Gene Expression Omnibus series ID (e.g., "GSE41588").
        email : str, optional
            Email address for NCBI E-Utils identification.
            If not provided, uses session default.
        api_key : str, optional
            NCBI API key for higher rate limits.
            If not provided, uses session default.
        
        Returns
        -------
        str
            Path to the saved metadata file.
        """
        # Use session defaults if not provided
        if email is None:
            email = default_email
        if api_key is None:
            api_key = default_api_key
        
        return extract_series_matrix_metadata_impl(gse_id, session_dir, email, api_key)

    @function_tool
    def extract_paper_abstract(
        pmid: int,
        email: str = None,
        api_key: str = None,
        source_gse_file: str = None
    ) -> str:
        """
        Extract paper abstract and metadata for a given PMID.
        
        This tool retrieves paper title, abstract, authors, journal, and other
        metadata from PubMed.
        
        Parameters
        ----------
        pmid : int
            PubMed ID for the paper.
        email : str, optional
            Email address for NCBI E-Utils identification.
            If not provided, uses session default.
        api_key : str, optional
            NCBI API key for higher rate limits.
            If not provided, uses session default.
        source_gse_file : str, optional
            Path to the GSE metadata file that this PMID was extracted from.
            Used to determine the correct series directory for file organization.
        
        Returns
        -------
        str
            Path to the saved metadata file.
        """
        # Use session defaults if not provided
        if email is None:
            email = default_email
        if api_key is None:
            api_key = default_api_key
        
        return extract_paper_abstract_impl(pmid, session_dir, email, api_key, source_gse_file)

    @function_tool
    def extract_pubmed_id_from_gse_metadata(
        gse_metadata_file: str
    ) -> str:
        """
        Extract PubMed ID from a GSE metadata JSON file.
        
        This tool reads a GSE metadata file (produced by extract_gse_metadata tool)
        and extracts the PubMed ID from the "pubmed_id" field under attributes.
        This is useful for multi-step workflows where you need to extract the PMID
        to then call extract_paper_abstract.
        
        Parameters
        ----------
        gse_metadata_file : str
            Path to the GSE metadata JSON file (e.g., "GSE41588_metadata.json")
        
        Returns
        -------
        str
            JSON string containing the extracted PubMed ID and status information
        """
        return extract_pubmed_id_from_gse_metadata_impl(gse_metadata_file, session_dir)

    @function_tool
    def extract_series_id_from_gsm_metadata(
        gsm_metadata_file: str
    ) -> str:
        """
        Extract Series ID from a GSM metadata JSON file.
        
        This tool reads a GSM metadata file (produced by extract_gsm_metadata tool)
        and extracts the Series ID from the "series_id" field under attributes.
        This is useful for multi-step workflows where you need to extract the GSE ID
        to then call extract_gse_metadata and extract_series_matrix_metadata.
        
        Parameters
        ----------
        gsm_metadata_file : str
            Path to the GSM metadata JSON file (e.g., "GSM1019742_metadata.json")
        
        Returns
        -------
        str
            JSON string containing the extracted Series ID and status information
        """
        return extract_series_id_from_gsm_metadata_impl(gsm_metadata_file, session_dir)

    @function_tool
    def validate_geo_inputs(
        gsm_id: str = None,
        gse_id: str = None,
        pmid: int = None,
        email: str = None,
        api_key: str = None
    ) -> str:
        """
        Validate input parameters for GEO metadata extraction.
        
        This tool checks the format and validity of GSM IDs, GSE IDs, and PMIDs
        before attempting metadata extraction.
        
        Parameters
        ----------
        gsm_id : str, optional
            Gene Expression Omnibus sample ID to validate.
        gse_id : str, optional
            Gene Expression Omnibus series ID to validate.
        pmid : int, optional
            PubMed ID to validate.
        email : str, optional
            Email address for NCBI E-Utils identification.
            If not provided, uses session default.
        api_key : str, optional
            NCBI API key for higher rate limits.
            If not provided, uses session default.
        
        Returns
        -------
        str
            JSON string containing validation results.
        """
        # Use session defaults if not provided
        if email is None:
            email = default_email
        if api_key is None:
            api_key = default_api_key
        
        return validate_geo_inputs_impl(gsm_id, gse_id, pmid, email, api_key)

    @function_tool
    def create_series_sample_mapping() -> str:
        """
        Create a mapping file between series IDs and sample IDs in the main session directory.
        
        This tool scans the session directory for series subdirectories (GSE*) and creates
        a comprehensive mapping file that shows which sample IDs belong to which series.
        The mapping file is saved in the main session directory and can be used by later
        agents to quickly determine which subdirectory contains data for a given sample ID.
        
        The mapping file contains:
        - Forward mapping: series_id -> list of sample_ids
        - Reverse mapping: sample_id -> series_id (for quick lookup)
        - Summary statistics: total series and sample counts
        - Metadata: generation timestamp and session directory path
        
        Returns
        -------
        str
            Path to the created mapping file (series_sample_mapping.json)
        """
        return create_series_sample_mapping_impl(session_dir)

    # LinkerAgent tools
    @function_tool
    def load_mapping_file() -> str:
        """
        Load the series_sample_mapping.json file to understand directory structure.
        
        This tool loads the mapping file created by the IngestionAgent that shows
        the relationship between series IDs and sample IDs, and which subdirectory
        contains the data for each sample.
        
        Returns
        -------
        str
            JSON string containing the mapping data with success status
        """
        result = load_mapping_file_impl(session_dir)
        if not result.get('success', True):
            raise RuntimeError(f"Failed to load mapping file: {result.get('message', 'Unknown error')}")
        return json.dumps(result)

    @function_tool
    def find_sample_directory(sample_id: str) -> str:
        """
        Find the directory containing files for a specific sample ID.
        
        This tool uses the mapping file to locate the correct subdirectory
        that contains the metadata files for the given sample ID.
        
        Parameters
        ----------
        sample_id : str
            The sample ID to find (e.g., GSM1000981)
            
        Returns
        -------
        str
            JSON string containing directory information and success status
        """
        result = find_sample_directory_impl(sample_id, session_dir)
        if not result.get('success', True):
            raise RuntimeError(f"Failed to find sample directory: {result.get('message', 'Unknown error')}")
        return json.dumps(result)

    @function_tool
    def clean_metadata_files(sample_id: str, fields_to_remove: str = None) -> str:
        """
        Generate cleaned versions of metadata files by removing specified fields.
        
        This tool creates cleaned versions of the series metadata, abstract metadata,
        and series matrix metadata files by removing specified fields. The cleaned
        files are saved in a 'cleaned' subdirectory.
        
        Parameters
        ----------
        sample_id : str
            The sample ID to process
        fields_to_remove : str, optional
            JSON string containing list of fields to remove from metadata files.
            If not provided, uses default fields like 'status', 'submission_date', etc.
            
        Returns
        -------
        str
            JSON string containing paths to cleaned files and success status
        """
        fields_list = None
        if fields_to_remove:
            fields_list = json.loads(fields_to_remove)
        
        result = clean_metadata_files_impl(sample_id, session_dir, fields_list)
        if not result.get('success', True):
            raise RuntimeError(f"Failed to clean metadata files: {result.get('message', 'Unknown error')}")
        return json.dumps(result)

    @function_tool
    def download_series_matrix(sample_id: str) -> str:
        """
        Download the smallest series matrix file for a sample.
        
        This tool downloads the smallest available series matrix file for the given
        sample's series, which contains the actual expression data and metadata.
        
        Parameters
        ----------
        sample_id : str
            The sample ID to process
            
        Returns
        -------
        str
            JSON string containing download information and success status
        """
        result = download_series_matrix_impl(sample_id, session_dir)
        if not result.get('success', True):
            raise RuntimeError(f"Failed to download series matrix: {result.get('message', 'Unknown error')}")
        return json.dumps(result)

    @function_tool
    def extract_matrix_metadata(sample_id: str) -> str:
        """
        Extract metadata from the top of a series matrix file (prefixed with !).
        
        This tool extracts the metadata from the header section of a series matrix
        file, which contains important information about the experiment and samples.
        
        Parameters
        ----------
        sample_id : str
            The sample ID to process
            
        Returns
        -------
        str
            JSON string containing extracted metadata and success status
        """
        result = extract_matrix_metadata_impl(sample_id, session_dir)
        if not result.get('success', True):
            raise RuntimeError(f"Failed to extract matrix metadata: {result.get('message', 'Unknown error')}")
        return json.dumps(result)

    @function_tool
    def extract_sample_metadata(sample_id: str) -> str:
        """
        Extract metadata for a specific sample from the series matrix table.
        
        This tool extracts the expression data and metadata for a specific sample
        from the data table section of the series matrix file.
        
        Parameters
        ----------
        sample_id : str
            The sample ID to extract data for
            
        Returns
        -------
        str
            JSON string containing sample-specific metadata and success status
        """
        result = extract_sample_metadata_impl(sample_id, session_dir)
        if not result.get('success', True):
            raise RuntimeError(f"Failed to extract sample metadata: {result.get('message', 'Unknown error')}")
        return json.dumps(result)

    @function_tool
    def package_linked_data(sample_id: str, fields_to_remove: str = None) -> str:
        """
        Package all linked information for a sample into a comprehensive result.
        
        This tool combines all the processed information for a sample including:
        - Cleaned metadata files
        - Downloaded series matrix file
        - Extracted matrix metadata
        - Sample-specific data points
        - Original sample metadata
        
        Parameters
        ----------
        sample_id : str
            The sample ID to process
        fields_to_remove : str, optional
            JSON string containing list of fields to remove from metadata files
            
        Returns
        -------
        str
            JSON string containing all packaged information and success status
        """
        fields_list = None
        if fields_to_remove:
            fields_list = json.loads(fields_to_remove)
        
        result = package_linked_data_impl(sample_id, session_dir, fields_list)
        if not result.get('success', True):
            raise RuntimeError(f"Failed to package linked data: {result.get('message', 'Unknown error')}")
        return json.dumps(result)

    # Return all the tools
    return [
        extract_gsm_metadata,
        extract_gse_metadata,
        extract_series_matrix_metadata,
        extract_paper_abstract,
        extract_pubmed_id_from_gse_metadata,
        extract_series_id_from_gsm_metadata,
        validate_geo_inputs,
        create_series_sample_mapping,
        load_mapping_file,
        find_sample_directory,
        clean_metadata_files,
        download_series_matrix,
        extract_matrix_metadata,
        extract_sample_metadata,
        package_linked_data
    ]


def get_available_tools():
    """
    Get a list of available tool names for reference.
    
    Returns
    -------
    List[str]
        List of available tool function names.
    """
    return [
        "extract_gsm_metadata",
        "extract_gse_metadata", 
        "extract_series_matrix_metadata",
        "extract_paper_abstract",
        "extract_pubmed_id_from_gse_metadata",
        "extract_series_id_from_gsm_metadata",
        "validate_geo_inputs",
        "create_series_sample_mapping",
        "load_mapping_file",
        "find_sample_directory",
        "clean_metadata_files",
        "download_series_matrix",
        "extract_matrix_metadata",
        "extract_sample_metadata",
        "package_linked_data"
    ]
