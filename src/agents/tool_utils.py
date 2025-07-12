"""
Tool utilities for agents to use GEO metadata extraction functionality.

This module exposes the GEO metadata extraction tools as function tools
that can be used by agents in the system.
"""

from __future__ import annotations

import json
import os
from typing import Dict, Any, List, Optional
from functools import partial
from pathlib import Path

from agents import function_tool
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import the core tools
from ..tools.extract_geo_metadata import (
    get_gsm_metadata,
    get_gse_metadata,
    get_gse_series_matrix,
    get_paper_abstract
)


def get_geo_tools(
    session_id: str,
    default_email: str = None,
    default_api_key: str = None
) -> List:
    """
    Creates a suite of GEO metadata extraction tools that are bound to a specific session.

    This approach provides four distinct, independent tools:
    1. GSM metadata extraction - extracts sample-level metadata
    2. GSE metadata extraction - extracts series-level metadata
    3. Series matrix extraction - extracts matrix metadata and sample names
    4. Paper abstract extraction - extracts paper abstracts and metadata

    Parameters
    ----------
    session_id : str
        The unique session identifier.
    default_email : str, optional
        Default email for NCBI E-Utils. If not provided, uses NCBI_EMAIL environment variable.
    default_api_key : str, optional
        Default NCBI API key. If not provided, uses NCBI_API_KEY environment variable.

    Returns
    -------
    List
        A list of session-bound GEO metadata extraction tools.
        
    Raises
    ------
    ValueError
        If required environment variables are missing
    """
    # Get environment variables
    env_email = os.getenv("NCBI_EMAIL")
    env_api_key = os.getenv("NCBI_API_KEY")
    
    # Use provided defaults or environment variables
    if default_email is None:
        default_email = env_email
    if default_api_key is None:
        default_api_key = env_api_key
    
    # Validate that we have required configuration
    if not default_email:
        raise ValueError(
            "NCBI_EMAIL environment variable is required. "
            "Please set it in your .env file or provide it as a parameter."
        )
    
    # Warn if API key is not set (optional but recommended)
    if not default_api_key:
        print("Warning: NCBI_API_KEY environment variable is not set. "
              "This will limit API rate limits to 3 requests per second.")

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
            JSON string containing the GSM metadata with the following structure:
            {
                "gsm_id": "GSM1019742",
                "status": "retrieved",
                "attributes": {
                    "title": "Sample title",
                    "geo_accession": "GSM1019742",
                    "status": "Public on ...",
                    "submission_date": "...",
                    "last_update_date": "...",
                    "type": "SRA",
                    "channel_count": "1",
                    "source_name_ch1": "...",
                    "organism_ch1": "Homo sapiens",
                    "taxid_ch1": "9606",
                    "characteristics_ch1": "...",
                    "treatment_protocol_ch1": "...",
                    "growth_protocol_ch1": "...",
                    "molecule_ch1": "total RNA",
                    "extract_protocol_ch1": "...",
                    "description": "...",
                    "data_processing": "...",
                    "platform_id": "GPL11154",
                    "contact_name": "...",
                    "contact_email": "...",
                    "contact_institute": "...",
                    "instrument_model": "...",
                    "library_selection": "...",
                    "library_source": "...",
                    "library_strategy": "...",
                    "relation": "...",
                    "supplementary_file_1": "...",
                    "series_id": "GSE41588",
                    "data_row_count": "..."
                }
            }
        
        Examples
        --------
        >>> result = extract_gsm_metadata("GSM1019742")
        >>> data = json.loads(result)
        >>> print(data["gsm_id"])  # "GSM1019742"
        >>> print(data["attributes"]["title"])  # Sample title
        
        Note
        ----
        - GSM ID must be in valid format (GSM followed by digits)
        - Requires NCBI_EMAIL environment variable
        - NCBI_API_KEY is optional but recommended for higher rate limits
        - Rate limited to 3 requests per second without API key
        """
        
        # Use session defaults if not provided
        if email is None:
            email = default_email
        if api_key is None:
            api_key = default_api_key
        
        try:
            # Validate GSM ID format
            if not gsm_id.upper().startswith("GSM") or not gsm_id[3:].isdigit():
                raise ValueError(f"Invalid GSM ID format: {gsm_id}")
            
            # Extract metadata
            metadata = get_gsm_metadata(gsm_id)
            
            return json.dumps(metadata, indent=2, ensure_ascii=False)
            
        except Exception as e:
            error_result = {
                "error": str(e),
                "gsm_id": gsm_id,
                "status": "failed"
            }
            return json.dumps(error_result, indent=2, ensure_ascii=False)

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
            JSON string containing the GSE metadata with the following structure:
            {
                "gse_id": "GSE41588",
                "status": "retrieved",
                "attributes": {
                    "title": "Series title",
                    "geo_accession": "GSE41588",
                    "status": "Public on ...",
                    "submission_date": "...",
                    "last_update_date": "...",
                    "pubmed_id": "23902433",
                    "summary": "...",
                    "overall_design": "...",
                    "type": "Expression profiling by high throughput sequencing",
                    "sample_id": "GSM1019743",
                    "contact_name": "...",
                    "contact_email": "...",
                    "contact_institute": "...",
                    "supplementary_file": "...",
                    "platform_id": "GPL11154",
                    "platform_organism": "Homo sapiens",
                    "platform_taxid": "9606",
                    "sample_organism": "Homo sapiens",
                    "sample_taxid": "9606",
                    "relation": "..."
                },
                "type": "GSE"
            }
        
        Examples
        --------
        >>> result = extract_gse_metadata("GSE41588")
        >>> data = json.loads(result)
        >>> print(data["gse_id"])  # "GSE41588"
        >>> print(data["attributes"]["title"])  # Series title
        
        Note
        ----
        - GSE ID must be in valid format (GSE followed by digits)
        - Requires NCBI_EMAIL environment variable
        - NCBI_API_KEY is optional but recommended for higher rate limits
        - Rate limited to 3 requests per second without API key
        """
        
        # Use session defaults if not provided
        if email is None:
            email = default_email
        if api_key is None:
            api_key = default_api_key
        
        try:
            # Validate GSE ID format
            if not gse_id.upper().startswith("GSE") or not gse_id[3:].isdigit():
                raise ValueError(f"Invalid GSE ID format: {gse_id}")
            
            # Extract metadata
            metadata = get_gse_metadata(gse_id)
            
            return json.dumps(metadata, indent=2, ensure_ascii=False)
            
        except Exception as e:
            error_result = {
                "error": str(e),
                "gse_id": gse_id,
                "status": "failed"
            }
            return json.dumps(error_result, indent=2, ensure_ascii=False)

    @function_tool
    def extract_series_matrix_metadata(
        gse_id: str,
        email: str = None,
        api_key: str = None
    ) -> str:
        """
        Extract series matrix metadata and sample names for a GEO Series (GSE) record.
        
        This tool retrieves the series matrix metadata and sample names without
        downloading the actual gene expression data. It provides:
        - All metadata lines (starting with !)
        - Sample names from the matrix header
        - File download links
        - Platform information
        
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
            JSON string containing the series matrix metadata with the following structure:
            {
                "gse_id": "GSE41588",
                "type": "series_matrix_metadata",
                "sample_count": 6,
                "platform_count": 1,
                "metadata": {
                    "platform_id": {
                        "!Series_title": "...",
                        "!Series_summary": "...",
                        "!Series_overall_design": "...",
                        "!Sample_geo_accession": "GSM1019742",
                        "!Sample_title": "...",
                        "!Sample_characteristics_ch1": "...",
                        "!Platform_geo_accession": "GPL11154",
                        "!Platform_title": "...",
                        "!Platform_organism": "Homo sapiens",
                        "!Platform_taxid": "9606"
                    }
                },
                "samples": ["GSM1019742", "GSM1019743", ...],
                "platforms": ["GPL11154"],
                "available_files": ["GSE41588_series_matrix.txt.gz"],
                "file_links": ["https://ftp.ncbi.nlm.nih.gov/geo/series/..."],
                "base_url": "https://ftp.ncbi.nlm.nih.gov/geo/series/...",
                "total_matrices": 1
            }
        
        Examples
        --------
        >>> result = extract_series_matrix_metadata("GSE41588")
        >>> data = json.loads(result)
        >>> print(data["gse_id"])  # "GSE41588"
        >>> print(data["sample_count"])  # 6
        >>> print(data["samples"])  # ["GSM1019742", "GSM1019743", ...]
        
        Note
        ----
        - GSE ID must be in valid format (GSE followed by digits)
        - Only extracts metadata and sample names, not gene expression data
        - Provides direct download links to full matrix files
        - Requires NCBI_EMAIL environment variable
        - NCBI_API_KEY is optional but recommended for higher rate limits
        - Rate limited to 3 requests per second without API key
        """
        
        # Use session defaults if not provided
        if email is None:
            email = default_email
        if api_key is None:
            api_key = default_api_key
        
        try:
            # Validate GSE ID format
            if not gse_id.upper().startswith("GSE") or not gse_id[3:].isdigit():
                raise ValueError(f"Invalid GSE ID format: {gse_id}")
            
            # Extract series matrix metadata
            matrix_data = get_gse_series_matrix(gse_id)
            
            return json.dumps(matrix_data, indent=2, ensure_ascii=False)
            
        except Exception as e:
            error_result = {
                "error": str(e),
                "gse_id": gse_id,
                "status": "failed"
            }
            return json.dumps(error_result, indent=2, ensure_ascii=False)

    @function_tool
    def extract_paper_abstract(
        pmid: int,
        email: str = None,
        api_key: str = None
    ) -> str:
        """
        Extract paper abstract and metadata for a given PubMed ID (PMID).
        
        This tool retrieves comprehensive paper information including abstract,
        title, authors, journal, publication date, DOI, and other metadata.
        
        Parameters
        ----------
        pmid : int
            PubMed ID for the paper (e.g., 23902433).
        email : str, optional
            Email address for NCBI E-Utils identification.
            If not provided, uses session default.
        api_key : str, optional
            NCBI API key for higher rate limits.
            If not provided, uses session default.
        
        Returns
        -------
        str
            JSON string containing the paper metadata with the following structure:
            {
                "pmid": 23902433,
                "title": "Paper title",
                "abstract": "Paper abstract text...",
                "authors": ["Author 1", "Author 2", ...],
                "journal": "Journal name",
                "publication_date": "2013",
                "doi": "doi: 10.1186/...",
                "keywords": ["keyword1", "keyword2", ...],
                "mesh_terms": ["term1", "term2", ...]
            }
        
        Examples
        --------
        >>> result = extract_paper_abstract(23902433)
        >>> data = json.loads(result)
        >>> print(data["pmid"])  # 23902433
        >>> print(data["title"])  # Paper title
        >>> print(data["abstract"][:100])  # First 100 chars of abstract
        
        Note
        ----
        - PMID must be a valid integer
        - Requires NCBI_EMAIL environment variable
        - NCBI_API_KEY is optional but recommended for higher rate limits
        - Rate limited to 3 requests per second without API key
        - Uses both esummary and efetch APIs for comprehensive data
        """
        
        # Use session defaults if not provided
        if email is None:
            email = default_email
        if api_key is None:
            api_key = default_api_key
        
        try:
            # Validate PMID format
            if not isinstance(pmid, int) or pmid <= 0:
                raise ValueError(f"Invalid PMID format: {pmid}. Must be a positive integer.")
            
            # Extract paper abstract and metadata
            paper_data = get_paper_abstract(pmid)
            
            return json.dumps(paper_data, indent=2, ensure_ascii=False)
            
        except Exception as e:
            error_result = {
                "error": str(e),
                "pmid": pmid,
                "status": "failed"
            }
            return json.dumps(error_result, indent=2, ensure_ascii=False)

    @function_tool
    def validate_geo_inputs(
        gsm_id: str = None,
        gse_id: str = None,
        pmid: int = None,
        email: str = None,
        api_key: str = None
    ) -> str:
        """
        Validate GEO input parameters before metadata extraction.
        
        This tool performs comprehensive validation of input parameters to ensure
        they meet the requirements for GEO metadata extraction. It validates:
        - GSM ID format (must start with "GSM" followed by digits)
        - GSE ID format (must start with "GSE" followed by digits)
        - PMID format (must be a positive integer)
        - Email format (if provided)
        - API key format (if provided)
        
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
            JSON string containing validation results with the following structure:
            {
                "valid": true/false,
                "errors": ["error message 1", "error message 2", ...],
                "warnings": ["warning message 1", "warning message 2", ...],
                "validated_inputs": {
                    "gsm_id": "GSM1234567",
                    "gse_id": "GSE12345",
                    "pmid": 23902433,
                    "email": "user@example.com",
                    "api_key": "key_present" or null
                }
            }
        
        Examples
        --------
        >>> result = validate_geo_inputs("GSM1234567", "GSE12345", 23902433)
        >>> data = json.loads(result)
        >>> print(data["valid"])  # True
        
        >>> result = validate_geo_inputs("INVALID123", "GSE12345")
        >>> data = json.loads(result)
        >>> print(data["valid"])  # False
        >>> print(data["errors"])  # ["Invalid GSM ID format: INVALID123..."]
        
        Note
        ----
        - This tool does not make any API calls
        - Validation is purely format-based
        - Existence of IDs in GEO/PubMed databases is not verified
        - Email and API key validation is basic format checking
        """
        
        # Use session defaults if not provided
        if email is None:
            email = default_email
        if api_key is None:
            api_key = default_api_key
        
        errors = []
        warnings = []
        
        # Validate GSM ID format
        if gsm_id:
            if not gsm_id.upper().startswith("GSM"):
                errors.append(f"Invalid GSM ID format: {gsm_id}. Must start with 'GSM'")
            elif not gsm_id[3:].isdigit():
                errors.append(f"Invalid GSM ID format: {gsm_id}. Must be GSM followed by digits")
            elif len(gsm_id) < 7:
                warnings.append(f"GSM ID {gsm_id} seems unusually short")
        
        # Validate GSE ID format
        if gse_id:
            if not gse_id.upper().startswith("GSE"):
                errors.append(f"Invalid GSE ID format: {gse_id}. Must start with 'GSE'")
            elif not gse_id[3:].isdigit():
                errors.append(f"Invalid GSE ID format: {gse_id}. Must be GSE followed by digits")
            elif len(gse_id) < 6:
                warnings.append(f"GSE ID {gse_id} seems unusually short")
        
        # Validate PMID format
        if pmid is not None:
            if not isinstance(pmid, int) or pmid <= 0:
                errors.append(f"Invalid PMID format: {pmid}. Must be a positive integer.")
        
        # Validate email format (basic check)
        if email and "@" not in email:
            errors.append(f"Invalid email format: {email}")
        elif email and email == "user@example.com":
            warnings.append("Using default email address - consider providing a real email for NCBI E-Utils")
        
        # Validate API key format (basic check)
        if api_key and len(api_key) < 10:
            warnings.append("API key seems unusually short")
        
        # Prepare validated inputs
        validated_inputs = {
            "gsm_id": gsm_id.upper() if gsm_id else None,
            "gse_id": gse_id.upper() if gse_id else None,
            "pmid": pmid,
            "email": email,
            "api_key": "key_present" if api_key else None
        }
        
        result = {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "validated_inputs": validated_inputs
        }
        
        return json.dumps(result, indent=2, ensure_ascii=False)

    # Return all the tools
    return [
        extract_gsm_metadata,
        extract_gse_metadata,
        extract_series_matrix_metadata,
        extract_paper_abstract,
        validate_geo_inputs
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
        "validate_geo_inputs"
    ]
