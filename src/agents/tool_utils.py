"""
Tool utilities for agents to use GEO metadata extraction functionality.

This module exposes the GEO metadata extraction tools as function tools
that can be used by agents in the system.
"""

from __future__ import annotations

from functools import partial
from pathlib import Path
import json
import os
from typing import Dict, Any, List, Optional

from agents import function_tool
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import the core GEO tools
from src.tools.geo_metadata import (
    get_gsm_metadata,
    get_gse_metadata,
    get_gse_series_matrix,
    get_paper_abstract
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
            JSON string containing the GSM metadata.
        """
        # Use session defaults if not provided
        if email is None:
            email = default_email
        if api_key is None:
            api_key = default_api_key
        
        try:
            print(f"🔧 extract_gsm_metadata called with gsm_id: {gsm_id}")
            
            # Validate GSM ID format
            if not gsm_id.upper().startswith("GSM") or not gsm_id[3:].isdigit():
                raise ValueError(f"Invalid GSM ID format: {gsm_id}")
            
            # Extract metadata
            print(f"🔧 Calling get_gsm_metadata for {gsm_id}")
            metadata = get_gsm_metadata(gsm_id)
            print(f"🔧 get_gsm_metadata returned: {type(metadata)}")
            
            # Save to session directory
            output_file = Path(session_dir) / f"{gsm_id}_metadata.json"
            with open(output_file, 'w') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            # Return a concise summary instead of full JSON
            summary = {
                "status": "success",
                "gsm_id": gsm_id,
                "file_saved": str(output_file),
                "sample_count": 1,
                "key_attributes": list(metadata.get("attributes", {}).keys())[:10] if metadata.get("attributes") else [],
                "message": f"Metadata extracted and saved to {output_file}"
            }
            return json.dumps(summary, indent=2, ensure_ascii=False)
            
        except Exception as e:
            print(f"❌ Exception in extract_gsm_metadata: {e}")
            import traceback
            traceback.print_exc()
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
            JSON string containing the GSE metadata.
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
            print(f"🔧 Calling get_gse_metadata for {gse_id}")
            metadata = get_gse_metadata(gse_id)
            print(f"🔧 get_gse_metadata returned: {type(metadata)}")
            
            # Save to session directory
            output_file = Path(session_dir) / f"{gse_id}_metadata.json"
            with open(output_file, 'w') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            # Return a concise summary instead of full JSON
            summary = {
                "status": "success",
                "gse_id": gse_id,
                "file_saved": str(output_file),
                "key_attributes": list(metadata.get("attributes", {}).keys())[:10] if metadata.get("attributes") else [],
                "message": f"Metadata extracted and saved to {output_file}"
            }
            return json.dumps(summary, indent=2, ensure_ascii=False)
            
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
            JSON string containing the series matrix metadata and sample names.
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
            metadata = get_gse_series_matrix(gse_id)
            
            # Save to session directory
            output_file = Path(session_dir) / f"{gse_id}_series_matrix.json"
            with open(output_file, 'w') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            # Return a concise summary instead of full JSON
            summary = {
                "status": "success",
                "gse_id": gse_id,
                "file_saved": str(output_file),
                "sample_count": metadata.get("sample_count", 0),
                "platform_count": metadata.get("platform_count", 0),
                "total_matrices": metadata.get("total_matrices", 0),
                "message": f"Series matrix metadata extracted and saved to {output_file}"
            }
            return json.dumps(summary, indent=2, ensure_ascii=False)
            
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
        
        Returns
        -------
        str
            JSON string containing the paper metadata and abstract.
        """
        # Use session defaults if not provided
        if email is None:
            email = default_email
        if api_key is None:
            api_key = default_api_key
        
        try:
            # Validate PMID format
            if not isinstance(pmid, int) or pmid <= 0:
                raise ValueError(f"Invalid PMID format: {pmid}")
            
            # Extract paper metadata
            metadata = get_paper_abstract(pmid)
            
            # Save to session directory
            output_file = Path(session_dir) / f"PMID_{pmid}_metadata.json"
            with open(output_file, 'w') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            # Return a concise summary instead of full JSON
            summary = {
                "status": "success",
                "pmid": pmid,
                "file_saved": str(output_file),
                "title": metadata.get("title", ""),
                "journal": metadata.get("journal", ""),
                "authors": metadata.get("authors", [])[:3],  # First 3 authors
                "has_abstract": bool(metadata.get("abstract")),
                "message": f"Paper metadata extracted and saved to {output_file}"
            }
            return json.dumps(summary, indent=2, ensure_ascii=False)
            
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
        
        result = {
            "validation_status": "success",
            "validated_inputs": {},
            "errors": []
        }
        
        # Validate GSM ID
        if gsm_id is not None:
            if gsm_id.upper().startswith("GSM") and gsm_id[3:].isdigit():
                result["validated_inputs"]["gsm_id"] = gsm_id
            else:
                result["errors"].append(f"Invalid GSM ID format: {gsm_id}")
        
        # Validate GSE ID
        if gse_id is not None:
            if gse_id.upper().startswith("GSE") and gse_id[3:].isdigit():
                result["validated_inputs"]["gse_id"] = gse_id
            else:
                result["errors"].append(f"Invalid GSE ID format: {gse_id}")
        
        # Validate PMID
        if pmid is not None:
            if isinstance(pmid, int) and pmid > 0:
                result["validated_inputs"]["pmid"] = pmid
            else:
                result["errors"].append(f"Invalid PMID format: {pmid}")
        
        # Check environment variables
        if not email:
            result["errors"].append("NCBI_EMAIL environment variable is required")
        
        if result["errors"]:
            result["validation_status"] = "failed"
        
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
