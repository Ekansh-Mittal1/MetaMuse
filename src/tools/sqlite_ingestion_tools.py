"""
SQLite-based ingestion tools for GEO metadata extraction.

This module provides the same functionality as the original ingestion_tools.py
but uses the local GEOmetadb SQLite database instead of ENTREZ API calls.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import traceback

from .sqlite_manager import GEOmetadbManager, get_geometadb_manager, download_geometadb


def extract_gsm_metadata_sqlite_impl(
    gsm_id: str, 
    session_dir: str, 
    db_path: str = "data/GEOmetadb.sqlite"
) -> str:
    """
    Extract metadata for a GEO Sample (GSM) record using local SQLite database.
    
    This function replaces the ENTREZ API call with a local database query,
    providing the same metadata structure but much faster performance.
    
    Parameters
    ----------
    gsm_id : str
        Gene Expression Omnibus sample ID (e.g., "GSM1019742").
    session_dir : str
        Directory to save the extracted metadata.
    db_path : str
        Path to the GEOmetadb SQLite database.
        
    Returns
    -------
    str
        Path to the saved metadata file.
    """
    try:
        session_path = Path(session_dir)
        session_path.mkdir(parents=True, exist_ok=True)
        
        # Get metadata from local database
        with get_geometadb_manager(db_path) as manager:
            metadata = manager.get_gsm_metadata(gsm_id)
        
        if "error" in metadata:
            error_msg = f"Failed to extract GSM metadata from SQLite: {metadata['error']}"
            print(f"⚠️  {error_msg}")
            print(f"🔄 Attempting HTTP API fallback for {gsm_id}...")
            
            try:
                # Import and use HTTP-based implementation
                from src.tools.ingestion_tools import extract_gsm_metadata_impl
                import os
                
                # Get email and API key from environment
                email = os.getenv("NCBI_EMAIL")
                api_key = os.getenv("NCBI_API_KEY")
                
                if not email:
                    print(f"❌ NCBI_EMAIL environment variable not set, cannot use HTTP API fallback")
                    return json.dumps({
                        "success": False,
                        "message": f"{error_msg}. HTTP API fallback requires NCBI_EMAIL environment variable.",
                        "gsm_id": gsm_id
                    })
                
                # Use the HTTP API implementation
                result_path = extract_gsm_metadata_impl(
                    gsm_id=gsm_id,
                    session_dir=session_dir,
                    email=email,
                    api_key=api_key
                )
                
                print(f"✅ Successfully extracted GSM metadata via HTTP API fallback: {result_path}")
                return result_path
                
            except ImportError as e:
                print(f"❌ HTTP API fallback not available: {e}")
                return json.dumps({
                    "success": False,
                    "message": f"{error_msg}. HTTP API fallback not available: {str(e)}",
                    "gsm_id": gsm_id
                })
            except Exception as e:
                print(f"❌ HTTP API fallback also failed: {e}")
                return json.dumps({
                    "success": False,
                    "message": f"{error_msg}. HTTP API fallback also failed: {str(e)}",
                    "gsm_id": gsm_id
                })
        
        # Restructure metadata to match original workflow structure
        # The original workflow uses an 'attributes' wrapper
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
        
        # Add required fields that the Pydantic models expect
        # Don't add geo_accession to match original workflow structure
        
        # Add series information to attributes
        if "series" in metadata:
            restructured_metadata["attributes"]["series_id"] = metadata["series"][0] if metadata["series"] else None
            restructured_metadata["attributes"]["all_series_ids"] = ", ".join(metadata["series"]) if metadata["series"] else None
        
        # Save metadata to file
        output_file = session_path / f"{gsm_id}_metadata.json"
        with open(output_file, 'w') as f:
            json.dump(restructured_metadata, f, indent=2, default=str)
        
        return str(output_file)
        
    except Exception as e:
        error_msg = f"Error extracting GSM metadata for {gsm_id}: {str(e)}"
        print(f"❌ {error_msg}")
        print("🔍 Full traceback:")
        traceback.print_exc()
        raise ValueError(error_msg)


def extract_gse_metadata_sqlite_impl(
    gse_id: str, 
    session_dir: str, 
    db_path: str = "data/GEOmetadb.sqlite"
) -> str:
    """
    Extract metadata for a GEO Series (GSE) record using local SQLite database.
    
    This function replaces the ENTREZ API call with a local database query,
    providing the same metadata structure but much faster performance.
    
    Parameters
    ----------
    gse_id : str
        Gene Expression Omnibus series ID (e.g., "GSE41588").
    session_dir : str
        Directory to save the extracted metadata.
    db_path : str
        Path to the GEOmetadb SQLite database.
        
    Returns
    -------
    str
        Path to the saved metadata file.
    """
    try:
        session_path = Path(session_dir)
        session_path.mkdir(parents=True, exist_ok=True)
        
        # Get metadata from local database
        with get_geometadb_manager(db_path) as manager:
            metadata = manager.get_gse_metadata(gse_id)
        
        if "error" in metadata:
            error_msg = f"Failed to extract GSE metadata from SQLite: {metadata['error']}"
            print(f"⚠️  {error_msg}")
            print(f"🔄 Attempting HTTP API fallback for {gse_id}...")
            
            try:
                # Import and use HTTP-based implementation
                from src.tools.ingestion_tools import extract_gse_metadata_impl
                import os
                
                # Get email and API key from environment
                email = os.getenv("NCBI_EMAIL")
                api_key = os.getenv("NCBI_API_KEY")
                
                if not email:
                    print(f"❌ NCBI_EMAIL environment variable not set, cannot use HTTP API fallback")
                    return json.dumps({
                        "success": False,
                        "message": f"{error_msg}. HTTP API fallback requires NCBI_EMAIL environment variable.",
                        "gse_id": gse_id
                    })
                
                # Use the HTTP API implementation
                result_path = extract_gse_metadata_impl(
                    gse_id=gse_id,
                    session_dir=session_dir,
                    email=email,
                    api_key=api_key
                )
                
                print(f"✅ Successfully extracted GSE metadata via HTTP API fallback: {result_path}")
                return result_path
                
            except ImportError as e:
                print(f"❌ HTTP API fallback not available: {e}")
                return json.dumps({
                    "success": False,
                    "message": f"{error_msg}. HTTP API fallback not available: {str(e)}",
                    "gse_id": gse_id
                })
            except Exception as e:
                print(f"❌ HTTP API fallback also failed: {e}")
                return json.dumps({
                    "success": False,
                    "message": f"{error_msg}. HTTP API fallback also failed: {str(e)}",
                    "gse_id": gse_id
                })
        
        # Restructure metadata to match original workflow structure
        # The original workflow uses an 'attributes' wrapper
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
        
        # Add required fields that the Pydantic models expect
        # Don't add geo_accession to match original workflow structure
        
        # Add samples and platforms to attributes
        if "samples" in metadata:
            restructured_metadata["attributes"]["sample_id"] = ", ".join(metadata["samples"]) if metadata["samples"] else None
        if "platforms" in metadata:
            restructured_metadata["attributes"]["platform_id"] = ", ".join(metadata["platforms"]) if metadata["platforms"] else None
        
        # Save metadata to file
        output_file = session_path / f"{gse_id}_metadata.json"
        with open(output_file, 'w') as f:
            json.dump(restructured_metadata, f, indent=2, default=str)
        
        return str(output_file)
        
    except Exception as e:
        error_msg = f"Error extracting GSE metadata for {gse_id}: {str(e)}"
        print(f"❌ {error_msg}")
        print("🔍 Full traceback:")
        traceback.print_exc()
        raise ValueError(error_msg)


def extract_paper_abstract_sqlite_impl(
    pmid: str,
    session_dir: str,
    db_path: str = "data/GEOmetadb.sqlite"
) -> str:
    """
    Extract abstract and metadata for a PubMed paper.
    
    This function first tries to use a local PubMed SQLite database for fast lookups.
    If the local database is not available, it falls back to the HTTP API.
    
    Parameters
    ----------
    pmid : str
        PubMed ID (e.g., "23902433").
    session_dir : str
        Directory to save the extracted metadata.
    db_path : str
        Path to the GEOmetadb SQLite database (unused for PubMed data).
        
    Returns
    -------
    str
        Path to the saved abstract metadata file.
    """
    try:
        session_path = Path(session_dir)
        session_path.mkdir(parents=True, exist_ok=True)
        
        # Try local PubMed SQLite database first
        try:
            from src.tools.pubmed_sqlite_manager import PubMedSQLiteManager
            
            pubmed_manager = PubMedSQLiteManager()
            if pubmed_manager.is_available():
                print(f"📋 Using local PubMed database for PMID {pmid}")
                
                metadata = pubmed_manager.get_pubmed_metadata(pmid)
                if "error" not in metadata:
                    # Create metadata in format compatible with original workflow
                    restructured_metadata = {
                        "pmid": int(pmid),
                        "title": metadata.get("title", ""),
                        "abstract": metadata.get("abstract", ""),
                        "authors": metadata.get("authors", []),
                        "journal": metadata.get("journal", ""),
                        "publication_date": metadata.get("publication_date", ""),
                        "keywords": metadata.get("keywords", []),
                        "mesh_terms": metadata.get("mesh_terms", []),
                        "doi": metadata.get("doi", ""),
                        "series_id": ""  # Will be set by caller if needed
                    }
                    
                    # Save metadata to file
                    output_file = session_path / f"PMID_{pmid}_metadata.json"
                    with open(output_file, 'w') as f:
                        json.dump(restructured_metadata, f, indent=2, default=str)
                    
                    return str(output_file)
                else:
                    print(f"⚠️  PMID {pmid} not found in local database, falling back to HTTP API")
            else:
                print(f"⚠️  Local PubMed database not available, falling back to HTTP API for PMID {pmid}")
                
        except ImportError:
            print(f"⚠️  PubMed SQLite manager not available, falling back to HTTP API for PMID {pmid}")
        except Exception as e:
            print(f"⚠️  Error accessing local PubMed database: {e}, falling back to HTTP API for PMID {pmid}")
        
        # Fallback to HTTP API
        from src.tools.ingestion_tools import extract_paper_abstract_impl
        import os
        
        # Convert pmid to int as expected by the HTTP API function
        pmid_int = int(pmid)
        
        # Get email and API key from environment
        email = os.getenv("NCBI_EMAIL")
        api_key = os.getenv("NCBI_API_KEY")
        
        if not email:
            raise ValueError("NCBI_EMAIL environment variable is required for PubMed API access")
                    
        # Use the original HTTP-based implementation
        result_path = extract_paper_abstract_impl(
            pmid=pmid_int,
            session_dir=session_dir,
            email=email,
            api_key=api_key
        )
        
        return result_path
        
    except Exception as e:
        error_msg = f"Error extracting PubMed metadata for PMID {pmid}: {str(e)}"
        print(f"❌ {error_msg}")
        print("🔍 Full traceback:")
        traceback.print_exc()
        raise ValueError(error_msg)


def extract_pubmed_id_from_geo_website(gse_id: str) -> str:
    """
    Extract PubMed ID directly from the GEO website.
    
    This function makes a direct HTTP request to the GEO website to extract
    the PubMed ID for a given GSE ID.
    
    Parameters
    ----------
    gse_id : str
        The GSE ID to extract PubMed ID for
        
    Returns
    -------
    str
        PubMed ID if found, empty string otherwise
    """
    try:
        import requests
        import re
        from urllib.parse import urljoin
        
        # GEO website URL for the specific GSE
        geo_url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={gse_id}"
        
        print(f"🌐 Requesting: {geo_url}")
        
        # Make HTTP request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(geo_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Parse the HTML content
        html_content = response.text
        
        # Look for PubMed ID patterns in the HTML
        # Pattern 1: Look for "PubMed ID:" followed by numbers
        pubmed_patterns = [
            r'PubMed ID:\s*(\d+)',
            r'PubMed ID</a>:\s*(\d+)',
            r'pubmed_id">(\d+)</a>',
            r'pubmed_id=(\d+)',
            r'PMID:\s*(\d+)',
            r'PMID</a>:\s*(\d+)',
            r'<span class="pubmed_id" id="(\d+)">',
            r'class="pubmed_id" id="(\d+)"'
        ]
        
        for pattern in pubmed_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                pubmed_id = match.group(1)
                print(f"✅ Found PubMed ID: {pubmed_id}")
                return pubmed_id
        
        # Pattern 2: Look for links to PubMed
        pubmed_link_patterns = [
            r'href="https://pubmed\.gov/(\d+)"',
            r'href="https://pubmed\.gov/\?term=(\d+)"',
            r'href="https://www\.ncbi\.nlm\.nih\.gov/pubmed/(\d+)"'
        ]
        
        for pattern in pubmed_link_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                pubmed_id = match.group(1)
                print(f"✅ Found PubMed ID from link: {pubmed_id}")
                return pubmed_id
        
        print(f"❌ No PubMed ID found in GEO website HTML for {gse_id}")
        return ""
        
    except ImportError:
        print(f"❌ requests module not available for direct GEO website requests")
        return ""
    except requests.RequestException as e:
        print(f"❌ HTTP request failed for {gse_id}: {e}")
        return ""
    except Exception as e:
        print(f"❌ Error extracting PubMed ID from GEO website for {gse_id}: {e}")
        return ""


def extract_pubmed_id_from_gse_metadata_sqlite_impl(gse_metadata_file: str) -> str:
    """
    Extract PubMed ID from a GSE metadata file with HTTP API fallback.
    
    This function first tries to extract PubMed ID from the local metadata file.
    If no PubMed ID is found, it falls back to making an HTTP request to
    extract the PubMed ID directly from the GEO series.
    
    Parameters
    ----------
    gse_metadata_file : str
        Path to the GSE metadata JSON file.
        
    Returns
    -------
    str
        JSON string with the extracted PubMed ID or error information.
    """
    import os  # Import os at the top of the function
    
    try:
        with open(gse_metadata_file, 'r') as f:
            metadata = json.load(f)
        
        # The new structure has pubmed_id in attributes
        pubmed_id = metadata.get('attributes', {}).get('pubmed_id', '')
        if not pubmed_id:
            # Fallback to old structure
            pubmed_id = metadata.get('pubmed_id', '')
        
        # If we found a PubMed ID in the local metadata, return it
        if pubmed_id:
            result = {
                "success": True,
                "pubmed_id": str(pubmed_id),
                "message": f"PubMed ID extracted from local metadata: {pubmed_id}"
            }
            return json.dumps(result, indent=2)
        
        # No PubMed ID found locally, try HTTP API fallback
        gse_id = metadata.get('gse_id', '')
        if not gse_id:
            # Try to extract GSE ID from the file path if not in metadata
            file_path = os.path.basename(gse_metadata_file)
            if file_path.endswith('_metadata.json'):
                gse_id = file_path.replace('_metadata.json', '')
        
        if gse_id:
            print(f"⚠️  No PubMed ID found in local metadata for {gse_id}, trying HTTP API fallback...")
            
            try:
                # Try direct HTTP request to GEO website first
                print(f"🌐 Making direct HTTP request to GEO website for {gse_id}...")
                direct_pubmed_id = extract_pubmed_id_from_geo_website(gse_id)
                
                if direct_pubmed_id:
                    result = {
                        "success": True,
                        "pubmed_id": direct_pubmed_id,
                        "message": f"PubMed ID extracted via direct GEO website request: {direct_pubmed_id}",
                        "source": "geo_website_direct"
                    }
                    print(f"✅ Successfully extracted PubMed ID {direct_pubmed_id} via direct GEO website request for {gse_id}")
                else:
                    # Fallback to HTTP API implementation
                    print(f"⚠️  Direct GEO website request failed, trying HTTP API fallback...")
                    from src.tools.ingestion_tools import extract_pubmed_id_from_gse_metadata_impl
                    
                    # Call the HTTP API implementation
                    # Extract session_dir from the metadata file path
                    session_dir = os.path.dirname(os.path.dirname(gse_metadata_file))  # Go up two levels to get session dir
                    http_result = extract_pubmed_id_from_gse_metadata_impl(gse_metadata_file, session_dir)
                    http_data = json.loads(http_result)
                    
                    if http_data.get("success", False) and http_data.get("pubmed_id"):
                        result = {
                            "success": True,
                            "pubmed_id": http_data["pubmed_id"],
                            "message": f"PubMed ID extracted via HTTP API fallback: {http_data['pubmed_id']}",
                            "source": "http_api_fallback"
                        }
                        print(f"✅ Successfully extracted PubMed ID {http_data['pubmed_id']} via HTTP API for {gse_id}")
                    else:
                        result = {
                            "success": False,
                            "pubmed_id": None,
                            "message": f"Both direct GEO website request and HTTP API fallback failed. Direct: No PubMed ID found, HTTP API: {http_data.get('message', 'Unknown error')}",
                            "source": "both_failed"
                        }
                        print(f"❌ Both fallback methods failed for {gse_id}")
                
            except ImportError as e:
                result = {
                    "success": False,
                    "pubmed_id": None,
                    "message": f"HTTP API fallback not available: {str(e)}",
                    "source": "import_error"
                }
                print(f"❌ HTTP API fallback not available: {str(e)}")
            except Exception as e:
                result = {
                    "success": False,
                    "pubmed_id": None,
                    "message": f"Fallback error: {str(e)}",
                    "source": "fallback_error"
                }
                print(f"❌ Fallback error for {gse_id}: {str(e)}")
        else:
            result = {
                "success": False,
                "pubmed_id": None,
                "message": "Could not determine GSE ID for HTTP API fallback",
                "source": "gse_id_not_found"
            }
            print(f"❌ Could not determine GSE ID for HTTP API fallback")
            
        return json.dumps(result, indent=2)
            
    except Exception as e:
        error_msg = f"Error extracting PubMed ID from {gse_metadata_file}: {str(e)}"
        print(f"❌ {error_msg}")
        print("🔍 Full traceback:")
        traceback.print_exc()
        
        # Try HTTP API fallback even on file reading errors
        try:
            print(f"🔄 Attempting HTTP API fallback due to file reading error...")
            from src.tools.ingestion_tools import extract_pubmed_id_from_gse_metadata_impl
            # Extract session_dir from the metadata file path
            session_dir = os.path.dirname(os.path.dirname(gse_metadata_file))  # Go up two levels to get session dir
            return extract_pubmed_id_from_gse_metadata_impl(gse_metadata_file, session_dir)
        except Exception as fallback_error:
            result = {
                "success": False,
                "pubmed_id": None,
                "message": f"Both local extraction and HTTP API fallback failed. Local error: {str(e)}, Fallback error: {str(fallback_error)}",
                "source": "both_failed"
            }
            return json.dumps(result, indent=2)


def extract_series_id_from_gsm_metadata_sqlite_impl(gsm_metadata_file: str) -> str:
    """
    Extract series ID from a GSM metadata file.
    
    This function parses a GSM metadata JSON file and extracts the associated
    series ID.
    
    Parameters
    ----------
    gsm_metadata_file : str
        Path to the GSM metadata JSON file.
        
    Returns
    -------
    str
        JSON string with the extracted series ID or error information.
    """
    try:
        with open(gsm_metadata_file, 'r') as f:
            metadata = json.load(f)
        
        # The new structure has series_id in attributes
        series_id = metadata.get('attributes', {}).get('series_id', '')
        if not series_id:
            # Fallback to old structure
            series_ids = metadata.get('series', [])
            if series_ids:
                series_id = series_ids[0] if len(series_ids) > 0 else ''
        
        if series_id:
            result = {
                "success": True,
                "series_id": series_id,
                "message": f"Series ID extracted: {series_id}"
            }
        else:
            result = {
                "success": False,
                "series_id": None,
                "message": "No series ID found in GSM metadata"
            }
            
        return json.dumps(result, indent=2)
            
    except Exception as e:
        error_msg = f"Error extracting series ID from {gsm_metadata_file}: {str(e)}"
        print(f"❌ {error_msg}")
        print("🔍 Full traceback:")
        traceback.print_exc()
        result = {
            "success": False,
            "series_id": None,
            "message": error_msg
        }
        return json.dumps(result, indent=2)


def validate_geo_inputs_sqlite_impl(
    gsm_id: str = None,
    gse_id: str = None,
    pmid: str = None,
    target_field: str = None,
) -> str:
    """
    Validate GEO and PubMed inputs for proper format.
    
    This function validates that provided IDs follow the correct format
    and patterns expected by the system.
    
    Parameters
    ----------
    gsm_id : str, optional
        GEO Sample ID to validate.
    gse_id : str, optional
        GEO Series ID to validate.
    pmid : str, optional
        PubMed ID to validate.
    target_field : str, optional
        Target metadata field to validate.
        
    Returns
    -------
    str
        JSON string with validation results.
    """
    validation_results = {
        "valid": True,
        "errors": [],
        "warnings": []
    }
    
    try:
        # Validate GSM ID format
        if gsm_id:
            if not gsm_id.upper().startswith("GSM") or not gsm_id[3:].isdigit():
                validation_results["valid"] = False
                validation_results["errors"].append(f"Invalid GSM ID format: {gsm_id}")
        
        # Validate GSE ID format
        if gse_id:
            if not gse_id.upper().startswith("GSE") or not gse_id[3:].isdigit():
                validation_results["valid"] = False
                validation_results["errors"].append(f"Invalid GSE ID format: {gse_id}")
        
        # Validate PMID format
        if pmid:
            if not pmid.isdigit():
                validation_results["valid"] = False
                validation_results["errors"].append(f"Invalid PMID format: {pmid}")
        
        # Validate target field
        if target_field:
            if not target_field.strip():
                validation_results["warnings"].append("Target field is empty")
        
        return json.dumps(validation_results, indent=2)
        
    except Exception as e:
        error_msg = f"Error during validation: {str(e)}"
        print(f"❌ {error_msg}")
        print("🔍 Full traceback:")
        traceback.print_exc()
        return json.dumps({
            "valid": False,
            "errors": [error_msg],
            "warnings": []
        }, indent=2)


def create_series_sample_mapping_sqlite_impl(
    session_dir: str,
    db_path: str = "data/GEOmetadb.sqlite"
) -> str:
    """
    Create a mapping between series and samples using the local database.
    
    This function scans the session directory for GEO metadata files and
    creates a mapping structure showing the relationship between
    series (GSE) and samples (GSM).
    
    Parameters
    ----------
    session_dir : str
        Path to the session directory.
    db_path : str
        Path to the GEOmetadb SQLite database.
        
    Returns
    -------
    str
        JSON string with the series-sample mapping.
    """
    try:
        session_path = Path(session_dir)
        
        # Get all metadata files in the session directory and subdirectories
        metadata_files = list(session_path.rglob("*_metadata.json"))
        
        if not metadata_files:
            return json.dumps({
                "mapping": {},
                "total_series": 0,
                "total_samples": 0,
                "message": "No metadata files found in session directory"
            }, indent=2)
        
        # Extract GSE and GSM IDs from metadata files
        gse_ids = []
        gsm_ids = []
        
        for file_path in metadata_files:
            try:
                with open(file_path, 'r') as f:
                    metadata = json.load(f)
                
                # Check for GSE metadata (new structure)
                if metadata.get('gse_id'):
                    gse_ids.append(metadata.get('gse_id', ''))
                # Check for GSM metadata (new structure)
                elif metadata.get('gsm_id'):
                    gsm_ids.append(metadata.get('gsm_id', ''))
                # Fallback to old structure
                elif metadata.get('type') == 'GSE':
                    gse_ids.append(metadata.get('geo_accession', ''))
                elif metadata.get('type') == 'GSM':
                    gsm_ids.append(metadata.get('geo_accession', ''))
                    
            except Exception as e:
                print(f"⚠️ Warning: Could not parse {file_path}: {e}")
                continue
        
        # Get mapping from database
        with get_geometadb_manager(db_path) as manager:
            if gse_ids:
                mapping_result = manager.get_series_sample_mapping(gse_ids)
            else:
                mapping_result = manager.get_series_sample_mapping()
        
        if "error" in mapping_result:
            return json.dumps({
                "mapping": {},
                "total_series": 0,
                "total_samples": 0,
                "error": mapping_result["error"]
            }, indent=2)
        
        # Add required fields for SeriesSampleMapping model
        mapping_result["reverse_mapping"] = {}
        for gse_id, gsm_list in mapping_result["mapping"].items():
            for gsm_id in gsm_list:
                mapping_result["reverse_mapping"][gsm_id] = gse_id
        
        mapping_result["generated_at"] = str(session_path)
        mapping_result["session_directory"] = str(session_path)
        
        # Save mapping to file
        mapping_file = session_path / "series_sample_mapping.json"
        with open(mapping_file, 'w') as f:
            json.dump(mapping_result, f, indent=2, default=str)
        
        return json.dumps(mapping_result, indent=2, default=str)
        
    except Exception as e:
        error_msg = f"Error creating series-sample mapping: {str(e)}"
        print(f"❌ {error_msg}")
        print("🔍 Full traceback:")
        traceback.print_exc()
        return json.dumps({
            "mapping": {},
            "total_series": 0,
            "total_samples": 0,
            "error": error_msg
        }, indent=2)


def search_geo_sqlite_impl(
    query: str,
    search_type: str = "all",
    limit: int = 100,
    db_path: str = "data/GEOmetadb.sqlite"
) -> str:
    """
    Search GEO database using local SQLite database.
    
    This function provides search functionality across GSE, GSM, and GPL records
    using SQL LIKE queries on the local database.
    
    Parameters
    ----------
    query : str
        Search query string.
    search_type : str
        Type of search: 'all', 'gse', 'gsm', 'gpl'.
    limit : int
        Maximum number of results to return.
    db_path : str
        Path to the GEOmetadb SQLite database.
        
    Returns
    -------
    str
        JSON string with search results.
    """
    try:
        with get_geometadb_manager(db_path) as manager:
            results = manager.search_geo(query, search_type, limit)
        
        if "error" in results:
            return json.dumps({
                "query": query,
                "search_type": search_type,
                "results": [],
                "total_results": 0,
                "error": results["error"]
            }, indent=2)
        
        return json.dumps(results, indent=2, default=str)
        
    except Exception as e:
        error_msg = f"Error searching GEO database: {str(e)}"
        print(f"❌ {error_msg}")
        print("🔍 Full traceback:")
        traceback.print_exc()
        return json.dumps({
            "query": query,
            "search_type": search_type,
            "results": [],
            "total_results": 0,
            "error": error_msg
        }, indent=2)


def get_database_info_sqlite_impl(db_path: str = "data/GEOmetadb.sqlite") -> str:
    """
    Get information about the local GEOmetadb database.
    
    This function provides database statistics, table information, and
    other useful metadata about the local SQLite database.
    
    Parameters
    ----------
    db_path : str
        Path to the GEOmetadb SQLite database.
        
    Returns
    -------
    str
        JSON string with database information.
    """
    try:
        with get_geometadb_manager(db_path) as manager:
            info = manager.get_database_info()
        
        return json.dumps(info, indent=2, default=str)
        
    except Exception as e:
        error_msg = f"Error getting database info: {str(e)}"
        print(f"❌ {error_msg}")
        print("🔍 Full traceback:")
        traceback.print_exc()
        return json.dumps({"error": error_msg}, indent=2)


def download_geometadb_impl(db_path: str = "data/GEOmetadb.sqlite", force: bool = False) -> str:
    """
    Download the GEOmetadb SQLite database.
    
    This function downloads the latest version of the GEOmetadb database
    from the official source and extracts it for local use.
    
    Parameters
    ----------
    db_path : str
        Path where the database should be saved.
    force : bool
        Force download even if database already exists.
        
    Returns
    -------
    str
        Success or error message.
    """
    try:
        success = download_geometadb(db_path, force)
        
        if success:
            return f"✅ GEOmetadb database successfully downloaded to {db_path}"
        else:
            return "❌ Failed to download GEOmetadb database"
            
    except Exception as e:
        error_msg = f"Error downloading GEOmetadb database: {str(e)}"
        print(f"❌ {error_msg}")
        print("🔍 Full traceback:")
        traceback.print_exc()
        return error_msg
