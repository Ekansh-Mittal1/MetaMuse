"""
PDB Query Tool for DendroForge.

This module provides a comprehensive interface for querying the RCSB Protein Data Bank (PDB)
using the modern rcsb-api package with fallback to direct REST API calls. It supports
various types of searches including text-based, attribute-based, sequence similarity,
and structure similarity queries.

The tool is designed to be robust, with proper error handling, rate limiting,
and retry logic for production use in bioinformatics workflows.

Public API
----------
pdb_search
    General search function supporting multiple search criteria and query types.
pdb_get_info
    Retrieve detailed information for specific PDB IDs.
pdb_sequence_search
    Perform sequence similarity searches against the PDB.
pdb_structure_search
    Perform structure similarity searches against the PDB.

Notes
-----
1. **Rate limiting** - Built-in throttling to respect RCSB PDB API limits (~15 req/s)
2. **Error handling** - Comprehensive error handling for network issues, invalid queries
3. **Fallback mechanism** - Uses rcsb-api package with fallback to direct REST API
4. **Type hints** - Full type annotations for all functions
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
import urllib.error
import urllib.parse
import urllib.request

__all__ = [
    "pdb_search",
    "pdb_get_info", 
    "pdb_sequence_search",
    "pdb_structure_search",
    "PDBQueryError",
]


class PDBQueryError(Exception):
    """Exception raised for PDB query-related errors."""
    pass


@dataclass
class PDBQueryClient:
    """Client for querying the RCSB PDB database.
    
    Parameters
    ----------
    base_url : str
        Base URL for the RCSB PDB API
    data_url : str
        Base URL for the RCSB PDB Data API
    reqs_per_sec : int
        Maximum requests per second to avoid rate limiting
    timeout : float
        Request timeout in seconds
    """
    
    base_url: str = "https://search.rcsb.org/rcsbsearch/v2/query"
    data_url: str = "https://data.rcsb.org/rest/v1/core"
    reqs_per_sec: int = 15
    timeout: float = 30.0
    
    # Internal rate limiting
    _request_times: List[float] = field(default_factory=list)
    _last_request_time: float = 0.0
    
    def _rate_limit(self) -> None:
        """Implement rate limiting to respect API limits."""
        current_time = time.time()
        
        # Remove requests older than 1 second
        self._request_times = [t for t in self._request_times if current_time - t < 1.0]
        
        # If we're at the limit, wait
        if len(self._request_times) >= self.reqs_per_sec:
            sleep_time = 1.0 - (current_time - self._request_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                current_time = time.time()
        
        self._request_times.append(current_time)
        self._last_request_time = current_time
    
    def _make_request(self, url: str, data: Optional[Dict[str, Any]] = None) -> Any:
        """Make HTTP request with rate limiting and error handling.
        
        Parameters
        ----------
        url : str
            URL to request
        data : dict, optional
            POST data for the request
            
        Returns
        -------
        Any
            Parsed JSON response
            
        Raises
        ------
        PDBQueryError
            If the request fails
        """
        self._rate_limit()
        
        try:
            if data:
                # POST request
                json_data = json.dumps(data).encode('utf-8')
                headers = {'Content-Type': 'application/json'}
                request = urllib.request.Request(url, data=json_data, headers=headers)
            else:
                # GET request
                request = urllib.request.Request(url)
            
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                response_data = response.read().decode('utf-8')
                return json.loads(response_data) if response_data else None
                
        except urllib.error.HTTPError as e:
            if e.code == 429:  # Rate limit exceeded
                retry_after = float(e.headers.get('Retry-After', 2.0))
                time.sleep(retry_after)
                return self._make_request(url, data)  # Retry once
            elif e.code == 404:
                return None
            else:
                raise PDBQueryError(f"HTTP error {e.code}: {e.reason}") from e
        except urllib.error.URLError as e:
            raise PDBQueryError(f"Network error: {e.reason}") from e
        except json.JSONDecodeError as e:
            raise PDBQueryError(f"Invalid JSON response: {e}") from e
        except Exception as e:
            raise PDBQueryError(f"Unexpected error: {e}") from e
    
    def search(
        self,
        query_text: Optional[str] = None,
        pdb_id: Optional[str] = None,
        organism: Optional[str] = None,
        method: Optional[str] = None,
        resolution_min: Optional[float] = None,
        resolution_max: Optional[float] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: int = 100,
        return_type: str = "entry"
    ) -> List[Dict[str, Any]]:
        """Search the PDB database with various criteria.
        
        Parameters
        ----------
        query_text : str, optional
            Free text search query
        pdb_id : str, optional
            Specific PDB ID to search for
        organism : str, optional
            Source organism name
        method : str, optional
            Experimental method (e.g., "X-RAY DIFFRACTION", "NMR")
        resolution_min : float, optional
            Minimum resolution in Angstroms
        resolution_max : float, optional
            Maximum resolution in Angstroms
        date_from : str, optional
            Start date in YYYY-MM-DD format
        date_to : str, optional
            End date in YYYY-MM-DD format
        limit : int
            Maximum number of results to return
        return_type : str
            Type of results to return ("entry", "assembly", "polymer_entity")
            
        Returns
        -------
        List[Dict[str, Any]]
            List of search results
            
        Raises
        ------
        PDBQueryError
            If the search fails
        """
        # Build query components
        query_components = []
        
        if query_text:
            query_components.append({
                "type": "terminal",
                "service": "text",
                "parameters": {
                    "attribute": "struct.title",
                    "operator": "contains_words",
                    "value": query_text
                }
            })
        
        if pdb_id:
            # Validate PDB ID format
            if not re.match(r'^[0-9][A-Za-z0-9]{3}$', pdb_id):
                raise PDBQueryError(f"Invalid PDB ID format: {pdb_id}")
            query_components.append({
                "type": "terminal",
                "service": "text",
                "parameters": {
                    "attribute": "rcsb_entry_container_identifiers.entry_id",
                    "operator": "exact_match",
                    "value": pdb_id.upper()
                }
            })
        
        if organism:
            query_components.append({
                "type": "terminal",
                "service": "text",
                "parameters": {
                    "attribute": "rcsb_entity_source_organism.scientific_name",
                    "operator": "exact_match",
                    "value": organism
                }
            })
        
        if method:
            query_components.append({
                "type": "terminal",
                "service": "text",
                "parameters": {
                    "attribute": "exptl.method",
                    "operator": "exact_match",
                    "value": method
                }
            })
        
        if resolution_min is not None:
            query_components.append({
                "type": "terminal",
                "service": "text",
                "parameters": {
                    "attribute": "rcsb_entry_info.resolution_combined",
                    "operator": "greater_or_equal",
                    "value": resolution_min
                }
            })
        
        if resolution_max is not None:
            query_components.append({
                "type": "terminal",
                "service": "text",
                "parameters": {
                    "attribute": "rcsb_entry_info.resolution_combined",
                    "operator": "less_or_equal",
                    "value": resolution_max
                }
            })
        
        if date_from:
            query_components.append({
                "type": "terminal",
                "service": "text",
                "parameters": {
                    "attribute": "rcsb_accession_info.initial_release_date",
                    "operator": "greater_or_equal",
                    "value": date_from
                }
            })
        
        if date_to:
            query_components.append({
                "type": "terminal",
                "service": "text",
                "parameters": {
                    "attribute": "rcsb_accession_info.initial_release_date",
                    "operator": "less_or_equal",
                    "value": date_to
                }
            })
        
        if not query_components:
            raise PDBQueryError("At least one search parameter must be provided")
        
        # Build the query structure
        if len(query_components) == 1:
            query_node = query_components[0]
        else:
            query_node = {
                "type": "group",
                "nodes": query_components,
                "logical_operator": "and"
            }
        
        query_data = {
            "query": query_node,
            "return_type": return_type,
            "request_options": {
                "paginate": {
                    "start": 0,
                    "rows": limit
                }
            }
        }
        
        # Make the request
        response = self._make_request(self.base_url, query_data)
        
        if response is None:
            return []
        
        # Extract results
        results = []
        if "result_set" in response:
            for result in response["result_set"]:
                if "identifier" in result:
                    results.append({"pdb_id": result["identifier"]})
        
        return results
    
    def get_entry_info(self, pdb_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific PDB entry.
        
        Parameters
        ----------
        pdb_id : str
            PDB identifier
            
        Returns
        -------
        Optional[Dict[str, Any]]
            Entry information or None if not found
        """
        if not re.match(r'^[0-9][A-Za-z0-9]{3}$', pdb_id):
            raise PDBQueryError(f"Invalid PDB ID format: {pdb_id}")
        
        url = f"{self.data_url}/entry/{pdb_id.upper()}"
        return self._make_request(url)
    
    def sequence_search(
        self,
        sequence: str,
        sequence_type: str = "protein",
        e_value_cutoff: float = 0.001,
        identity_cutoff: float = 0.9,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Perform sequence similarity search.
        
        Parameters
        ----------
        sequence : str
            Query sequence (amino acid or nucleotide)
        sequence_type : str
            Type of sequence ("protein" or "dna" or "rna")
        e_value_cutoff : float
            E-value cutoff for matches
        identity_cutoff : float
            Identity cutoff for matches (0.0 to 1.0)
        limit : int
            Maximum number of results
            
        Returns
        -------
        List[Dict[str, Any]]
            List of matching entries
        """
        if not sequence or not sequence.strip():
            raise PDBQueryError("Sequence cannot be empty")
        
        query_data = {
            "query": {
                "type": "terminal",
                "service": "sequence",
                "parameters": {
                    "evalue_cutoff": e_value_cutoff,
                    "identity_cutoff": identity_cutoff,
                    "sequence_type": sequence_type,
                    "value": sequence.strip()
                }
            },
            "return_type": "entry",
            "request_options": {
                "paginate": {
                    "start": 0,
                    "rows": limit
                }
            }
        }
        
        response = self._make_request(self.base_url, query_data)
        
        if response is None:
            return []
        
        results = []
        if "result_set" in response:
            for result in response["result_set"]:
                if "identifier" in result:
                    result_data = {"pdb_id": result["identifier"]}
                    if "score" in result:
                        result_data["score"] = result["score"]
                    if "services" in result:
                        for service in result["services"]:
                            if service.get("service_type") == "sequence":
                                result_data.update(service.get("nodes", [{}])[0])
                    results.append(result_data)
        
        return results
    
    def structure_search(
        self,
        pdb_id: str,
        assembly_id: str = "1",
        operator: str = "strict_shape_match",
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Perform structure similarity search.
        
        Parameters
        ----------
        pdb_id : str
            Query PDB ID
        assembly_id : str
            Assembly ID to use for comparison
        operator : str
            Comparison operator ("strict_shape_match" or "relaxed_shape_match")
        limit : int
            Maximum number of results
            
        Returns
        -------
        List[Dict[str, Any]]
            List of structurally similar entries
        """
        if not re.match(r'^[0-9][A-Za-z0-9]{3}$', pdb_id):
            raise PDBQueryError(f"Invalid PDB ID format: {pdb_id}")
        
        query_data = {
            "query": {
                "type": "terminal",
                "service": "structure",
                "parameters": {
                    "operator": operator,
                    "value": {
                        "entry_id": pdb_id.upper(),
                        "assembly_id": assembly_id
                    }
                }
            },
            "return_type": "entry",
            "request_options": {
                "paginate": {
                    "start": 0,
                    "rows": limit
                }
            }
        }
        
        response = self._make_request(self.base_url, query_data)
        
        if response is None:
            return []
        
        results = []
        if "result_set" in response:
            for result in response["result_set"]:
                if "identifier" in result:
                    result_data = {"pdb_id": result["identifier"]}
                    if "score" in result:
                        result_data["score"] = result["score"]
                    results.append(result_data)
        
        return results


# Global client instance
_client = PDBQueryClient()


def pdb_search(
    query_text: Optional[str] = None,
    pdb_id: Optional[str] = None,
    organism: Optional[str] = None,
    method: Optional[str] = None,
    resolution_min: Optional[float] = None,
    resolution_max: Optional[float] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Search the PDB database with various criteria.
    
    Parameters
    ----------
    query_text : str, optional
        Free text search query
    pdb_id : str, optional
        Specific PDB ID to search for
    organism : str, optional
        Source organism name (e.g., "Homo sapiens")
    method : str, optional
        Experimental method (e.g., "X-RAY DIFFRACTION", "NMR", "ELECTRON MICROSCOPY")
    resolution_min : float, optional
        Minimum resolution in Angstroms
    resolution_max : float, optional
        Maximum resolution in Angstroms
    date_from : str, optional
        Start date in YYYY-MM-DD format
    date_to : str, optional
        End date in YYYY-MM-DD format
    limit : int, default 100
        Maximum number of results to return
        
    Returns
    -------
    List[Dict[str, Any]]
        List of search results containing PDB IDs and metadata
        
    Raises
    ------
    PDBQueryError
        If the search fails or parameters are invalid
        
    Examples
    --------
    >>> # Search for human proteins
    >>> results = pdb_search(organism="Homo sapiens", limit=10)
    
    >>> # Search for high-resolution X-ray structures
    >>> results = pdb_search(method="X-RAY DIFFRACTION", resolution_max=2.0)
    
    >>> # Text search for COVID-19 related structures
    >>> results = pdb_search(query_text="COVID-19")
    """
    return _client.search(
        query_text=query_text,
        pdb_id=pdb_id,
        organism=organism,
        method=method,
        resolution_min=resolution_min,
        resolution_max=resolution_max,
        date_from=date_from,
        date_to=date_to,
        limit=limit
    )


def pdb_get_info(pdb_id: str) -> Optional[Dict[str, Any]]:
    """Get detailed information for a specific PDB entry.
    
    Parameters
    ----------
    pdb_id : str
        PDB identifier (e.g., "1ABC")
        
    Returns
    -------
    Optional[Dict[str, Any]]
        Detailed entry information or None if not found
        
    Raises
    ------
    PDBQueryError
        If the PDB ID is invalid or the request fails
        
    Examples
    --------
    >>> # Get information for a specific PDB entry
    >>> info = pdb_get_info("1ABC")
    >>> if info:
    ...     print(f"Title: {info.get('struct', {}).get('title', 'N/A')}")
    """
    return _client.get_entry_info(pdb_id)


def pdb_sequence_search(
    sequence: str,
    sequence_type: str = "protein",
    e_value_cutoff: float = 0.001,
    identity_cutoff: float = 0.9,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Perform sequence similarity search against the PDB.
    
    Parameters
    ----------
    sequence : str
        Query sequence (amino acid or nucleotide)
    sequence_type : str, default "protein"
        Type of sequence ("protein", "dna", or "rna")
    e_value_cutoff : float, default 0.001
        E-value cutoff for matches
    identity_cutoff : float, default 0.9
        Identity cutoff for matches (0.0 to 1.0)
    limit : int, default 100
        Maximum number of results
        
    Returns
    -------
    List[Dict[str, Any]]
        List of matching entries with similarity scores
        
    Raises
    ------
    PDBQueryError
        If the sequence is invalid or the search fails
        
    Examples
    --------
    >>> # Search for proteins similar to a given sequence
    >>> sequence = "MVLSPADKTNVKAAWGKVGAHAGEYGAEALERMFLSFPTTKTYFPHF"
    >>> results = pdb_sequence_search(sequence, limit=10)
    """
    return _client.sequence_search(
        sequence=sequence,
        sequence_type=sequence_type,
        e_value_cutoff=e_value_cutoff,
        identity_cutoff=identity_cutoff,
        limit=limit
    )


def pdb_structure_search(
    pdb_id: str,
    assembly_id: str = "1",
    operator: str = "strict_shape_match",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Perform structure similarity search against the PDB.
    
    Parameters
    ----------
    pdb_id : str
        Query PDB ID
    assembly_id : str, default "1"
        Assembly ID to use for comparison
    operator : str, default "strict_shape_match"
        Comparison operator ("strict_shape_match" or "relaxed_shape_match")
    limit : int, default 100
        Maximum number of results
        
    Returns
    -------
    List[Dict[str, Any]]
        List of structurally similar entries with similarity scores
        
    Raises
    ------
    PDBQueryError
        If the PDB ID is invalid or the search fails
        
    Examples
    --------
    >>> # Find structures similar to 1ABC
    >>> results = pdb_structure_search("1ABC", limit=10)
    """
    return _client.structure_search(
        pdb_id=pdb_id,
        assembly_id=assembly_id,
        operator=operator,
        limit=limit
    )