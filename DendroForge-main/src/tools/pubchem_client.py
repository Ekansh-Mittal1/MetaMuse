"""
PubChem REST API client for DendroForge.

This module provides a client for the PubChem PUG-REST API to search for chemical
compounds and retrieve their associated literature information. It focuses on
accessing chemical data and cross-references to PubMed articles.

PubChem (https://pubchem.ncbi.nlm.nih.gov) is a public repository for chemical
information maintained by the US National Institutes of Health. It provides
programmatic access through the PUG-REST API.

The tool emphasizes **reliability** and **robustness** over performance, using
only the standard library for HTTP requests to avoid external dependencies.

----------
Public API
----------

search_compounds_by_name
    Search for chemical compounds by name and return basic information
    including CIDs (Compound IDs).

get_compound_literature
    Retrieve PubMed article IDs (PMIDs) associated with a compound.

get_compound_details
    Get detailed information about a compound including properties,
    synonyms, and structure.

search_compounds_by_topic
    Search for compounds related to a topic using MeSH terms or chemical
    names, returning compounds with their literature associations.

get_paper_content
    Retrieve full paper content including abstract and full-text when available
    for a given PMID.

get_papers_content
    Retrieve full paper content for multiple PMIDs.

Notes
-----
1. **Rate limiting** - PubChem enforces a 3 requests/second limit. A simple
   throttling mechanism is included to respect this limit.
2. **Error handling** - Network/HTTP issues raise descriptive RuntimeError
   exceptions that callers can handle appropriately.
3. **Type hints** and comprehensive docstrings follow the project's guidelines.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
import urllib.error
import urllib.parse
import urllib.request

__all__ = [
    "PubChemClient",
    "search_compounds_by_name", 
    "get_compound_literature",
    "get_compound_details",
    "search_compounds_by_topic",
    "get_paper_content",
    "get_papers_content",
]


@dataclass(slots=True)
class PubChemClient:
    """Client for the PubChem PUG-REST API.
    
    Parameters
    ----------
    server : str, default 'https://pubchem.ncbi.nlm.nih.gov'
        Base URL of the PubChem REST service.
    reqs_per_sec : int, default 3
        Maximum number of requests per second to self-throttle at.
        PubChem limits requests to 3 per second.
    """
    
    server: str = "https://pubchem.ncbi.nlm.nih.gov"
    reqs_per_sec: int = 3
    
    # Internal bookkeeping for rate limiting
    _req_counter: int = 0
    _last_reset: float = 0.0
    
    def __post_init__(self):
        """Initialize the rate limiter after object creation."""
        self._last_reset = time.time()
    
    def _rate_limit(self) -> None:
        """Sleep if we have exceeded reqs_per_sec during the last second."""
        now = time.time()
        if now - self._last_reset >= 1.0:
            # Reset the window
            self._req_counter = 0
            self._last_reset = now
            return
            
        if self._req_counter >= self.reqs_per_sec:
            sleep_time = 1.0 - (now - self._last_reset)
            if sleep_time > 0:
                time.sleep(sleep_time)
            # Reset after sleep
            self._req_counter = 0
            self._last_reset = time.time()
    
    def perform_request(
        self,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
    ) -> Union[Dict[str, Any], List[Any], str]:
        """Issue one HTTP GET request and return the response.
        
        Parameters
        ----------
        endpoint : str
            The path part of the URL including leading '/'.
        params : dict, optional
            Query parameters to append to the URL.
        headers : dict, optional
            HTTP headers to include in the request.
        timeout : int, default 30
            Request timeout in seconds.
            
        Returns
        -------
        Union[Dict, List, str]
            Parsed response - JSON as dict/list or text as string.
            
        Raises
        ------
        RuntimeError
            If the request fails or returns an error status.
        """
        self._rate_limit()
        
        # Build URL with parameters
        url = f"{self.server}{endpoint}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        
        # Prepare headers
        request_headers = {"User-Agent": "DendroForge PubChem Client"}
        if headers:
            request_headers.update(headers)
        
        request = urllib.request.Request(url, headers=request_headers)
        
        try:
            response = urllib.request.urlopen(request, timeout=timeout)  # nosec B310
            raw_data = response.read()
            self._req_counter += 1
            
            # Try to parse as JSON, fall back to text
            try:
                return json.loads(raw_data.decode())
            except json.JSONDecodeError:
                return raw_data.decode()
                
        except urllib.error.HTTPError as e:
            # Handle specific HTTP errors
            if e.code == 404:
                return None
            elif e.code == 400:
                raise RuntimeError(f"Bad request to PubChem API: {e.reason}")
            elif e.code == 503:
                raise RuntimeError("PubChem service temporarily unavailable")
            else:
                raise RuntimeError(f"PubChem API error - status {e.code}: {e.reason}")
        except urllib.error.URLError as e:
            raise RuntimeError(f"Network error contacting PubChem: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error in PubChem request: {e}")
    
    def perform_eutils_request(
        self,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
    ) -> Union[Dict[str, Any], List[Any], str]:
        """Issue one HTTP GET request to NCBI E-utilities and return the response.
        
        Parameters
        ----------
        endpoint : str
            The path part of the URL including leading '/'.
        params : dict, optional
            Query parameters to append to the URL.
        headers : dict, optional
            HTTP headers to include in the request.
        timeout : int, default 30
            Request timeout in seconds.
            
        Returns
        -------
        Union[Dict, List, str]
            Parsed response - JSON as dict/list or text as string.
            
        Raises
        ------
        RuntimeError
            If the request fails or returns an error status.
        """
        self._rate_limit()
        
        # Build URL with parameters
        eutils_server = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
        url = f"{eutils_server}{endpoint}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        
        # Prepare headers
        request_headers = {"User-Agent": "DendroForge PubChem Client"}
        if headers:
            request_headers.update(headers)
        
        request = urllib.request.Request(url, headers=request_headers)
        
        try:
            response = urllib.request.urlopen(request, timeout=timeout)  # nosec B310
            raw_data = response.read()
            self._req_counter += 1
            
            # Try to parse as JSON, fall back to text
            try:
                return json.loads(raw_data.decode())
            except json.JSONDecodeError:
                return raw_data.decode()
                
        except urllib.error.HTTPError as e:
            # Handle specific HTTP errors
            if e.code == 404:
                return None
            elif e.code == 400:
                raise RuntimeError(f"Bad request to E-utilities API: {e.reason}")
            elif e.code == 503:
                raise RuntimeError("E-utilities service temporarily unavailable")
            else:
                raise RuntimeError(f"E-utilities API error - status {e.code}: {e.reason}")
        except urllib.error.URLError as e:
            raise RuntimeError(f"Network error contacting E-utilities: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error in E-utilities request: {e}")
    
    def search_compounds_by_name(
        self, 
        name: str, 
        max_results: int = 10
    ) -> List[Dict[str, Any]]:
        """Search for chemical compounds by name.
        
        Parameters
        ----------
        name : str
            Chemical name to search for.
        max_results : int, default 10
            Maximum number of results to return.
            
        Returns
        -------
        List[Dict[str, Any]]
            List of compound information dictionaries with CID, name, etc.
        """
        # First search for CIDs by name
        endpoint = f"/rest/pug/compound/name/{urllib.parse.quote(name)}/cids/JSON"
        
        try:
            response = self.perform_request(endpoint)
            if not response or 'IdentifierList' not in response:
                return []
            
            cids = response['IdentifierList']['CID'][:max_results]
            
            # Get detailed information for each CID
            compounds = []
            for cid in cids:
                compound_info = self.get_compound_details(cid)
                if compound_info:
                    compounds.append(compound_info)
                    
            return compounds
            
        except Exception as e:
            raise RuntimeError(f"Error searching compounds by name '{name}': {e}")
    
    def get_compound_literature(self, cid: int) -> Dict[str, Any]:
        """Get literature information for a compound.
        
        Parameters
        ----------
        cid : int
            PubChem Compound ID.
            
        Returns
        -------
        Dict[str, Any]
            Dictionary containing literature information including PMIDs.
        """
        literature_info = {
            'cid': cid,
            'depositor_pmids': [],
            'mesh_pmids': [],
            'total_pmids': 0
        }
        
        try:
            # Get depositor-provided PMIDs
            endpoint = f"/rest/pug/compound/cid/{cid}/xrefs/PubMedID/JSON"
            response = self.perform_request(endpoint)
            if response and 'InformationList' in response:
                info_list = response['InformationList']['Information']
                for info in info_list:
                    if 'PubMedID' in info:
                        literature_info['depositor_pmids'].extend(info['PubMedID'])
            
            # Get MeSH-associated PMIDs (approximation using compound properties)
            # This is a simplified approach - in practice, MeSH associations are complex
            # Note: MeSH associations would require additional processing to get actual PMIDs
            # For now, we'll skip this as it's not directly available via the API
            
            # Calculate total unique PMIDs
            all_pmids = set(literature_info['depositor_pmids'] + literature_info['mesh_pmids'])
            literature_info['total_pmids'] = len(all_pmids)
            
            return literature_info
            
        except Exception as e:
            raise RuntimeError(f"Error getting literature for CID {cid}: {e}")
    
    def get_compound_details(self, cid: int) -> Optional[Dict[str, Any]]:
        """Get detailed information about a compound.
        
        Parameters
        ----------
        cid : int
            PubChem Compound ID.
            
        Returns
        -------
        Optional[Dict[str, Any]]
            Compound information dictionary or None if not found.
        """
        try:
            # Get basic compound properties in multiple requests to avoid API issues
            # First, get basic properties
            endpoint = f"/rest/pug/compound/cid/{cid}/property/MolecularFormula,MolecularWeight/JSON"
            response = self.perform_request(endpoint)
            
            if not response or 'PropertyTable' not in response:
                return None
            
            properties = response['PropertyTable']['Properties'][0]
            
            # Check if we got valid properties (not empty)
            if not properties.get('MolecularFormula') and not properties.get('MolecularWeight'):
                return None
            
            # Get additional properties
            endpoint = f"/rest/pug/compound/cid/{cid}/property/IUPACName,SMILES/JSON"
            response2 = self.perform_request(endpoint)
            
            if response2 and 'PropertyTable' in response2:
                properties.update(response2['PropertyTable']['Properties'][0])
            
            # Get synonyms
            synonym_endpoint = f"/rest/pug/compound/cid/{cid}/synonyms/JSON"
            synonym_response = self.perform_request(synonym_endpoint)
            synonyms = []
            if synonym_response and 'InformationList' in synonym_response:
                info_list = synonym_response['InformationList']['Information']
                if info_list and 'Synonym' in info_list[0]:
                    synonyms = info_list[0]['Synonym'][:10]  # Limit to first 10
            
            return {
                'cid': cid,
                'molecular_formula': properties.get('MolecularFormula', ''),
                'molecular_weight': float(properties.get('MolecularWeight', 0)),
                'iupac_name': properties.get('IUPACName', ''),
                'canonical_smiles': properties.get('SMILES', ''),
                'synonyms': synonyms,
            }
            
        except Exception as e:
            raise RuntimeError(f"Error getting details for CID {cid}: {e}")
    
    def search_compounds_by_topic(
        self, 
        topic: str, 
        max_compounds: int = 20
    ) -> List[Dict[str, Any]]:
        """Search for compounds related to a topic.
        
        This searches for compounds by topic using multiple strategies:
        1. Direct name search
        2. Search for compounds with related synonyms
        3. Return compounds with their literature associations
        
        Parameters
        ----------
        topic : str
            Topic or keyword to search for.
        max_compounds : int, default 20
            Maximum number of compounds to return.
            
        Returns
        -------
        List[Dict[str, Any]]
            List of compound dictionaries with literature information.
        """
        compounds = []
        
        try:
            # Strategy 1: Direct name search
            try:
                name_results = self.search_compounds_by_name(topic, max_compounds // 2)
                compounds.extend(name_results)
            except:
                pass  # Skip if direct name search fails
            
            # Strategy 2: Search with topic-related compound names
            # This is a simplified approach - could be enhanced with more sophisticated matching
            search_terms = []
            
            # Add specific compound names based on topic
            if topic.lower() in ['diabetes', 'diabetic']:
                search_terms.extend(['metformin', 'insulin', 'glucose', 'glipizide'])
            elif topic.lower() in ['cancer', 'oncology']:
                search_terms.extend(['doxorubicin', 'cisplatin', 'paclitaxel', 'tamoxifen'])
            elif topic.lower() in ['antimicrobial', 'antibiotic']:
                search_terms.extend(['penicillin', 'amoxicillin', 'ciprofloxacin', 'azithromycin'])
            elif topic.lower() in ['antioxidant']:
                search_terms.extend(['vitamin E', 'ascorbic acid', 'tocopherol', 'quercetin'])
            elif topic.lower() in ['anti-inflammatory', 'inflammation']:
                search_terms.extend(['aspirin', 'ibuprofen', 'naproxen', 'celecoxib'])
            else:
                # Generic search terms
                search_terms.extend([
                    f"{topic} acid",
                    f"{topic}ol",
                    f"{topic}ine",
                ])
            
            for term in search_terms:
                if len(compounds) >= max_compounds:
                    break
                try:
                    term_results = self.search_compounds_by_name(term, 3)
                    compounds.extend(term_results)
                except:
                    continue  # Skip failed searches
            
            # Remove duplicates based on CID
            seen_cids = set()
            unique_compounds = []
            for compound in compounds:
                cid = compound.get('cid')
                if cid and cid not in seen_cids:
                    seen_cids.add(cid)
                    unique_compounds.append(compound)
            
            # Limit results and add literature information
            results = []
            for compound in unique_compounds[:max_compounds]:
                try:
                    literature = self.get_compound_literature(compound['cid'])
                    compound.update(literature)
                    results.append(compound)
                except:
                    # Include compound even if literature retrieval fails
                    results.append(compound)
            
            return results
            
        except Exception as e:
            raise RuntimeError(f"Error searching compounds by topic '{topic}': {e}")
    
    def get_paper_content(self, pmid: int) -> Dict[str, Any]:
        """Get full paper content for a given PMID.
        
        Parameters
        ----------
        pmid : int
            PubMed ID for the paper.
            
        Returns
        -------
        Dict[str, Any]
            Dictionary containing paper content including abstract, full-text when available,
            title, authors, journal, and other metadata.
        """
        paper_info = {
            'pmid': pmid,
            'title': '',
            'abstract': '',
            'authors': [],
            'journal': '',
            'publication_date': '',
            'doi': '',
            'full_text_available': False,
            'full_text': '',
            'keywords': [],
            'mesh_terms': [],
        }
        
        try:
            # Get abstract and metadata from PubMed
            endpoint = "/efetch.fcgi"
            params = {
                'db': 'pubmed',
                'id': str(pmid),
                'retmode': 'xml',
                'rettype': 'abstract'
            }
            
            response = self.perform_eutils_request(endpoint, params=params)
            if response and isinstance(response, str):
                # Parse XML response to extract information
                paper_info.update(self._parse_pubmed_xml(response))
            
            # Try to get full text from PMC if available
            try:
                pmc_id = self._get_pmc_id(pmid)
                if pmc_id:
                    full_text = self._get_pmc_full_text(pmc_id)
                    if full_text:
                        paper_info['full_text_available'] = True
                        paper_info['full_text'] = full_text
            except:
                pass  # Full text not available or error occurred
            
            return paper_info
            
        except Exception as e:
            raise RuntimeError(f"Error getting paper content for PMID {pmid}: {e}")
    
    def get_papers_content(self, pmids: List[int]) -> List[Dict[str, Any]]:
        """Get full paper content for multiple PMIDs.
        
        Parameters
        ----------
        pmids : List[int]
            List of PubMed IDs for the papers.
            
        Returns
        -------
        List[Dict[str, Any]]
            List of dictionaries containing paper content for each PMID.
        """
        papers = []
        for pmid in pmids:
            try:
                paper_content = self.get_paper_content(pmid)
                papers.append(paper_content)
            except Exception as e:
                # Include error information for failed retrievals
                papers.append({
                    'pmid': pmid,
                    'error': str(e),
                    'title': '',
                    'abstract': '',
                    'authors': [],
                    'journal': '',
                    'publication_date': '',
                    'doi': '',
                    'full_text_available': False,
                    'full_text': '',
                    'keywords': [],
                    'mesh_terms': [],
                })
        
        return papers
    
    def _parse_pubmed_xml(self, xml_content: str) -> Dict[str, Any]:
        """Parse PubMed XML content to extract paper information.
        
        Parameters
        ----------
        xml_content : str
            XML content from PubMed efetch.
            
        Returns
        -------
        Dict[str, Any]
            Parsed paper information.
        """
        import re
        
        paper_info = {}
        
        # Extract title
        title_match = re.search(r'<ArticleTitle>(.*?)</ArticleTitle>', xml_content, re.DOTALL)
        if title_match:
            paper_info['title'] = title_match.group(1).strip()
        
        # Extract abstract
        abstract_match = re.search(r'<Abstract>(.*?)</Abstract>', xml_content, re.DOTALL)
        if abstract_match:
            # Remove XML tags from abstract
            abstract_text = re.sub(r'<[^>]+>', '', abstract_match.group(1))
            paper_info['abstract'] = abstract_text.strip()
        
        # Extract authors
        authors = []
        author_matches = re.findall(r'<Author.*?>(.*?)</Author>', xml_content, re.DOTALL)
        for author_match in author_matches:
            last_name_match = re.search(r'<LastName>(.*?)</LastName>', author_match)
            first_name_match = re.search(r'<ForeName>(.*?)</ForeName>', author_match)
            if last_name_match and first_name_match:
                authors.append(f"{first_name_match.group(1)} {last_name_match.group(1)}")
        paper_info['authors'] = authors
        
        # Extract journal
        journal_match = re.search(r'<Title>(.*?)</Title>', xml_content)
        if journal_match:
            paper_info['journal'] = journal_match.group(1).strip()
        
        # Extract publication date
        pub_date_match = re.search(r'<PubDate>(.*?)</PubDate>', xml_content, re.DOTALL)
        if pub_date_match:
            year_match = re.search(r'<Year>(\d{4})</Year>', pub_date_match.group(1))
            month_match = re.search(r'<Month>(\w+)</Month>', pub_date_match.group(1))
            if year_match:
                date_str = year_match.group(1)
                if month_match:
                    date_str = f"{month_match.group(1)} {date_str}"
                paper_info['publication_date'] = date_str
        
        # Extract DOI
        doi_match = re.search(r'<ELocationID EIdType="doi">(.*?)</ELocationID>', xml_content)
        if doi_match:
            paper_info['doi'] = doi_match.group(1).strip()
        
        # Extract keywords
        keywords = []
        keyword_matches = re.findall(r'<Keyword.*?>(.*?)</Keyword>', xml_content)
        paper_info['keywords'] = [kw.strip() for kw in keyword_matches]
        
        # Extract MeSH terms
        mesh_terms = []
        mesh_matches = re.findall(r'<DescriptorName.*?>(.*?)</DescriptorName>', xml_content)
        paper_info['mesh_terms'] = [term.strip() for term in mesh_matches]
        
        return paper_info
    
    def _get_pmc_id(self, pmid: int) -> Optional[str]:
        """Get PMC ID for a given PMID if available.
        
        Parameters
        ----------
        pmid : int
            PubMed ID.
            
        Returns
        -------
        Optional[str]
            PMC ID if available, None otherwise.
        """
        try:
            endpoint = "/elink.fcgi"
            params = {
                'dbfrom': 'pubmed',
                'db': 'pmc',
                'id': str(pmid)
            }
            
            response = self.perform_eutils_request(endpoint, params=params)
            if response and isinstance(response, str):
                # Parse XML to find PMC ID specifically from the PMC linkset
                import re
                pmc_match = re.search(r'<LinkSetDb>.*?<DbTo>pmc</DbTo>.*?<Link>.*?<Id>(\d+)</Id>', response, re.DOTALL)
                if pmc_match:
                    return pmc_match.group(1)
            
            return None
            
        except Exception:
            return None
    
    def _get_pmc_full_text(self, pmc_id: str) -> Optional[str]:
        """Get full text from PMC for a given PMC ID.
        
        Parameters
        ----------
        pmc_id : str
            PMC ID.
            
        Returns
        -------
        Optional[str]
            Full text content if available, None otherwise.
        """
        try:
            endpoint = "/efetch.fcgi"
            params = {
                'db': 'pmc',
                'id': pmc_id,
                'retmode': 'xml'
            }
            
            response = self.perform_eutils_request(endpoint, params=params)
            if response and isinstance(response, str):
                # Parse XML to extract meaningful text content
                import re
                
                # Check if we got an error response
                if 'error' in response.lower() and 'not available' in response.lower():
                    return None
                
                # Extract main text sections
                text_sections = []
                
                # Extract abstract
                abstract_match = re.search(r'<abstract[^>]*>(.*?)</abstract>', response, re.DOTALL | re.IGNORECASE)
                if abstract_match:
                    abstract_text = re.sub(r'<[^>]+>', '', abstract_match.group(1))
                    text_sections.append(f"ABSTRACT:\n{abstract_text.strip()}")
                
                # Extract body text
                body_match = re.search(r'<body[^>]*>(.*?)</body>', response, re.DOTALL | re.IGNORECASE)
                if body_match:
                    body_text = re.sub(r'<[^>]+>', '', body_match.group(1))
                    text_sections.append(f"FULL TEXT:\n{body_text.strip()}")
                
                # Extract sections like introduction, methods, results, etc.
                section_matches = re.findall(r'<sec[^>]*>.*?<title[^>]*>(.*?)</title>(.*?)</sec>', response, re.DOTALL | re.IGNORECASE)
                for title, content in section_matches:
                    title_clean = re.sub(r'<[^>]+>', '', title).strip()
                    content_clean = re.sub(r'<[^>]+>', '', content).strip()
                    if title_clean and content_clean:
                        text_sections.append(f"{title_clean.upper()}:\n{content_clean}")
                
                # If we have text sections, join them
                if text_sections:
                    full_text = '\n\n'.join(text_sections)
                    # Clean up excessive whitespace
                    full_text = re.sub(r'\s+', ' ', full_text)
                    full_text = re.sub(r'\n\s*\n', '\n\n', full_text)
                    return full_text.strip()
                
                # Fallback: extract all text content
                text_only = re.sub(r'<[^>]+>', '', response)
                text_only = re.sub(r'\s+', ' ', text_only).strip()
                
                # Only return if we have substantial content (more than just metadata)
                if len(text_only) > 1000:
                    return text_only
            
            return None
            
        except Exception:
            return None


# Module-level convenience functions
def search_compounds_by_name(name: str, max_results: int = 10) -> List[Dict[str, Any]]:
    """Search for chemical compounds by name.
    
    Parameters
    ----------
    name : str
        Chemical name to search for.
    max_results : int, default 10
        Maximum number of results to return.
        
    Returns
    -------
    List[Dict[str, Any]]
        List of compound information dictionaries.
    """
    return PubChemClient().search_compounds_by_name(name, max_results)


def get_compound_literature(cid: int) -> Dict[str, Any]:
    """Get literature information for a compound.
    
    Parameters
    ----------
    cid : int
        PubChem Compound ID.
        
    Returns
    -------
    Dict[str, Any]
        Dictionary containing literature information including PMIDs.
    """
    return PubChemClient().get_compound_literature(cid)


def get_compound_details(cid: int) -> Optional[Dict[str, Any]]:
    """Get detailed information about a compound.
    
    Parameters
    ----------
    cid : int
        PubChem Compound ID.
        
    Returns
    -------
    Optional[Dict[str, Any]]
        Compound information dictionary or None if not found.
    """
    return PubChemClient().get_compound_details(cid)


def search_compounds_by_topic(topic: str, max_compounds: int = 20) -> List[Dict[str, Any]]:
    """Search for compounds related to a topic.
    
    Parameters
    ----------
    topic : str
        Topic or keyword to search for.
    max_compounds : int, default 20
        Maximum number of compounds to return.
        
    Returns
    -------
    List[Dict[str, Any]]
        List of compound dictionaries with literature information.
    """
    return PubChemClient().search_compounds_by_topic(topic, max_compounds)


def get_paper_content(pmid: int) -> Dict[str, Any]:
    """Get full paper content for a given PMID.
    
    Parameters
    ----------
    pmid : int
        PubMed ID for the paper.
        
    Returns
    -------
    Dict[str, Any]
        Dictionary containing paper content including abstract, full-text when available,
        title, authors, journal, and other metadata.
    """
    return PubChemClient().get_paper_content(pmid)


def get_papers_content(pmids: List[int]) -> List[Dict[str, Any]]:
    """Get full paper content for multiple PMIDs.
    
    Parameters
    ----------
    pmids : List[int]
        List of PubMed IDs for the papers.
        
    Returns
    -------
    List[Dict[str, Any]]
        List of dictionaries containing paper content for each PMID.
    """
    return PubChemClient().get_papers_content(pmids)