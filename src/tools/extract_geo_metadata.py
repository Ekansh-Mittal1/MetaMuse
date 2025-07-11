import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
import urllib.error
import urllib.parse
import urllib.request
import os
from dotenv import load_dotenv


class NCBIClient:
    """
    A client for interacting with NCBI's E-Utilities API.

    This class provides a Python interface to NCBI's E-Utilities API,
    which allows for querying and retrieving data from NCBI's databases.
    """

    def __init__(self):
        load_dotenv()
        
        # Get NCBI credentials from environment variables
        self.email = os.getenv("NCBI_EMAIL")
        self.api_key = os.getenv("NCBI_API_KEY")
        self.api_url = os.getenv("NCBI_API_URL", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/")

        # Validate required email
        if not self.email or not self.api_key or not self.api_url:
            raise ValueError("NCBI_EMAIL, NCBI_API_KEY, and NCBI_API_URL environment variables are required")
        
        # Initialize the E-Utilities client
        self.client = urllib.request.build_opener(
            urllib.request.HTTPHandler(debuglevel=0),
            urllib.request.HTTPSHandler(debuglevel=0),  
        )
        self.client.addheaders = [
            ("User-Agent", "Python-NCBI-E-Utilities/1.0"),
            ("Email", self.email),
        ]
        
        # Ensure API URL ends with a slash
        if not self.api_url.endswith('/'):
            self.api_url += '/'

    def get_gsm_metadata(self, gsm_id: str) -> Dict[str, Any]:
        """
        Retrieve metadata for a GEO Sample (GSM) record using NCBI E-Utilities.
        
        Args:
            gsm_id (str): The GSM ID to retrieve metadata for
            
        Returns:
            Dict containing the metadata response from NCBI
            
        Raises:
            urllib.error.HTTPError: If the request fails
            ValueError: If the GSM ID is invalid
        """
        # Validate GSM ID format
        if not gsm_id.upper().startswith("GSM") or not gsm_id[3:].isdigit():
            raise ValueError(f"Invalid GSM ID format: {gsm_id}")
            
        # Use the GEO soft file format which is more reliable for GSM records
        geo_url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={gsm_id}&targ=self&form=text&view=full"
        
        try:
            # Make the request to GEO
            response = self.client.open(geo_url)
            content = response.read().decode('utf-8')
            
            # Parse the SOFT format response
            metadata = self._parse_soft_format(content, gsm_id)
            
            # Add small delay to respect rate limits
            time.sleep(0.34)  # ~3 requests per second
            
            return metadata
            
        except urllib.error.HTTPError as e:
            raise urllib.error.HTTPError(
                geo_url, e.code, f"Failed to retrieve metadata for {gsm_id}", 
                e.hdrs, e.fp
            )
    
    def get_gse_metadata(self, gse_id: str) -> Dict[str, Any]:
        """
        Retrieve metadata for a GEO Series (GSE) record using GEO website.
        
        Args:
            gse_id (str): The GSE ID to retrieve metadata for
            
        Returns:
            Dict containing the metadata response from GEO
            
        Raises:
            urllib.error.HTTPError: If the request fails
            ValueError: If the GSE ID is invalid
        """
        # Validate GSE ID format
        if not gse_id.upper().startswith("GSE") or not gse_id[3:].isdigit():
            raise ValueError(f"Invalid GSE ID format: {gse_id}")
        
        # Use the GEO soft file format for GSE records
        geo_url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={gse_id}&targ=self&form=text&view=full"
        
        try:
            # Make the request to GEO
            response = self.client.open(geo_url)
            content = response.read().decode('utf-8')
            
            # Parse the SOFT format response
            metadata = self._parse_soft_format(content, gse_id)
            metadata["type"] = "GSE"
            
            # Add small delay to respect rate limits
            time.sleep(0.34)  # ~3 requests per second
            
            return metadata
            
        except urllib.error.HTTPError as e:
            raise urllib.error.HTTPError(
                geo_url, e.code, f"Failed to retrieve metadata for {gse_id}", 
                e.hdrs, e.fp
            )
        except Exception as e:
            raise RuntimeError(f"Error retrieving metadata for {gse_id}: {e}")
    
    def get_paper_abstract(self, pmid: int) -> Dict[str, Any]:
        """
        Get paper abstract and metadata for a given PMID.
        
        Parameters
        ----------
        pmid : int
            PubMed ID for the paper.
            
        Returns
        -------
        Dict[str, Any]
            Dictionary containing paper content including abstract, title, authors, 
            journal, and other metadata.
        """
        paper_info = {
            'pmid': pmid,
            'title': '',
            'abstract': '',
            'authors': [],
            'journal': '',
            'publication_date': '',
            'doi': '',
            'keywords': [],
            'mesh_terms': [],
        }
        
        try:
            # First search for the PMID to get the correct database ID
            search_params = {
                'db': 'pubmed',
                'term': str(pmid),
                'retmode': 'json'
            }
            
            search_url = f"{self.api_url}esearch.fcgi?{urllib.parse.urlencode(search_params)}"
            
            # Use urllib.request directly instead of client
            search_response = urllib.request.urlopen(search_url)
            search_content = search_response.read().decode('utf-8')
            
            if search_content:
                search_data = json.loads(search_content)
                id_list = search_data.get('esearchresult', {}).get('idlist', [])
                
                if id_list:
                    # Use the first ID from search results
                    db_id = id_list[0]
                    
                    # Get metadata using esummary
                    summary_params = {
                        'db': 'pubmed',
                        'id': db_id,
                        'retmode': 'json'
                    }
                    
                    summary_url = f"{self.api_url}esummary.fcgi?{urllib.parse.urlencode(summary_params)}"
                    
                    # Use urllib.request directly instead of client
                    summary_response = urllib.request.urlopen(summary_url)
                    summary_content = summary_response.read().decode('utf-8')
                    
                    if summary_content:
                        try:
                            json_data = json.loads(summary_content)
                            paper_info.update(self._parse_pubmed_json(json_data, pmid))
                        except json.JSONDecodeError:
                            # Fallback to XML if JSON fails
                            paper_info.update(self._parse_pubmed_xml(summary_content))
                    
                    # Get full paper content using efetch to extract abstract
                    fetch_params = {
                        'db': 'pubmed',
                        'id': db_id,
                        'retmode': 'xml',
                        'rettype': 'abstract'
                    }
                    
                    fetch_url = f"{self.api_url}efetch.fcgi?{urllib.parse.urlencode(fetch_params)}"
                    
                    # Use urllib.request directly instead of client
                    fetch_response = urllib.request.urlopen(fetch_url)
                    fetch_content = fetch_response.read().decode('utf-8')
                    
                    if fetch_content:
                        # Parse the XML to extract the abstract
                        abstract = self._extract_abstract_from_xml(fetch_content)
                        if abstract:
                            paper_info['abstract'] = abstract
            
            # Add small delay to respect rate limits
            time.sleep(1.0)  # Increased delay to avoid rate limiting
            
            return paper_info
            
        except Exception as e:
            raise RuntimeError(f"Error getting paper abstract for PMID {pmid}: {e}")
    
    def _parse_pubmed_xml(self, xml_content: str) -> Dict[str, Any]:
        """
        Parse PubMed XML response to extract paper information.
        
        Args:
            xml_content (str): XML content from PubMed
            
        Returns:
            Dict containing parsed paper information
        """
        import xml.etree.ElementTree as ET
        
        paper_info = {}
        
        try:
            # Parse XML content
            root = ET.fromstring(xml_content)
            
            # Extract title
            title_elem = root.find('.//ArticleTitle')
            if title_elem is not None:
                paper_info['title'] = title_elem.text or ''
            
            # Extract abstract
            abstract_elem = root.find('.//AbstractText')
            if abstract_elem is not None:
                paper_info['abstract'] = abstract_elem.text or ''
            
            # Extract authors
            authors = []
            for author_elem in root.findall('.//Author'):
                last_name = author_elem.find('LastName')
                first_name = author_elem.find('ForeName')
                if last_name is not None and first_name is not None:
                    authors.append(f"{first_name.text} {last_name.text}")
            paper_info['authors'] = authors
            
            # Extract journal
            journal_elem = root.find('.//Journal/Title')
            if journal_elem is not None:
                paper_info['journal'] = journal_elem.text or ''
            
            # Extract publication date
            pub_date_elem = root.find('.//PubDate')
            if pub_date_elem is not None:
                year_elem = pub_date_elem.find('Year')
                month_elem = pub_date_elem.find('Month')
                if year_elem is not None:
                    date_parts = [year_elem.text]
                    if month_elem is not None:
                        date_parts.append(month_elem.text)
                    paper_info['publication_date'] = ' '.join(date_parts)
            
            # Extract DOI
            doi_elem = root.find('.//ELocationID[@EIdType="doi"]')
            if doi_elem is not None:
                paper_info['doi'] = doi_elem.text or ''
            
            # Extract keywords
            keywords = []
            for keyword_elem in root.findall('.//Keyword'):
                if keyword_elem.text:
                    keywords.append(keyword_elem.text)
            paper_info['keywords'] = keywords
            
            # Extract MeSH terms
            mesh_terms = []
            for mesh_elem in root.findall('.//MeshHeading/DescriptorName'):
                if mesh_elem.text:
                    mesh_terms.append(mesh_elem.text)
            paper_info['mesh_terms'] = mesh_terms
            
        except ET.ParseError as e:
            # If XML parsing fails, try to extract basic information from text
            paper_info['abstract'] = xml_content[:1000] if xml_content else ''
        
        return paper_info

    def _parse_pubmed_json(self, json_data: Dict[str, Any], pmid: int) -> Dict[str, Any]:
        """
        Parse PubMed JSON response to extract paper information.
        
        Args:
            json_data (Dict): JSON data from PubMed
            pmid (int): The PMID
            
        Returns:
            Dict containing parsed paper information
        """
        paper_info = {}
        
        try:
            # Extract data from the JSON response
            result = json_data.get('result', {})
            if str(pmid) in result:
                article_data = result[str(pmid)]
                
                # Extract title
                paper_info['title'] = article_data.get('title', '')
                
                # Extract abstract
                paper_info['abstract'] = article_data.get('abstract', '')
                
                # Extract authors
                authors = article_data.get('authors', [])
                if isinstance(authors, list):
                    paper_info['authors'] = [author.get('name', '') for author in authors if author.get('name')]
                else:
                    paper_info['authors'] = []
                
                # Extract journal
                paper_info['journal'] = article_data.get('fulljournalname', '')
                
                # Extract publication date
                pubdate = article_data.get('pubdate', '')
                paper_info['publication_date'] = pubdate
                
                # Extract DOI
                paper_info['doi'] = article_data.get('elocationid', '')
                
                # Extract keywords and MeSH terms
                paper_info['keywords'] = article_data.get('keywords', [])
                paper_info['mesh_terms'] = article_data.get('mesh', [])
                
        except Exception as e:
            # If JSON parsing fails, return empty data
            pass
        
        return paper_info

    def get_gse_series_matrix(self, gse_id: str) -> Dict[str, Any]:
        """
        Retrieve the series matrix table for a GEO Series (GSE) record.
        Only extracts metadata and sample names, not the actual gene expression data.
        
        Args:
            gse_id (str): The GSE ID to retrieve series matrix for
            
        Returns:
            Dict containing the series matrix metadata and sample names
            
        Raises:
            urllib.error.HTTPError: If the request fails
            ValueError: If the GSE ID is invalid
        """
        # Validate GSE ID format
        if not gse_id.upper().startswith("GSE") or not gse_id[3:].isdigit():
            raise ValueError(f"Invalid GSE ID format: {gse_id}")
        
        try:
            # GEO stores series folders in batches of 1,000: e.g. GSE123 → GSE123nnn
            prefix = gse_id[:-3] + "nnn"
            base_url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{prefix}/{gse_id}/matrix/"
            
            # First, try to get the directory listing to find available matrix files
            try:
                # Try to get directory listing
                dir_response = urllib.request.urlopen(base_url)
                dir_content = dir_response.read().decode('utf-8')
                
                # Parse directory listing to find all series matrix files
                import re
                matrix_files = []
                # Simple pattern to find all files ending with _series_matrix.txt.gz
                pattern = r'<a href="([^"]+_series_matrix\.txt\.gz)">'
                matches = re.findall(pattern, dir_content)
                matrix_files.extend(matches)
                
                if not matrix_files:
                    # Fallback: try the standard naming convention
                    matrix_files = [f"{gse_id}_series_matrix.txt.gz"]
                    
            except:
                # If directory listing fails, try the standard naming convention
                matrix_files = [f"{gse_id}_series_matrix.txt.gz"]
            
            # Process each matrix file found
            all_metadata = {}
            all_samples = []
            all_platforms = []
            
            for matrix_file in matrix_files:
                url = base_url + matrix_file
                
                try:
                    # Download the gzipped matrix
                    response = urllib.request.urlopen(url)
                    gzipped_content = response.read()
                    
                    # Decompress and read the content
                    import gzip
                    from io import BytesIO
                    
                    # Decompress the gzipped content
                    with gzip.open(BytesIO(gzipped_content), 'rt') as f:
                        content = f.read()
                    
                    # Parse the content to extract metadata and sample names only
                    lines = content.split('\n')
                    metadata = {}
                    sample_names = []
                    in_matrix_section = False
                    found_sample_row = False
                    
                    for line in lines:
                        line = line.strip()
                        if line.startswith('!'):
                            # Parse metadata lines
                            if '=' in line:
                                key, value = line.split('=', 1)
                                key = key.replace('!', '').strip()
                                value = value.strip()
                                metadata[key] = value
                        elif line.startswith('!series_matrix_table_begin'):
                            # Mark the beginning of the matrix section
                            in_matrix_section = True
                        elif in_matrix_section and line and not line.startswith('^') and not found_sample_row:
                            # This is the first row after the table begin marker - sample names
                            sample_names = line.split('\t')
                            found_sample_row = True
                            # Stop processing after getting sample names
                            break
                    
                    # Extract platform ID from filename
                    platform_id = matrix_file.replace(f"{gse_id}-", "").replace("_series_matrix.txt.gz", "")
                    
                    # Store metadata for this platform
                    all_metadata[platform_id] = metadata
                    
                    # Extract sample and platform information from metadata
                    for key, value in metadata.items():
                        if 'sample_geo_accession' in key.lower():
                            all_samples.append(value)
                        elif 'platform_geo_accession' in key.lower():
                            all_platforms.append(value)
                    
                    # Add sample names from the matrix header
                    if sample_names:
                        all_samples.extend(sample_names[1:])  # Skip the first column (probe IDs)
                    
                except Exception as e:
                    print(f"Warning: Could not process matrix file {matrix_file}: {e}")
                    continue
            
            # Combine all data
            series_matrix = {
                "gse_id": gse_id,
                "type": "series_matrix_metadata",
                "sample_count": len(set(all_samples)),  # Remove duplicates
                "platform_count": len(set(all_platforms)),  # Remove duplicates
                "metadata": all_metadata,
                "samples": list(set(all_samples)),  # Remove duplicates
                "platforms": list(set(all_platforms)),  # Remove duplicates
                "available_files": matrix_files,
                "file_links": [f"{base_url}{filename}" for filename in matrix_files],
                "base_url": base_url,
                "total_matrices": len(all_metadata)
            }
            
            # Add small delay to respect rate limits
            time.sleep(0.34)  # ~3 requests per second
            
            return series_matrix
            
        except urllib.error.HTTPError as e:
            raise urllib.error.HTTPError(
                base_url, e.code, f"Failed to retrieve series matrix for {gse_id}", 
                    e.hdrs, e.fp
                )
        except Exception as e:
            raise RuntimeError(f"Error processing series matrix for {gse_id}: {e}")

    def _parse_soft_format(self, content: str, record_id: str) -> Dict[str, Any]:
        """
        Parse the SOFT format response from GEO.
        
        Args:
            content (str): The SOFT format content
            record_id (str): The record ID (GSM or GSE)
            
        Returns:
            Dict containing parsed metadata
        """
        # Determine if this is a GSM or GSE record
        is_gse = record_id.upper().startswith("GSE")
        
        metadata = {
            "gsm_id" if not is_gse else "gse_id": record_id,
            "status": "retrieved",
            "attributes": {}
        }
        
        lines = content.split('\n')
        current_section = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if line.startswith('^SAMPLE = '):
                current_section = 'sample'
            elif line.startswith('^SERIES = '):
                current_section = 'series'
            elif line.startswith('!Sample_'):
                # Parse sample attributes
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.replace('!Sample_', '').strip()
                    value = value.strip()
                    metadata["attributes"][key] = value
            elif line.startswith('!Series_'):
                # Parse series attributes
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.replace('!Series_', '').strip()
                    value = value.strip()
                    metadata["attributes"][key] = value
        
        return metadata

    def _extract_abstract_from_xml(self, xml_content: str) -> str:
        """
        Extract abstract from PubMed XML content.
        
        Args:
            xml_content (str): XML content from PubMed efetch
            
        Returns:
            str: Extracted abstract text
        """
        import xml.etree.ElementTree as ET
        
        try:
            # Parse XML content
            root = ET.fromstring(xml_content)
            
            # Look for AbstractText elements
            abstract_elements = root.findall('.//AbstractText')
            
            if abstract_elements:
                # Combine all abstract text elements
                abstract_parts = []
                for elem in abstract_elements:
                    if elem.text:
                        abstract_parts.append(elem.text.strip())
                    # Also get text from child elements
                    for child in elem:
                        if child.text:
                            abstract_parts.append(child.text.strip())
                
                return ' '.join(abstract_parts)
            
            # Fallback: look for any text in Abstract section
            abstract_section = root.find('.//Abstract')
            if abstract_section is not None:
                # Get all text content from the abstract section
                abstract_text = ''.join(abstract_section.itertext()).strip()
                if abstract_text:
                    return abstract_text
            
            return ""
            
        except ET.ParseError:
            # If XML parsing fails, try to extract basic information from text
            return ""
        
def get_gsm_metadata(gsm_id: str) -> Dict[str, Any]:
    """
    Retrieve metadata for a GEO Sample (GSM) record using NCBI E-Utilities.
    
    Args:
        gsm_id (str): The GSM ID to retrieve metadata for
    
    Returns:
        Dict containing the metadata response from NCBI
        
    Raises:
        urllib.error.HTTPError: If the request fails
        ValueError: If the GSM ID is invalid
    """

    return NCBIClient().get_gsm_metadata(gsm_id)


def get_gse_metadata(gse_id: str) -> Dict[str, Any]:
    """
    Retrieve metadata for a GEO Series (GSE) record using GEO website.
    
    Args:
        gse_id (str): The GSE ID to retrieve metadata for
    
    Returns:
        Dict containing the metadata response from GEO
        
    Raises:
        urllib.error.HTTPError: If the request fails
        ValueError: If the GSE ID is invalid
    """

    return NCBIClient().get_gse_metadata(gse_id)


def get_gse_series_matrix(gse_id: str) -> Dict[str, Any]:
    """
    Retrieve the series matrix table for a GEO Series (GSE) record.
    Only extracts metadata and sample names, not the actual gene expression data.
    
    Parameters
    ----------
    gse_id : str
        The GSE ID to retrieve series matrix for
        
    Returns
    -------
    Dict[str, Any]
        Dictionary containing the series matrix metadata and sample names
        
    Raises
    ------
    urllib.error.HTTPError: If the request fails
    ValueError: If the GSE ID is invalid
    """

    return NCBIClient().get_gse_series_matrix(gse_id)


def get_paper_abstract(pmid: int) -> Dict[str, Any]:
    """
    Get paper abstract and metadata for a given PMID.
    
    Parameters
    ----------
    pmid : int
        PubMed ID for the paper.
        
    Returns
    -------
    Dict[str, Any]
        Dictionary containing paper content including abstract, title, authors, 
        journal, and other metadata.
    """

    return NCBIClient().get_paper_abstract(pmid)