import json
import time
from typing import Any, Dict
import urllib.error
import urllib.parse
import urllib.request
import os
from dotenv import load_dotenv
from pathlib import Path
import traceback

# Import field definitions for filtering
from src.models.metadata_models import (
    GSM_STANDARD_FIELDS,
    GSE_STANDARD_FIELDS,
    PMID_STANDARD_FIELDS,
)


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
        self.api_url = os.getenv(
            "NCBI_API_URL", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
        )

        # Validate required email (API key is optional)
        if not self.email or not self.api_url:
            raise ValueError(
                "NCBI_EMAIL and NCBI_API_URL environment variables are required"
            )

        # Initialize the E-Utilities client
        self.client = urllib.request.build_opener(
            urllib.request.HTTPHandler(debuglevel=0),
            urllib.request.HTTPSHandler(debuglevel=0),
        )

        # Set headers - email is required, API key is optional
        headers = [
            ("User-Agent", "Python-NCBI-E-Utilities/1.0"),
            ("Email", self.email),
        ]

        if self.api_key:
            headers.append(("API-Key", self.api_key))

        self.client.addheaders = headers

        # Ensure API URL ends with a slash
        if not self.api_url.endswith("/"):
            self.api_url += "/"

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

        max_retries = 3
        retry_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                # Make the request to GEO
                response = self.client.open(geo_url)
                content = response.read().decode("utf-8")

                # Parse the SOFT format response
                metadata = self._parse_soft_format(content, gsm_id)

                # Add small delay to respect rate limits
                time.sleep(0.34)  # ~3 requests per second

                return metadata

            except urllib.error.HTTPError as e:
                if e.code in [502, 503, 504, 429] and attempt < max_retries - 1:
                    # Retry on server errors (502, 503, 504) and rate limiting (429)
                    print(
                        f"⚠️  HTTP {e.code} error for {gsm_id}, attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    raise urllib.error.HTTPError(
                        geo_url,
                        e.code,
                        f"Failed to retrieve metadata for {gsm_id} after {attempt + 1} attempts",
                        e.hdrs,
                        e.fp,
                    )
            except Exception:
                if attempt < max_retries - 1:
                    print(
                        f"⚠️  Unexpected error for {gsm_id}, attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    traceback.print_exc()
                    raise

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

        max_retries = 3
        retry_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                # Make the request to GEO
                response = self.client.open(geo_url)
                content = response.read().decode("utf-8")

                # Parse the SOFT format response
                metadata = self._parse_soft_format(content, gse_id)
                metadata["type"] = "GSE"

                # Add small delay to respect rate limits
                time.sleep(0.34)  # ~3 requests per second

                return metadata

            except urllib.error.HTTPError as e:
                if e.code in [502, 503, 504, 429] and attempt < max_retries - 1:
                    # Retry on server errors (502, 503, 504) and rate limiting (429)
                    print(
                        f"⚠️  HTTP {e.code} error for {gse_id}, attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    raise urllib.error.HTTPError(
                        geo_url,
                        e.code,
                        f"Failed to retrieve metadata for {gse_id} after {attempt + 1} attempts",
                        e.hdrs,
                        e.fp,
                    )
            except Exception:
                if attempt < max_retries - 1:
                    print(
                        f"⚠️  Unexpected error for {gse_id}, attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    traceback.print_exc()
                    raise

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
            "pmid": pmid,
            "title": "",
            "abstract": "",
            "authors": [],
            "journal": "",
            "publication_date": "",
            "doi": "",
            "keywords": [],
            "mesh_terms": [],
        }

        max_retries = 3
        retry_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                # First search for the PMID to get the correct database ID
                search_params = {"db": "pubmed", "term": str(pmid), "retmode": "json"}

                search_url = f"{self.api_url}esearch.fcgi?{urllib.parse.urlencode(search_params)}"

                # Use the configured client with proper headers
                search_response = self.client.open(search_url)
                search_content = search_response.read().decode("utf-8")

                if search_content:
                    search_data = json.loads(search_content)
                    id_list = search_data.get("esearchresult", {}).get("idlist", [])

                    if id_list:
                        # Use the first ID from search results
                        db_id = id_list[0]

                        # Get metadata using esummary
                        summary_params = {
                            "db": "pubmed",
                            "id": db_id,
                            "retmode": "json",
                        }

                        summary_url = f"{self.api_url}esummary.fcgi?{urllib.parse.urlencode(summary_params)}"

                        # Use the configured client with proper headers
                        summary_response = self.client.open(summary_url)
                        summary_content = summary_response.read().decode("utf-8")

                        if summary_content:
                            try:
                                json_data = json.loads(summary_content)
                                paper_info.update(
                                    self._parse_pubmed_json(json_data, pmid)
                                )
                            except json.JSONDecodeError:
                                # Fallback to XML if JSON fails
                                paper_info.update(
                                    self._parse_pubmed_xml(summary_content)
                                )

                        # Get full paper content using efetch to extract abstract
                        fetch_params = {
                            "db": "pubmed",
                            "id": db_id,
                            "retmode": "xml",
                            "rettype": "abstract",
                        }

                        fetch_url = f"{self.api_url}efetch.fcgi?{urllib.parse.urlencode(fetch_params)}"

                        # Use the configured client with proper headers
                        fetch_response = self.client.open(fetch_url)
                        fetch_content = fetch_response.read().decode("utf-8")

                        if fetch_content:
                            # Parse the XML to extract the abstract
                            abstract = self._extract_abstract_from_xml(fetch_content)
                            if abstract:
                                paper_info["abstract"] = abstract

                # Add small delay to respect rate limits
                time.sleep(0.34)  # ~3 requests per second

                # Filter to only include standard PMID fields
                paper_info = self._filter_standard_fields(paper_info, "PMID")

                return paper_info

            except urllib.error.HTTPError as e:
                if e.code in [502, 503, 504, 429] and attempt < max_retries - 1:
                    # Retry on server errors (502, 503, 504) and rate limiting (429)
                    print(
                        f"⚠️  HTTP {e.code} error for PMID {pmid}, attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    raise urllib.error.HTTPError(
                        search_url,
                        e.code,
                        f"Failed to retrieve metadata for PMID {pmid} after {attempt + 1} attempts",
                        e.hdrs,
                        e.fp,
                    )
            except Exception:
                if attempt < max_retries - 1:
                    print(
                        f"⚠️  Unexpected error for PMID {pmid}, attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    traceback.print_exc()
                    raise

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
            title_elem = root.find(".//ArticleTitle")
            if title_elem is not None:
                paper_info["title"] = title_elem.text or ""

            # Extract abstract
            abstract_elem = root.find(".//AbstractText")
            if abstract_elem is not None:
                paper_info["abstract"] = abstract_elem.text or ""

            # Extract authors
            authors = []
            for author_elem in root.findall(".//Author"):
                last_name = author_elem.find("LastName")
                first_name = author_elem.find("ForeName")
                if last_name is not None and first_name is not None:
                    authors.append(f"{first_name.text} {last_name.text}")
            paper_info["authors"] = authors

            # Extract journal
            journal_elem = root.find(".//Journal/Title")
            if journal_elem is not None:
                paper_info["journal"] = journal_elem.text or ""

            # Extract publication date
            pub_date_elem = root.find(".//PubDate")
            if pub_date_elem is not None:
                year_elem = pub_date_elem.find("Year")
                month_elem = pub_date_elem.find("Month")
                if year_elem is not None:
                    date_parts = [year_elem.text]
                    if month_elem is not None:
                        date_parts.append(month_elem.text)
                    paper_info["publication_date"] = " ".join(date_parts)

            # Extract DOI
            doi_elem = root.find('.//ELocationID[@EIdType="doi"]')
            if doi_elem is not None:
                paper_info["doi"] = doi_elem.text or ""

            # Extract keywords
            keywords = []
            for keyword_elem in root.findall(".//Keyword"):
                if keyword_elem.text:
                    keywords.append(keyword_elem.text)
            paper_info["keywords"] = keywords

            # Extract MeSH terms
            mesh_terms = []
            for mesh_elem in root.findall(".//MeshHeading/DescriptorName"):
                if mesh_elem.text:
                    mesh_terms.append(mesh_elem.text)
            paper_info["mesh_terms"] = mesh_terms

        except ET.ParseError:
            # If XML parsing fails, try to extract basic information from text
            paper_info["abstract"] = xml_content[:1000] if xml_content else ""

        return paper_info

    def _parse_pubmed_json(
        self, json_data: Dict[str, Any], pmid: int
    ) -> Dict[str, Any]:
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
            result = json_data.get("result", {})
            if str(pmid) in result:
                article_data = result[str(pmid)]

                # Extract title
                paper_info["title"] = article_data.get("title", "")

                # Extract abstract
                paper_info["abstract"] = article_data.get("abstract", "")

                # Extract authors
                authors = article_data.get("authors", [])
                if isinstance(authors, list):
                    paper_info["authors"] = [
                        author.get("name", "")
                        for author in authors
                        if author.get("name")
                    ]
                else:
                    paper_info["authors"] = []

                # Extract journal
                paper_info["journal"] = article_data.get("fulljournalname", "")

                # Extract publication date
                pubdate = article_data.get("pubdate", "")
                paper_info["publication_date"] = pubdate

                # Extract DOI
                paper_info["doi"] = article_data.get("elocationid", "")

                # Extract keywords and MeSH terms
                paper_info["keywords"] = article_data.get("keywords", [])
                paper_info["mesh_terms"] = article_data.get("mesh", [])

        except Exception:
            # If JSON parsing fails, return empty data
            pass

        return paper_info

    def _get_file_size(self, url: str) -> Dict[str, Any]:
        """
        Get file size information for a given URL using HEAD request.

        Args:
            url (str): The URL to check

        Returns:
            Dict containing file size information
        """
        try:
            # Create a request with HEAD method
            req = urllib.request.Request(url, method="HEAD")
            response = urllib.request.urlopen(req)

            # Get content length from headers
            content_length = response.headers.get("Content-Length")
            file_size_bytes = int(content_length) if content_length else None

            # Convert to human readable format
            if file_size_bytes:
                if file_size_bytes < 1024:
                    file_size_human = f"{file_size_bytes} B"
                elif file_size_bytes < 1024 * 1024:
                    file_size_human = f"{file_size_bytes / 1024:.1f} KB"
                elif file_size_bytes < 1024 * 1024 * 1024:
                    file_size_human = f"{file_size_bytes / (1024 * 1024):.1f} MB"
                else:
                    file_size_human = f"{file_size_bytes / (1024 * 1024 * 1024):.1f} GB"
            else:
                file_size_human = "Unknown"

            return {
                "file_size_bytes": file_size_bytes,
                "file_size_human": file_size_human,
                "status": "success",
            }

        except Exception as e:
            traceback.print_exc()
            raise
            return {
                "file_size_bytes": None,
                "file_size_human": "Unknown",
                "status": "error",
                "error": str(e),
            }

    def get_gse_series_matrix(self, gse_id: str) -> Dict[str, Any]:
        """
        Retrieve the series matrix table for a GEO Series (GSE) record.
        Only extracts metadata and sample names, not the actual gene expression data.
        Includes file size information for each matrix file.

        Args:
            gse_id (str): The GSE ID to retrieve series matrix for

        Returns:
            Dict containing the series matrix metadata, sample names, and file sizes

        Raises:
            urllib.error.HTTPError: If the request fails
            ValueError: If the GSE ID is invalid
        """
        # Validate GSE ID format
        if not gse_id.upper().startswith("GSE") or not gse_id[3:].isdigit():
            raise ValueError(f"Invalid GSE ID format: {gse_id}")

        max_retries = 3
        retry_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                # GEO stores series folders in batches of 1,000: e.g. GSE123 → GSE123nnn
                prefix = gse_id[:-3] + "nnn"
                base_url = (
                    f"https://ftp.ncbi.nlm.nih.gov/geo/series/{prefix}/{gse_id}/matrix/"
                )

                # First, try to get the directory listing to find available matrix files
                try:
                    # Try to get directory listing
                    dir_response = urllib.request.urlopen(base_url)
                    dir_content = dir_response.read().decode("utf-8")

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
                file_info = []

                for matrix_file in matrix_files:
                    url = base_url + matrix_file

                    # Get file size information
                    file_size_info = self._get_file_size(url)
                    file_info.append(
                        {
                            "filename": matrix_file,
                            "url": url,
                            "file_size_bytes": file_size_info["file_size_bytes"],
                            "file_size_human": file_size_info["file_size_human"],
                            "size_status": file_size_info["status"],
                        }
                    )

                    try:
                        # Download the gzipped matrix
                        response = urllib.request.urlopen(url)
                        gzipped_content = response.read()

                        # Decompress and read the content
                        import gzip
                        from io import BytesIO

                        # Decompress the gzipped content
                        with gzip.open(BytesIO(gzipped_content), "rt") as f:
                            content = f.read()

                        # Parse the content to extract metadata and sample names only
                        lines = content.split("\n")
                        metadata = {}
                        sample_names = []
                        in_matrix_section = False
                        found_sample_row = False

                        for line in lines:
                            line = line.strip()
                            if line.startswith("!"):
                                # Parse metadata lines
                                if "=" in line:
                                    key, value = line.split("=", 1)
                                    key = key.replace("!", "").strip()
                                    value = value.strip()
                                    metadata[key] = value
                            elif line.startswith("!series_matrix_table_begin"):
                                # Mark the beginning of the matrix section
                                in_matrix_section = True
                            elif (
                                in_matrix_section
                                and line
                                and not line.startswith("^")
                                and not found_sample_row
                            ):
                                # This is the first row after the table begin marker - sample names
                                sample_names = line.split("\t")
                                found_sample_row = True
                                # Stop processing after getting sample names
                                break

                        # Extract platform ID from filename
                        platform_id = matrix_file.replace(f"{gse_id}-", "").replace(
                            "_series_matrix.txt.gz", ""
                        )

                        # Store metadata for this platform
                        all_metadata[platform_id] = metadata

                        # Extract sample and platform information from metadata
                        for key, value in metadata.items():
                            if "sample_geo_accession" in key.lower():
                                all_samples.append(value)
                            elif "platform_geo_accession" in key.lower():
                                all_platforms.append(value)

                        # Add sample names from the matrix header
                        if sample_names:
                            all_samples.extend(
                                sample_names[1:]
                            )  # Skip the first column (probe IDs)

                    except Exception as e:
                        traceback.print_exc()
                        raise
                        print(
                            f"Warning: Could not process matrix file {matrix_file}: {e}"
                        )
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
                    "file_links": [
                        f"{base_url}{filename}" for filename in matrix_files
                    ],
                    "file_info": file_info,  # New field with file size information
                    "base_url": base_url,
                    "total_matrices": len(all_metadata),
                }

                # Add small delay to respect rate limits
                time.sleep(0.34)  # ~3 requests per second

                return series_matrix

            except urllib.error.HTTPError as e:
                if e.code in [502, 503, 504, 429] and attempt < max_retries - 1:
                    # Retry on server errors (502, 503, 504) and rate limiting (429)
                    print(
                        f"⚠️  HTTP {e.code} error for series matrix {gse_id}, attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    raise urllib.error.HTTPError(
                        base_url,
                        e.code,
                        f"Failed to retrieve series matrix for {gse_id} after {attempt + 1} attempts",
                        e.hdrs,
                        e.fp,
                    )
            except Exception as e:
                if attempt < max_retries - 1:
                    print(
                        f"⚠️  Unexpected error for series matrix {gse_id}, attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    traceback.print_exc()
                    raise RuntimeError(
                        f"Error processing series matrix for {gse_id} after {attempt + 1} attempts: {e}"
                    )

    def _filter_standard_fields(
        self, attributes: Dict[str, Any], record_type: str
    ) -> Dict[str, Any]:
        """
        Filter attributes to only include standard fields defined in the metadata models.

        Args:
            attributes (Dict[str, Any]): Raw attributes from NCBI
            record_type (str): Type of record ('GSM', 'GSE', or 'PMID')

        Returns:
            Dict[str, Any]: Filtered attributes containing only standard fields
        """
        # Get the appropriate standard fields set
        if record_type == "GSM":
            standard_fields = GSM_STANDARD_FIELDS
        elif record_type == "GSE":
            standard_fields = GSE_STANDARD_FIELDS
        elif record_type == "PMID":
            standard_fields = PMID_STANDARD_FIELDS
        else:
            # If unknown type, pass through all fields (shouldn't happen)
            return attributes

        # Filter to only include standard fields
        filtered_attributes = {}
        filtered_count = 0

        for key, value in attributes.items():
            if key in standard_fields:
                filtered_attributes[key] = value
            else:
                filtered_count += 1

        # Log filtering results if fields were filtered
        if filtered_count > 0:
            print(
                f"🔧 Filtered {filtered_count} non-standard fields from {record_type} attributes"
            )

        return filtered_attributes

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
            "attributes": {},
        }

        lines = content.split("\n")
        current_section = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.startswith("^SAMPLE = "):
                current_section = "sample"
            elif line.startswith("^SERIES = "):
                current_section = "series"
            elif line.startswith("!Sample_"):
                # Parse sample attributes
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.replace("!Sample_", "").strip()
                    value = value.strip()

                    # Handle multiple values for the same key (e.g., characteristics_ch1)
                    if key in metadata["attributes"]:
                        # If the key already exists, concatenate the values with commas
                        existing_value = metadata["attributes"][key]
                        metadata["attributes"][key] = f"{existing_value}, {value}"
                    else:
                        metadata["attributes"][key] = value
            elif line.startswith("!Series_"):
                # Parse series attributes
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.replace("!Series_", "").strip()
                    value = value.strip()

                    # Handle multiple values for the same key (e.g., characteristics_ch1)
                    if key in metadata["attributes"]:
                        # If the key already exists, concatenate the values with commas
                        existing_value = metadata["attributes"][key]
                        metadata["attributes"][key] = f"{existing_value}, {value}"
                    else:
                        metadata["attributes"][key] = value

        # Filter attributes to only include standard fields
        record_type = "GSE" if is_gse else "GSM"
        metadata["attributes"] = self._filter_standard_fields(
            metadata["attributes"], record_type
        )

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
            abstract_elements = root.findall(".//AbstractText")

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

                return " ".join(abstract_parts)

            # Fallback: look for any text in Abstract section
            abstract_section = root.find(".//Abstract")
            if abstract_section is not None:
                # Get all text content from the abstract section
                abstract_text = "".join(abstract_section.itertext()).strip()
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


def get_paper_abstract(
    pmid: int, email: str = None, api_key: str = None
) -> Dict[str, Any]:
    """
    Get paper abstract and metadata for a given PMID.

    Parameters
    ----------
    pmid : int
        PubMed ID for the paper.
    email : str, optional
        Email address for NCBI E-Utils identification.
    api_key : str, optional
        NCBI API key for higher rate limits.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing paper content including abstract, title, authors,
        journal, and other metadata.
    """

    # Create client with provided credentials or use environment variables
    client = NCBIClient()
    if email:
        client.email = email
    if api_key:
        client.api_key = api_key

    return client.get_paper_abstract(pmid)


def extract_pubmed_id_from_gse_metadata(gse_metadata_file: str) -> Dict[str, Any]:
    """
    Extract PubMed ID from a GSE metadata JSON file.

    This function reads a GSE metadata file (produced by extract_gse_metadata tool)
    and extracts the PubMed ID from the "pubmed_id" field under attributes.

    Parameters
    ----------
    gse_metadata_file : str
        Path to the GSE metadata JSON file

    Returns
    -------
    Dict[str, Any]
        Dictionary containing the extracted PubMed ID and status information

    Raises
    ------
    FileNotFoundError: If the metadata file doesn't exist
    ValueError: If the file is not valid JSON or doesn't contain expected structure
    KeyError: If pubmed_id is not found in the metadata
    """
    try:
        # Read the metadata file
        with open(gse_metadata_file, "r") as f:
            metadata = json.load(f)

        # Extract PubMed ID from attributes
        attributes = metadata.get("attributes", {})
        pubmed_id = attributes.get("pubmed_id")

        if not pubmed_id:
            # Try alternative field names
            pubmed_id = (
                attributes.get("pmid")
                or attributes.get("PubMed ID")
                or attributes.get("pubmed")
            )

        if not pubmed_id:
            # Return a result indicating no PubMed ID was found, but don't fail the pipeline
            result = {
                "status": "warning",
                "gse_metadata_file": gse_metadata_file,
                "pubmed_id": None,
                "pubmed_id_original": None,
                "pubmed_ids": [],
                "pubmed_id_count": 0,
                "message": f"No PubMed ID found in GSE metadata file: {gse_metadata_file}. This is normal for some datasets.",
            }
            return result

        # Handle multiple PubMed IDs (separated by commas or newlines)
        # Split by both commas and newlines to handle different formats
        pubmed_ids = []
        for separator in [",", "\n"]:
            if separator in pubmed_id:
                pubmed_ids = [
                    pid.strip() for pid in pubmed_id.split(separator) if pid.strip()
                ]
                break
        if not pubmed_ids:
            # If no separators found, treat as single value
            pubmed_ids = [pubmed_id.strip()] if pubmed_id.strip() else []

        # Validate and convert all PubMed IDs to integers
        pubmed_id_ints = []
        for pid in pubmed_ids:
            try:
                pubmed_id_ints.append(int(pid))
            except (ValueError, TypeError):
                raise ValueError(f"Invalid PubMed ID format: {pid}")

        # Use the first PubMed ID as the primary one, but return all
        primary_pubmed_id = pubmed_id_ints[0] if pubmed_id_ints else None

        result = {
            "status": "success",
            "gse_metadata_file": gse_metadata_file,
            "pubmed_id": primary_pubmed_id,
            "pubmed_id_original": pubmed_id,
            "pubmed_ids": pubmed_id_ints,  # All PubMed IDs
            "pubmed_id_count": len(pubmed_id_ints),
            "message": f"Successfully extracted {len(pubmed_id_ints)} PubMed ID(s) from {gse_metadata_file}. Primary: {primary_pubmed_id}",
        }

        return result

    except FileNotFoundError:
        raise FileNotFoundError(f"GSE metadata file not found: {gse_metadata_file}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in GSE metadata file {gse_metadata_file}: {e}")
    except Exception as e:
        traceback.print_exc()
        raise RuntimeError(f"Error extracting PubMed ID from {gse_metadata_file}: {e}")


def extract_series_id_from_gsm_metadata(gsm_metadata_file: str) -> Dict[str, Any]:
    """
    Extract Series ID from a GSM metadata JSON file.

    This function reads a GSM metadata file (produced by extract_gsm_metadata tool)
    and extracts the Series ID from the "series_id" field under attributes.

    Parameters
    ----------
    gsm_metadata_file : str
        Path to the GSM metadata JSON file

    Returns
    -------
    Dict[str, Any]
        Dictionary containing the extracted Series ID and status information

    Raises
    ------
    FileNotFoundError: If the metadata file doesn't exist
    ValueError: If the file is not valid JSON or doesn't contain expected structure
    KeyError: If series_id is not found in the metadata
    """
    try:
        # Read the metadata file
        with open(gsm_metadata_file, "r") as f:
            metadata = json.load(f)

        # Extract Series ID from attributes
        attributes = metadata.get("attributes", {})
        series_id = attributes.get("series_id")

        if not series_id:
            # Try alternative field names
            series_id = (
                attributes.get("gse_id")
                or attributes.get("Series ID")
                or attributes.get("series")
            )

        if not series_id:
            raise KeyError(
                f"Series ID not found in GSM metadata file: {gsm_metadata_file}"
            )

        # Handle multiple series IDs (separated by commas)
        series_ids = [sid.strip() for sid in series_id.split(",") if sid.strip()]

        # Validate all series IDs
        valid_series_ids = []
        for sid in series_ids:
            if sid.upper().startswith("GSE") and sid[3:].isdigit():
                valid_series_ids.append(sid.upper())
            else:
                raise ValueError(f"Invalid Series ID format: {sid}")

        # Use the first series ID as the primary one
        primary_series_id = valid_series_ids[0] if valid_series_ids else None

        result = {
            "status": "success",
            "gsm_metadata_file": gsm_metadata_file,
            "series_id": primary_series_id,  # Primary (first) series ID
            "series_id_original": series_id,
            "series_ids": valid_series_ids,  # All valid series IDs
            "series_id_count": len(valid_series_ids),
            "message": f"Successfully extracted {len(valid_series_ids)} Series ID(s) from {gsm_metadata_file}. Primary: {primary_series_id}",
        }

        return result

    except FileNotFoundError:
        raise FileNotFoundError(f"GSM metadata file not found: {gsm_metadata_file}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in GSM metadata file {gsm_metadata_file}: {e}")
    except Exception as e:
        traceback.print_exc()
        raise RuntimeError(f"Error extracting Series ID from {gsm_metadata_file}: {e}")


def _get_series_subdirectory(session_dir: str, series_id: str) -> Path:
    """
    Get or create a subdirectory for a specific series ID within the session directory.

    Parameters
    ----------
    session_dir : str
        The session directory path
    series_id : str
        The series ID (e.g., "GSE41588")

    Returns
    -------
    Path
        Path to the series subdirectory
    """
    series_dir = Path(session_dir) / series_id
    series_dir.mkdir(parents=True, exist_ok=True)
    return series_dir


def extract_gsm_metadata_impl(
    gsm_id: str, session_dir: str, email: str = None, api_key: str = None
) -> str:
    """
    Extract metadata for a GEO Sample (GSM) record.

    Parameters
    ----------
    gsm_id : str
        Gene Expression Omnibus sample ID (e.g., "GSM1019742").
    session_dir : str
        Session directory to save the metadata file.
    email : str, optional
        Email address for NCBI E-Utils identification.
    api_key : str, optional
        NCBI API key for higher rate limits.

    Returns
    -------
    str
        Path to the saved metadata file.
    """

    # Validate GSM ID format
    if not gsm_id.upper().startswith("GSM") or not gsm_id[3:].isdigit():
        raise ValueError(f"Invalid GSM ID format: {gsm_id}")

    # Extract metadata
    metadata = get_gsm_metadata(gsm_id)

    # Extract series ID from metadata
    attributes = metadata.get("attributes", {})
    series_id = (
        attributes.get("series_id")
        or attributes.get("gse_id")
        or attributes.get("Series ID")
    )

    if not series_id:
        # If no series ID found, save to session root with a warning
        print(f"⚠️  No series ID found for {gsm_id}, saving to session root")
        output_file = Path(session_dir) / f"{gsm_id}_metadata.json"
    else:
        # Handle multiple series IDs (separated by commas)
        series_ids = [sid.strip() for sid in series_id.split(",") if sid.strip()]

        # Validate all series IDs
        valid_series_ids = []
        for sid in series_ids:
            if sid.upper().startswith("GSE") and sid[3:].isdigit():
                valid_series_ids.append(sid.upper())
            else:
                print(f"⚠️  Invalid series ID format '{sid}' for {gsm_id}")

        if not valid_series_ids:
            # If no valid series IDs found, save to session root
            print(f"⚠️  No valid series IDs found for {gsm_id}, saving to session root")
            output_file = Path(session_dir) / f"{gsm_id}_metadata.json"
        else:
            # Use the first valid series ID for directory organization
            primary_series_id = valid_series_ids[0]
            series_dir = _get_series_subdirectory(session_dir, primary_series_id)
            output_file = series_dir / f"{gsm_id}_metadata.json"
            # If there are multiple series IDs, add a note to the metadata
            if len(valid_series_ids) > 1:
                metadata["attributes"]["all_series_ids"] = ", ".join(valid_series_ids)
                print(
                    f"📝 Sample {gsm_id} belongs to multiple series: {', '.join(valid_series_ids)}"
                )

    # Save metadata
    with open(output_file, "w") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    return str(output_file)


def extract_gse_metadata_impl(
    gse_id: str, session_dir: str, email: str = None, api_key: str = None
) -> str:
    """
    Extract metadata for a GEO Series (GSE) record.

    Parameters
    ----------
    gse_id : str
        Gene Expression Omnibus series ID (e.g., "GSE41588").
    session_dir : str
        Session directory to save the metadata file.
    email : str, optional
        Email address for NCBI E-Utils identification.
    api_key : str, optional
        NCBI API key for higher rate limits.

    Returns
    -------
    str
        Path to the saved metadata file.
    """

    # Validate GSE ID format
    if not gse_id.upper().startswith("GSE") or not gse_id[3:].isdigit():
        raise ValueError(f"Invalid GSE ID format: {gse_id}")

    # Extract metadata
    metadata = get_gse_metadata(gse_id)

    # Create series subdirectory and save there
    series_dir = _get_series_subdirectory(session_dir, gse_id.upper())
    output_file = series_dir / f"{gse_id}_metadata.json"

    # Save metadata
    with open(output_file, "w") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    return str(output_file)


def extract_series_matrix_metadata_impl(
    gse_id: str, session_dir: str, email: str = None, api_key: str = None
) -> str:
    """
    Extract series matrix metadata and sample names for a GEO Series (GSE) record.

    Parameters
    ----------
    gse_id : str
        Gene Expression Omnibus series ID (e.g., "GSE41588").
    session_dir : str
        Session directory to save the metadata file.
    email : str, optional
        Email address for NCBI E-Utils identification.
    api_key : str, optional
        NCBI API key for higher rate limits.

    Returns
    -------
    str
        Path to the saved metadata file.
    """

    # Validate GSE ID format
    if not gse_id.upper().startswith("GSE") or not gse_id[3:].isdigit():
        raise ValueError(f"Invalid GSE ID format: {gse_id}")

    # Extract series matrix metadata
    metadata = get_gse_series_matrix(gse_id)

    # Create series subdirectory and save there
    series_dir = _get_series_subdirectory(session_dir, gse_id.upper())
    output_file = series_dir / f"{gse_id}_series_matrix.json"

    # Save metadata
    with open(output_file, "w") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    # Print file size information if available
    if metadata.get("file_info"):
        total_size = 0
        for file_info in metadata["file_info"]:
            if file_info.get("file_size_bytes"):
                total_size += file_info["file_size_bytes"]

        if total_size > 0:
            if total_size < 1024 * 1024:
                total_size_str = f"{total_size / 1024:.1f} KB"
            elif total_size < 1024 * 1024 * 1024:
                total_size_str = f"{total_size / (1024 * 1024):.1f} MB"
            else:
                total_size_str = f"{total_size / (1024 * 1024 * 1024):.1f} GB"

        else:
            pass
    else:
        pass

    return str(output_file)


def extract_paper_abstract_impl(
    pmid: int,
    session_dir: str,
    email: str = None,
    api_key: str = None,
    source_gse_file: str = None,
) -> str:
    """
    Extract paper abstract and metadata for a given PMID.

    Parameters
    ----------
    pmid : int
        PubMed ID for the paper.
    session_dir : str
        Session directory to save the metadata file.
    email : str, optional
        Email address for NCBI E-Utils identification.
    api_key : str, optional
        NCBI API key for higher rate limits.
    source_gse_file : str, optional
        Path to the GSE metadata file that this PMID was extracted from.
        Used to determine the correct series directory.

    Returns
    -------
    str
        Path to the saved metadata file.
    """

    # Validate PMID format
    if not isinstance(pmid, int) or pmid <= 0:
        raise ValueError(f"Invalid PMID format: {pmid}")

    # Extract paper metadata
    metadata = get_paper_abstract(pmid, email, api_key)

    # Determine the correct series directory based on source GSE file
    session_path = Path(session_dir)
    series_id = None

    if source_gse_file:
        # Extract series ID from the source GSE file path
        source_path = Path(source_gse_file)
        if source_path.name.startswith("GSE") and "_metadata.json" in source_path.name:
            series_id = source_path.name.replace("_metadata.json", "")
            series_dir = _get_series_subdirectory(session_dir, series_id)
            output_file = series_dir / f"PMID_{pmid}_metadata.json"

        else:
            # Fallback to session root if source file format is unexpected
            output_file = session_path / f"PMID_{pmid}_metadata.json"

    else:
        # Check if there are any GSE directories in the session
        gse_dirs = [
            d for d in session_path.iterdir() if d.is_dir() and d.name.startswith("GSE")
        ]

        if gse_dirs:
            # If we have GSE directories, we could potentially match the PMID to a series
            # For now, save to the first GSE directory (this could be enhanced)
            series_dir = gse_dirs[0]
            series_id = series_dir.name
            output_file = series_dir / f"PMID_{pmid}_metadata.json"

        else:
            # No GSE directories found, save to session root
            output_file = session_path / f"PMID_{pmid}_metadata.json"

    # Add series_id to the metadata if available
    if series_id:
        metadata["series_id"] = series_id
        metadata["source_gse_file"] = source_gse_file if source_gse_file else None

    # Save metadata
    with open(output_file, "w") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    return str(output_file)


def extract_pubmed_id_from_gse_metadata_impl(
    gse_metadata_file: str, session_dir: str
) -> str:
    """
    Extract PubMed ID from a GSE metadata JSON file.

    Parameters
    ----------
    gse_metadata_file : str
        Path to the GSE metadata JSON file (e.g., "GSE41588_metadata.json")
    session_dir : str
        Session directory for resolving relative paths.

    Returns
    -------
    str
        JSON string containing the extracted PubMed ID and status information.
    """

    # The gse_metadata_file should already be a full path from extract_gse_metadata_impl
    # Just verify the file exists
    if not os.path.exists(gse_metadata_file):
        # If it doesn't exist, try constructing the path relative to session_dir
        if not os.path.isabs(gse_metadata_file):
            gse_metadata_file = os.path.join(session_dir, gse_metadata_file)

        # Check again
        if not os.path.exists(gse_metadata_file):
            raise FileNotFoundError(f"GSE metadata file not found: {gse_metadata_file}")

    # Extract PubMed ID
    result = extract_pubmed_id_from_gse_metadata(gse_metadata_file)

    return json.dumps(result, indent=2, ensure_ascii=False)


def extract_series_id_from_gsm_metadata_impl(
    gsm_metadata_file: str, session_dir: str
) -> str:
    """
    Extract Series ID from a GSM metadata JSON file.

    Parameters
    ----------
    gsm_metadata_file : str
        Path to the GSM metadata JSON file (e.g., "GSM1019742_metadata.json")
    session_dir : str
        Session directory for resolving relative paths.

    Returns
    -------
    str
        JSON string containing the extracted Series ID and status information.
    """

    # The gsm_metadata_file should already be a full path from extract_gsm_metadata_impl
    # Just verify the file exists
    if not os.path.exists(gsm_metadata_file):
        # If it doesn't exist, try constructing the path relative to session_dir
        if not os.path.isabs(gsm_metadata_file):
            gsm_metadata_file = os.path.join(session_dir, gsm_metadata_file)

        # Check again
        if not os.path.exists(gsm_metadata_file):
            raise FileNotFoundError(f"GSM metadata file not found: {gsm_metadata_file}")

    # Extract Series ID
    result = extract_series_id_from_gsm_metadata(gsm_metadata_file)

    return json.dumps(result, indent=2, ensure_ascii=False)


def create_series_sample_mapping_impl(session_dir: str) -> str:
    """
    Create a mapping file between series IDs and sample IDs in the main session directory.
    This file will be used by later agents to determine which subdirectory contains data for a given sample ID.

    Parameters
    ----------
    session_dir : str
        Session directory to scan for series subdirectories and create the mapping file.

    Returns
    -------
    str
        Path to the created mapping file.
    """

    session_path = Path(session_dir)
    mapping = {}

    # Scan for series subdirectories (GSE*)
    series_dirs = [
        d for d in session_path.iterdir() if d.is_dir() and d.name.startswith("GSE")
    ]

    if not series_dirs:
        # Create empty mapping file
        mapping_file = session_path / "series_sample_mapping.json"
        with open(mapping_file, "w") as f:
            json.dump(
                {"mapping": {}, "total_series": 0, "total_samples": 0}, f, indent=2
            )

        return str(mapping_file)

    # Process each series directory
    for series_dir in series_dirs:
        series_id = series_dir.name
        sample_ids = []

        # Look for series matrix files that contain sample information
        series_matrix_files = list(series_dir.glob("*_series_matrix.json"))

        for matrix_file in series_matrix_files:
            try:
                with open(matrix_file, "r") as f:
                    matrix_data = json.load(f)

                # Extract sample IDs from the series matrix data
                if "samples" in matrix_data:
                    sample_ids.extend(matrix_data["samples"])

                # Also check for sample IDs in metadata
                if "metadata" in matrix_data:
                    for platform_id, platform_data in matrix_data["metadata"].items():
                        for key, value in platform_data.items():
                            if "sample_geo_accession" in key.lower() and value:
                                if isinstance(value, list):
                                    sample_ids.extend(value)
                                else:
                                    sample_ids.append(value)

            except Exception as e:
                traceback.print_exc()
                raise
                print(f"⚠️  Error processing {matrix_file}: {e}")
                continue

        # Also look for individual GSM metadata files
        gsm_files = list(series_dir.glob("GSM*_metadata.json"))
        for gsm_file in gsm_files:
            try:
                with open(gsm_file, "r") as f:
                    gsm_data = json.load(f)

                # Extract GSM ID from filename or metadata
                gsm_id = gsm_file.name.replace("_metadata.json", "")
                if gsm_id.startswith("GSM"):
                    sample_ids.append(gsm_id)

            except Exception as e:
                traceback.print_exc()
                raise
                print(f"⚠️  Error processing {gsm_file}: {e}")
                continue

        # Remove duplicates and sort
        sample_ids = sorted(list(set(sample_ids)))

        if sample_ids:
            # Store just the list of sample IDs to match Pydantic model expectation
            mapping[series_id] = sample_ids
        else:
            pass

    # Create reverse mapping (sample_id -> series_id) for quick lookup
    reverse_mapping = {}
    for series_id, sample_ids in mapping.items():
        for sample_id in sample_ids:
            reverse_mapping[sample_id] = series_id

    # Calculate totals
    total_series = len(mapping)
    total_samples = sum(len(sample_ids) for sample_ids in mapping.values())

    # Create the mapping file
    mapping_data = {
        "mapping": mapping,
        "reverse_mapping": reverse_mapping,
        "total_series": total_series,
        "total_samples": total_samples,
        "generated_at": str(Path().cwd()),
        "session_directory": str(session_path.absolute()),
    }

    mapping_file = session_path / "series_sample_mapping.json"
    with open(mapping_file, "w") as f:
        json.dump(mapping_data, f, indent=2, ensure_ascii=False)

    return str(mapping_file)


def validate_geo_inputs_impl(
    gsm_id: str = None,
    gse_id: str = None,
    pmid: int = None,
    email: str = None,
    api_key: str = None,
) -> str:
    """
    Validate input parameters for GEO metadata extraction.

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
    api_key : str, optional
        NCBI API key for higher rate limits.

    Returns
    -------
    str
        JSON string containing validation results.
    """

    result = {"validation_status": "success", "validated_inputs": {}, "errors": []}

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
