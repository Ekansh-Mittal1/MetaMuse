"""
Enhanced full-text retrieval for PubMed articles.

This module provides improved full-text retrieval capabilities by:
1. Better PMC XML parsing with section structure
2. ArXiv integration for preprints
3. Crossref API for publisher links
4. Structured text extraction
"""

from __future__ import annotations

import re
import time
import requests
from typing import Optional, Dict, List, Tuple
from urllib.parse import quote
from Bio import Entrez
from bs4 import BeautifulSoup, Tag


class EnhancedFullTextRetriever:
    """Enhanced full-text retrieval with multiple sources and better parsing."""
    
    def __init__(self, email: str = "research@dendroforge.ai", api_key: Optional[str] = None):
        """
        Initialize the enhanced full-text retriever.
        
        Parameters
        ----------
        email : str
            Email for NCBI API requests
        api_key : str, optional
            NCBI API key for higher rate limits
        """
        self.email = email
        self.api_key = api_key
        Entrez.email = email
        if api_key:
            Entrez.api_key = api_key
        
        # Rate limiting
        self.rate_limit = 0.1 if api_key else 0.34
        self.last_request_time = 0
        
        # Session for HTTP requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DendroForge-PubMed-Tool/1.0 (research@dendroforge.ai)',
        })
    
    def _rate_limit_wait(self) -> None:
        """Ensure we don't exceed rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.rate_limit:
            time.sleep(self.rate_limit - time_since_last)
        self.last_request_time = time.time()
    
    def get_full_text(self, pmid: str, pmcid: str = "", doi: str = "", 
                     title: str = "", authors: List[str] = None) -> Optional[Dict[str, str]]:
        """
        Retrieve full text from multiple sources.
        
        Parameters
        ----------
        pmid : str
            PubMed ID
        pmcid : str, optional
            PMC ID if available
        doi : str, optional
            DOI if available
        title : str, optional
            Paper title for arXiv search
        authors : List[str], optional
            Authors for arXiv search
            
        Returns
        -------
        Optional[Dict[str, str]]
            Dictionary with full text sections and metadata, or None if not available
        """
        # Try PMC first (highest quality structured content)
        if pmcid:
            pmc_result = self._get_pmc_structured_text(pmcid)
            if pmc_result:
                return pmc_result
        
        # Try arXiv for preprints (common in bioinformatics/ML)
        if title and authors:
            arxiv_result = self._get_arxiv_text(title, authors)
            if arxiv_result:
                return arxiv_result
        
        # Try Crossref for publisher full-text links
        if doi:
            crossref_result = self._get_crossref_fulltext(doi)
            if crossref_result:
                return crossref_result
        
        return None
    
    def _get_pmc_structured_text(self, pmcid: str) -> Optional[Dict[str, str]]:
        """
        Retrieve and parse PMC full text with proper structure.
        
        Parameters
        ----------
        pmcid : str
            PMC identifier
            
        Returns
        -------
        Optional[Dict[str, str]]
            Structured text with sections, or None if not available
        """
        if not pmcid:
            return None
        
        self._rate_limit_wait()
        
        try:
            # Clean PMC ID
            clean_pmcid = pmcid.replace("PMC", "")
            
            handle = Entrez.efetch(
                db="pmc",
                id=clean_pmcid,
                rettype="full",
                retmode="xml"
            )
            
            xml_content = handle.read()
            handle.close()
            
            # Parse XML with BeautifulSoup
            soup = BeautifulSoup(xml_content, 'xml')
            
            # Extract structured content
            result = {
                'source': 'PMC',
                'pmcid': pmcid,
                'title': self._extract_pmc_title(soup),
                'abstract': self._extract_pmc_abstract(soup),
                'full_text': self._extract_pmc_body(soup),
                'sections': self._extract_pmc_sections(soup),
                'references': self._extract_pmc_references(soup),
                'figures_tables': self._extract_pmc_figures_tables(soup)
            }
            
            return result
            
        except Exception as e:
            print(f"PMC retrieval failed for {pmcid}: {e}")
            return None
    
    def _extract_pmc_title(self, soup: BeautifulSoup) -> str:
        """Extract title from PMC XML."""
        title_elem = soup.find('title-group')
        if title_elem:
            article_title = title_elem.find('article-title')
            if article_title:
                return article_title.get_text(strip=True)
        return ""
    
    def _extract_pmc_abstract(self, soup: BeautifulSoup) -> str:
        """Extract abstract from PMC XML."""
        abstract_elem = soup.find('abstract')
        if abstract_elem:
            # Handle structured abstracts
            sections = []
            for p in abstract_elem.find_all(['p', 'sec']):
                if p.name == 'sec':
                    title_elem = p.find('title')
                    title = title_elem.get_text(strip=True) if title_elem else ""
                    content = p.get_text(strip=True)
                    if title:
                        sections.append(f"{title}: {content}")
                    else:
                        sections.append(content)
                else:
                    sections.append(p.get_text(strip=True))
            return " ".join(sections)
        return ""
    
    def _extract_pmc_body(self, soup: BeautifulSoup) -> str:
        """Extract main body text from PMC XML."""
        body_elem = soup.find('body')
        if not body_elem:
            return ""
        
        sections = []
        for sec in body_elem.find_all('sec'):
            section_text = self._process_pmc_section(sec)
            if section_text:
                sections.append(section_text)
        
        return "\n\n".join(sections)
    
    def _extract_pmc_sections(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract individual sections from PMC XML."""
        sections = {}
        body_elem = soup.find('body')
        if not body_elem:
            return sections
        
        for sec in body_elem.find_all('sec'):
            title_elem = sec.find('title')
            if title_elem:
                title = title_elem.get_text(strip=True)
                content = self._process_pmc_section(sec, include_title=False)
                if content:
                    sections[title] = content
        
        return sections
    
    def _process_pmc_section(self, section: Tag, include_title: bool = True) -> str:
        """Process a PMC section element."""
        parts = []
        
        # Add section title
        if include_title:
            title_elem = section.find('title')
            if title_elem:
                title = title_elem.get_text(strip=True)
                parts.append(f"## {title}")
        
        # Process paragraphs
        for p in section.find_all('p'):
            text = p.get_text(strip=True)
            if text:
                parts.append(text)
        
        # Process subsections
        for subsec in section.find_all('sec'):
            subsec_text = self._process_pmc_section(subsec)
            if subsec_text:
                parts.append(subsec_text)
        
        return "\n\n".join(parts)
    
    def _extract_pmc_references(self, soup: BeautifulSoup) -> List[str]:
        """Extract references from PMC XML."""
        refs = []
        ref_list = soup.find('ref-list')
        if ref_list:
            for ref in ref_list.find_all('ref'):
                ref_text = ref.get_text(strip=True)
                if ref_text:
                    refs.append(ref_text)
        return refs
    
    def _extract_pmc_figures_tables(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract figure and table captions from PMC XML."""
        items = []
        
        # Extract figures
        for fig in soup.find_all('fig'):
            caption_elem = fig.find('caption')
            if caption_elem:
                title_elem = caption_elem.find('title')
                title = title_elem.get_text(strip=True) if title_elem else ""
                caption = caption_elem.get_text(strip=True)
                items.append({
                    'type': 'figure',
                    'title': title,
                    'caption': caption
                })
        
        # Extract tables
        for table in soup.find_all('table-wrap'):
            caption_elem = table.find('caption')
            if caption_elem:
                title_elem = caption_elem.find('title')
                title = title_elem.get_text(strip=True) if title_elem else ""
                caption = caption_elem.get_text(strip=True)
                items.append({
                    'type': 'table',
                    'title': title,
                    'caption': caption
                })
        
        return items
    
    def _get_arxiv_text(self, title: str, authors: List[str]) -> Optional[Dict[str, str]]:
        """
        Search arXiv for paper and retrieve full text if available.
        
        Parameters
        ----------
        title : str
            Paper title
        authors : List[str]
            List of authors
            
        Returns
        -------
        Optional[Dict[str, str]]
            ArXiv paper content or None
        """
        try:
            # Search arXiv API
            search_query = f'ti:"{title}"'
            if authors:
                # Add first author to search
                first_author = authors[0].split()[-1]  # Last name
                search_query += f' AND au:"{first_author}"'
            
            arxiv_url = f"http://export.arxiv.org/api/query?search_query={quote(search_query)}&max_results=1"
            
            response = self.session.get(arxiv_url, timeout=10)
            if response.status_code != 200:
                return None
            
            # Parse arXiv response
            soup = BeautifulSoup(response.content, 'xml')
            entries = soup.find_all('entry')
            
            if not entries:
                return None
            
            entry = entries[0]
            
            # Extract arXiv metadata
            arxiv_title = entry.find('title').get_text(strip=True) if entry.find('title') else ""
            arxiv_abstract = entry.find('summary').get_text(strip=True) if entry.find('summary') else ""
            arxiv_id = ""
            
            id_elem = entry.find('id')
            if id_elem:
                arxiv_id = id_elem.get_text(strip=True).split('/')[-1]
            
            # Check title similarity (basic check)
            if not self._titles_similar(title, arxiv_title):
                return None
            
            return {
                'source': 'arXiv',
                'arxiv_id': arxiv_id,
                'title': arxiv_title,
                'abstract': arxiv_abstract,
                'full_text': f"ArXiv preprint: {arxiv_abstract}",
                'sections': {'Abstract': arxiv_abstract},
                'note': 'This is a preprint from arXiv. Full PDF available at: ' + 
                       f'https://arxiv.org/abs/{arxiv_id}'
            }
            
        except Exception as e:
            print(f"ArXiv search failed: {e}")
            return None
    
    def _titles_similar(self, title1: str, title2: str, threshold: float = 0.7) -> bool:
        """Check if two titles are similar (basic similarity check)."""
        # Simple word overlap check
        words1 = set(re.findall(r'\w+', title1.lower()))
        words2 = set(re.findall(r'\w+', title2.lower()))
        
        if not words1 or not words2:
            return False
        
        overlap = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return overlap / union > threshold
    
    def _get_crossref_fulltext(self, doi: str) -> Optional[Dict[str, str]]:
        """
        Try to get full-text links from Crossref API.
        
        Parameters
        ----------
        doi : str
            DOI of the paper
            
        Returns
        -------
        Optional[Dict[str, str]]
            Information about full-text availability
        """
        try:
            # Query Crossref API
            crossref_url = f"https://api.crossref.org/works/{doi}"
            response = self.session.get(crossref_url, timeout=10)
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            work = data.get('message', {})
            
            # Check for full-text links
            links = work.get('link', [])
            full_text_links = []
            
            for link in links:
                if link.get('intended-application') == 'text-mining':
                    full_text_links.append(link.get('URL'))
            
            if full_text_links:
                return {
                    'source': 'Crossref',
                    'doi': doi,
                    'full_text_links': full_text_links,
                    'note': 'Full text may be available through publisher (subscription required)'
                }
            
            return None
            
        except Exception as e:
            print(f"Crossref lookup failed: {e}")
            return None


def format_enhanced_fulltext(fulltext_data: Dict[str, str]) -> str:
    """
    Format enhanced full-text data into a readable string.
    
    Parameters
    ----------
    fulltext_data : Dict[str, str]
        Full-text data from enhanced retriever
        
    Returns
    -------
    str
        Formatted full text
    """
    if not fulltext_data:
        return ""
    
    parts = []
    source = fulltext_data.get('source', 'Unknown')
    
    parts.append(f"=== FULL TEXT ({source}) ===")
    
    # Add title if available
    if fulltext_data.get('title'):
        parts.append(f"Title: {fulltext_data['title']}")
        parts.append("")
    
    # Add abstract if available and different from main abstract
    if fulltext_data.get('abstract'):
        parts.append("Abstract:")
        parts.append(fulltext_data['abstract'])
        parts.append("")
    
    # Add structured sections if available
    sections = fulltext_data.get('sections', {})
    if sections:
        for section_title, section_content in sections.items():
            parts.append(f"## {section_title}")
            parts.append(section_content)
            parts.append("")
    
    # Add main full text if no sections available
    elif fulltext_data.get('full_text'):
        parts.append("Full Text:")
        parts.append(fulltext_data['full_text'])
        parts.append("")
    
    # Add figures/tables if available
    figures_tables = fulltext_data.get('figures_tables', [])
    if figures_tables:
        parts.append("## Figures and Tables")
        for item in figures_tables[:5]:  # Limit to first 5
            item_type = item.get('type', 'item').title()
            title = item.get('title', '')
            caption = item.get('caption', '')
            parts.append(f"{item_type}: {title}")
            if caption:
                parts.append(f"Caption: {caption}")
            parts.append("")
    
    # Add any notes
    if fulltext_data.get('note'):
        parts.append("Note:")
        parts.append(fulltext_data['note'])
        parts.append("")
    
    # Add references info if available
    references = fulltext_data.get('references', [])
    if references:
        parts.append(f"References: {len(references)} references available")
    
    return "\n".join(parts)