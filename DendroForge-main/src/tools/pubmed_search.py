"""
PubMed Search Tool for DendroForge.

This module provides functionality to search PubMed for papers and
retrieve their details including abstracts and full text when available from PMC.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import requests
from Bio import Entrez


@dataclass
class PubMedArticle:
    """
    Represents a PubMed article with its metadata.
    
    Attributes
    ----------
    pmid : str
        PubMed identifier
    title : str
        Article title
    authors : List[str]
        List of author names
    abstract : str
        Article abstract
    journal : str
        Journal name
    publication_date : str
        Publication date (YYYY-MM-DD format when available)
    doi : str
        Digital Object Identifier
    pmcid : str
        PubMed Central identifier (if available)
    keywords : List[str]
        MeSH terms and keywords
    affiliations : List[str]
        Author affiliations
    """
    pmid: str
    title: str
    authors: List[str]
    abstract: str
    journal: str
    publication_date: str
    doi: str
    pmcid: str
    keywords: List[str]
    affiliations: List[str]


class PubMedSearchTool:
    """
    A tool for searching PubMed and retrieving article information.
    
    This tool provides methods to search PubMed using the NCBI E-utilities API
    and retrieve detailed information about scientific articles.
    """
    
    def __init__(self, email: str = "research@dendroforge.ai", api_key: Optional[str] = None):
        """
        Initialize the PubMed search tool.
        
        Parameters
        ----------
        email : str, optional
            Email address for NCBI E-utilities (required by NCBI guidelines)
        api_key : str, optional
            NCBI API key for higher rate limits (10 requests/second vs 3)
        """
        self.email = email
        self.api_key = api_key
        Entrez.email = email
        if api_key:
            Entrez.api_key = api_key
        
        # Rate limiting: 3 requests/second without API key, 10 with API key
        self.rate_limit = 0.1 if api_key else 0.34
        self.last_request_time = 0
        
    def _rate_limit_wait(self) -> None:
        """Ensure we don't exceed NCBI rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.rate_limit:
            time.sleep(self.rate_limit - time_since_last)
        self.last_request_time = time.time()
        
    def search_pubmed(self, query: str, max_results: int = 20, 
                      sort_by: str = "relevance") -> List[str]:
        """
        Search PubMed for articles matching the given query.
        
        Parameters
        ----------
        query : str
            Search query (can include field tags like "cancer[MeSH]")
        max_results : int, optional
            Maximum number of results to return (default: 20)
        sort_by : str, optional
            Sort order: "relevance", "pub_date", "first_author", "last_author", "journal"
        
        Returns
        -------
        List[str]
            List of PubMed IDs (PMIDs) matching the query
            
        Raises
        ------
        Exception
            If the search fails due to network or API errors
        """
        self._rate_limit_wait()
        
        try:
            handle = Entrez.esearch(
                db="pubmed",
                term=query,
                retmax=max_results,
                sort=sort_by,
                usehistory="y"
            )
            results = Entrez.read(handle)
            handle.close()
            
            return results["IdList"]
            
        except Exception as e:
            raise Exception(f"PubMed search failed: {str(e)}")
    
    def get_article_details(self, pmids: Union[str, List[str]]) -> List[PubMedArticle]:
        """
        Retrieve detailed information for given PubMed IDs.
        
        Parameters
        ----------
        pmids : str or List[str]
            Single PMID or list of PMIDs to retrieve
            
        Returns
        -------
        List[PubMedArticle]
            List of article objects with detailed information
            
        Raises
        ------
        Exception
            If the retrieval fails due to network or API errors
        """
        if isinstance(pmids, str):
            pmids = [pmids]
            
        self._rate_limit_wait()
        
        try:
            handle = Entrez.efetch(
                db="pubmed",
                id=pmids,
                rettype="medline",
                retmode="xml"
            )
            records = Entrez.read(handle)
            handle.close()
            
            articles = []
            for record in records["PubmedArticle"]:
                article = self._parse_article_record(record)
                articles.append(article)
                
            return articles
            
        except Exception as e:
            raise Exception(f"Article retrieval failed: {str(e)}")
    
    def _parse_article_record(self, record: Dict[str, Any]) -> PubMedArticle:
        """
        Parse a PubMed article record into a PubMedArticle object.
        
        Parameters
        ----------
        record : Dict[str, Any]
            Raw article record from NCBI
            
        Returns
        -------
        PubMedArticle
            Parsed article object
        """
        citation = record.get("MedlineCitation", {})
        article = citation.get("Article", {})
        
        # Extract PMID
        pmid = str(citation.get("PMID", ""))
        
        # Extract title
        title = article.get("ArticleTitle", "")
        
        # Extract authors
        authors = []
        author_list = article.get("AuthorList", [])
        for author in author_list:
            if "LastName" in author and "ForeName" in author:
                authors.append(f"{author['ForeName']} {author['LastName']}")
            elif "CollectiveName" in author:
                authors.append(author["CollectiveName"])
        
        # Extract abstract
        abstract = ""
        abstract_texts = article.get("Abstract", {}).get("AbstractText", [])
        if abstract_texts:
            if isinstance(abstract_texts, list):
                abstract = " ".join(str(text) for text in abstract_texts)
            else:
                abstract = str(abstract_texts)
        
        # Extract journal
        journal = article.get("Journal", {}).get("Title", "")
        
        # Extract publication date
        pub_date = self._extract_publication_date(article)
        
        # Extract DOI
        doi = ""
        article_ids = record.get("PubmedData", {}).get("ArticleIdList", [])
        for article_id in article_ids:
            # Check if it's a StringElement with attributes
            if hasattr(article_id, 'attributes') and article_id.attributes.get("IdType") == "doi":
                doi = str(article_id)
                break
        
        # Extract PMC ID
        pmcid = ""
        for article_id in article_ids:
            # Check if it's a StringElement with attributes
            if hasattr(article_id, 'attributes') and article_id.attributes.get("IdType") == "pmc":
                pmcid = str(article_id)
                break
        
        # Extract keywords/MeSH terms
        keywords = []
        mesh_list = citation.get("MeshHeadingList", [])
        for mesh in mesh_list:
            descriptor = mesh.get("DescriptorName", {})
            if descriptor:
                keywords.append(str(descriptor))
        
        # Extract affiliations
        affiliations = []
        for author in author_list:
            if "AffiliationInfo" in author:
                for affiliation in author["AffiliationInfo"]:
                    if "Affiliation" in affiliation:
                        affiliations.append(affiliation["Affiliation"])
        
        return PubMedArticle(
            pmid=pmid,
            title=title,
            authors=authors,
            abstract=abstract,
            journal=journal,
            publication_date=pub_date,
            doi=doi,
            pmcid=pmcid,
            keywords=keywords,
            affiliations=affiliations
        )
    
    def _extract_publication_date(self, article: Dict[str, Any]) -> str:
        """Extract publication date from article record."""
        journal = article.get("Journal", {})
        journal_issue = journal.get("JournalIssue", {})
        pub_date = journal_issue.get("PubDate", {})
        
        if not pub_date:
            return ""
        
        year = pub_date.get("Year", "")
        month = pub_date.get("Month", "")
        day = pub_date.get("Day", "")
        
        # Try to format as YYYY-MM-DD
        if year:
            try:
                if month:
                    # Convert month name to number if necessary
                    month_num = self._month_to_number(month)
                    if day:
                        return f"{year}-{month_num:02d}-{int(day):02d}"
                    else:
                        return f"{year}-{month_num:02d}"
                else:
                    return year
            except (ValueError, TypeError):
                return year
        
        return ""
    
    def _month_to_number(self, month: str) -> int:
        """Convert month name to number."""
        months = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
            "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
            "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
        }
        if month.isdigit():
            return int(month)
        return months.get(month, 1)
    
    def get_enhanced_full_text(self, pmid: str, pmcid: str = "", doi: str = "", 
                             title: str = "", authors: List[str] = None) -> Optional[str]:
        """
        Retrieve enhanced full text from multiple sources.
        
        Parameters
        ----------
        pmid : str
            PubMed ID
        pmcid : str, optional
            PMC ID if available  
        doi : str, optional
            DOI if available
        title : str, optional
            Paper title for additional searches
        authors : List[str], optional
            Authors for additional searches
            
        Returns
        -------
        Optional[str]
            Enhanced full text content if available, None otherwise
        """
        try:
            from .enhanced_fulltext import EnhancedFullTextRetriever, format_enhanced_fulltext
            
            retriever = EnhancedFullTextRetriever(email=self.email, api_key=self.api_key)
            fulltext_data = retriever.get_full_text(
                pmid=pmid, 
                pmcid=pmcid, 
                doi=doi, 
                title=title, 
                authors=authors or []
            )
            
            if fulltext_data:
                return format_enhanced_fulltext(fulltext_data)
            
            return None
            
        except Exception as e:
            print(f"Enhanced full-text retrieval failed: {e}")
            return None
    
    def get_pmc_full_text(self, pmcid: str) -> Optional[str]:
        """
        Retrieve full text from PMC if available (legacy method).
        
        Parameters
        ----------
        pmcid : str
            PMC identifier (e.g., "PMC1234567")
            
        Returns
        -------
        Optional[str]
            Full text content if available, None otherwise
        """
        # Use enhanced retrieval for better results
        return self.get_enhanced_full_text(pmid="", pmcid=pmcid)


def search_pubmed_papers(query: str, max_results: int = 20, 
                        include_full_text: bool = False,
                        api_key: Optional[str] = None) -> str:
    """
    Search PubMed for papers related to a topic and return structured results.
    
    This function provides a simple interface to search PubMed and retrieve
    detailed information about scientific papers including abstracts and
    optionally full text from PMC when available.
    
    Parameters
    ----------
    query : str
        Search query. Can include field tags like "cancer[MeSH]" or
        "Smith[Author]" for more specific searches. Examples:
        - "machine learning bioinformatics"
        - "COVID-19[MeSH] AND vaccine"
        - "Smith[Author] AND genomics"
    max_results : int, optional
        Maximum number of results to return (default: 20, max: 100)
    include_full_text : bool, optional
        Whether to attempt to retrieve full text from PMC when available
        (default: False, as it significantly increases processing time)
    api_key : str, optional
        NCBI API key for higher rate limits. If not provided, will use
        standard rate limits (3 requests/second)
    
    Returns
    -------
    str
        JSON string containing search results with the following structure:
        {
            "query": "original search query",
            "total_results": number,
            "papers": [
                {
                    "pmid": "PubMed ID",
                    "title": "Paper title",
                    "authors": ["Author1", "Author2"],
                    "abstract": "Abstract text",
                    "journal": "Journal name",
                    "publication_date": "YYYY-MM-DD",
                    "doi": "DOI identifier",
                    "pmcid": "PMC identifier",
                    "keywords": ["keyword1", "keyword2"],
                    "affiliations": ["affiliation1", "affiliation2"],
                    "full_text": "Full text content (if requested and available)"
                }
            ]
        }
    
    Examples
    --------
    >>> results = search_pubmed_papers("CRISPR gene editing", max_results=5)
    >>> data = json.loads(results)
    >>> print(f"Found {data['total_results']} papers")
    >>> print(f"First paper: {data['papers'][0]['title']}")
    """
    # Limit max_results to prevent abuse
    max_results = min(max_results, 100)
    
    try:
        # Initialize the search tool
        tool = PubMedSearchTool(api_key=api_key)
        
        # Search for papers
        pmids = tool.search_pubmed(query, max_results=max_results)
        
        if not pmids:
            return json.dumps({
                "query": query,
                "total_results": 0,
                "papers": []
            })
        
        # Retrieve detailed information
        articles = tool.get_article_details(pmids)
        
        # Convert to dictionary format
        papers = []
        for article in articles:
            paper_dict = {
                "pmid": article.pmid,
                "title": article.title,
                "authors": article.authors,
                "abstract": article.abstract,
                "journal": article.journal,
                "publication_date": article.publication_date,
                "doi": article.doi,
                "pmcid": article.pmcid,
                "keywords": article.keywords,
                "affiliations": article.affiliations
            }
            
            # Add full text if requested
            if include_full_text:
                full_text = tool.get_enhanced_full_text(
                    pmid=article.pmid,
                    pmcid=article.pmcid,
                    doi=article.doi,
                    title=article.title,
                    authors=article.authors
                )
                paper_dict["full_text"] = full_text
            
            papers.append(paper_dict)
        
        return json.dumps({
            "query": query,
            "total_results": len(papers),
            "papers": papers
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "error": f"PubMed search failed: {str(e)}",
            "query": query,
            "total_results": 0,
            "papers": []
        })


def get_pubmed_paper_details(pmid: str, include_full_text: bool = False,
                           api_key: Optional[str] = None) -> str:
    """
    Retrieve detailed information for a specific PubMed paper.
    
    Parameters
    ----------
    pmid : str
        PubMed identifier (e.g., "12345678")
    include_full_text : bool, optional
        Whether to attempt to retrieve full text from PMC when available
        (default: False)
    api_key : str, optional
        NCBI API key for higher rate limits
    
    Returns
    -------
    str
        JSON string containing paper details with the same structure as
        individual papers in search_pubmed_papers results
    
    Examples
    --------
    >>> paper = get_pubmed_paper_details("12345678")
    >>> data = json.loads(paper)
    >>> print(f"Title: {data['title']}")
    >>> print(f"Authors: {', '.join(data['authors'])}")
    """
    try:
        # Initialize the search tool
        tool = PubMedSearchTool(api_key=api_key)
        
        # Retrieve detailed information
        articles = tool.get_article_details([pmid])
        
        if not articles:
            return json.dumps({
                "error": f"No article found with PMID: {pmid}",
                "pmid": pmid
            })
        
        article = articles[0]
        paper_dict = {
            "pmid": article.pmid,
            "title": article.title,
            "authors": article.authors,
            "abstract": article.abstract,
            "journal": article.journal,
            "publication_date": article.publication_date,
            "doi": article.doi,
            "pmcid": article.pmcid,
            "keywords": article.keywords,
            "affiliations": article.affiliations
        }
        
        # Add full text if requested
        if include_full_text:
            full_text = tool.get_enhanced_full_text(
                pmid=article.pmid,
                pmcid=article.pmcid,
                doi=article.doi,
                title=article.title,
                authors=article.authors
            )
            paper_dict["full_text"] = full_text
        
        return json.dumps(paper_dict, indent=2)
        
    except Exception as e:
        return json.dumps({
            "error": f"Failed to retrieve paper details: {str(e)}",
            "pmid": pmid
        })