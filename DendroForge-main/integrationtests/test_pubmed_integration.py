"""
Integration tests for PubMed search tool.

These tests make actual API calls to PubMed to verify the tool works correctly
with real data. They should be run sparingly to avoid rate limiting.
"""

import json
import time
import unittest
from unittest import skipIf
import os

from src.tools.pubmed_search import (
    PubMedSearchTool,
    search_pubmed_papers,
    get_pubmed_paper_details,
)


class TestPubMedIntegration(unittest.TestCase):
    """Integration tests for PubMed search tool."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.tool = PubMedSearchTool()
        # Add a small delay to avoid rate limiting
        time.sleep(0.5)
    
    def test_search_pubmed_real_query(self):
        """Test searching PubMed with a real query."""
        # Use a specific medical term that should return results
        results = self.tool.search_pubmed("aspirin cardiology", max_results=5)
        
        # Verify we got results
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)
        self.assertLessEqual(len(results), 5)
        
        # Verify PMIDs are numeric strings
        for pmid in results:
            self.assertIsInstance(pmid, str)
            self.assertTrue(pmid.isdigit())
    
    def test_get_article_details_real_pmid(self):
        """Test retrieving details for a real PMID."""
        # Using a well-known PMID (first paper describing PCR)
        pmid = "2448875"  # "Primer-directed enzymatic amplification of DNA..."
        
        articles = self.tool.get_article_details([pmid])
        
        self.assertEqual(len(articles), 1)
        article = articles[0]
        
        # Verify basic fields are populated
        self.assertEqual(article.pmid, pmid)
        self.assertIsInstance(article.title, str)
        self.assertGreater(len(article.title), 0)
        self.assertIsInstance(article.authors, list)
        self.assertGreater(len(article.authors), 0)
        self.assertIsInstance(article.journal, str)
        self.assertGreater(len(article.journal), 0)
        
        # Verify expected content for this specific paper
        self.assertIn("AMPLIFICATION", article.title.upper())
        self.assertIn("Mullis", str(article.authors))
    
    def test_get_article_details_multiple_pmids(self):
        """Test retrieving details for multiple real PMIDs."""
        # Using two well-known PMIDs
        pmids = ["2448875", "3180494"]  # PCR papers
        
        articles = self.tool.get_article_details(pmids)
        
        self.assertEqual(len(articles), 2)
        
        # Verify both articles have required fields
        for article in articles:
            self.assertIsInstance(article.pmid, str)
            self.assertIn(article.pmid, pmids)
            self.assertIsInstance(article.title, str)
            self.assertGreater(len(article.title), 0)
            self.assertIsInstance(article.authors, list)
            self.assertIsInstance(article.journal, str)
    
    def test_parse_article_record_real_structure(self):
        """Test parsing with real PubMed record structure."""
        # Get a real article to test parsing
        pmid = "2985564"
        articles = self.tool.get_article_details([pmid])
        
        self.assertEqual(len(articles), 1)
        article = articles[0]
        
        # Verify all expected fields are present
        self.assertIsInstance(article.pmid, str)
        self.assertIsInstance(article.title, str)
        self.assertIsInstance(article.authors, list)
        self.assertIsInstance(article.abstract, str)
        self.assertIsInstance(article.journal, str)
        self.assertIsInstance(article.publication_date, str)
        self.assertIsInstance(article.doi, str)
        self.assertIsInstance(article.pmcid, str)
        self.assertIsInstance(article.keywords, list)
        self.assertIsInstance(article.affiliations, list)
        
        # Check that some fields have meaningful content
        self.assertGreater(len(article.title), 10)
        self.assertGreater(len(article.authors), 0)
        self.assertGreater(len(article.journal), 0)
    
    def test_search_no_results(self):
        """Test search with query that returns no results."""
        # Use a very specific nonsensical query
        results = self.tool.search_pubmed("xyzabc12345nonexistent", max_results=5)
        
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 0)
    
    def test_get_article_details_invalid_pmid(self):
        """Test retrieving details for invalid PMID."""
        # Use a clearly invalid PMID - should return empty list instead of raising exception
        articles = self.tool.get_article_details(["999999999"])
        # Should return empty list for invalid PMID
        self.assertEqual(len(articles), 0)
    
    def test_publication_date_parsing(self):
        """Test publication date parsing with real data."""
        # Get an article with a known publication date
        pmid = "2448875"
        articles = self.tool.get_article_details([pmid])
        
        article = articles[0]
        
        # This paper was published in 1988
        self.assertTrue(article.publication_date.startswith("1988"))
    
    def test_mesh_keywords_extraction(self):
        """Test MeSH keyword extraction with real data."""
        # Get an article that should have MeSH terms
        pmid = "2448875"
        articles = self.tool.get_article_details([pmid])
        
        article = articles[0]
        
        # Should have some MeSH terms
        self.assertIsInstance(article.keywords, list)
        # This paper should have DNA-related keywords
        keywords_str = " ".join(article.keywords).lower()
        self.assertTrue(any(term in keywords_str for term in ["dna", "pcr", "amplification"]))
    
    def test_pmc_full_text_retrieval(self):
        """Test PMC full text retrieval with a known open access article."""
        # Use a known PMC ID for an open access article
        # PMC2279113 is a well-known open access paper
        pmcid = "PMC2279113"
        
        full_text = self.tool.get_pmc_full_text(pmcid)
        
        if full_text:  # Not all papers have full text available
            self.assertIsInstance(full_text, str)
            self.assertGreater(len(full_text), 100)
            # Should contain typical article sections
            self.assertTrue(any(section in full_text.lower() for section in 
                              ["abstract", "introduction", "methods", "results", "conclusion"]))
    
    def test_rate_limiting(self):
        """Test that rate limiting is working."""
        start_time = time.time()
        
        # Make multiple quick requests
        for i in range(3):
            self.tool.search_pubmed("test", max_results=1)
        
        elapsed_time = time.time() - start_time
        
        # Should take at least 1 second due to rate limiting (3 requests * 0.34s)
        self.assertGreater(elapsed_time, 1.0)


class TestPubMedSearchFunctionsIntegration(unittest.TestCase):
    """Integration tests for module-level functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Add a small delay to avoid rate limiting
        time.sleep(0.5)
    
    def test_search_pubmed_papers_real_query(self):
        """Test the search_pubmed_papers function with a real query."""
        result = search_pubmed_papers("machine learning bioinformatics", max_results=3)
        
        # Parse the JSON result
        data = json.loads(result)
        
        # Verify structure
        self.assertIn("query", data)
        self.assertIn("total_results", data)
        self.assertIn("papers", data)
        
        self.assertEqual(data["query"], "machine learning bioinformatics")
        self.assertIsInstance(data["total_results"], int)
        self.assertGreater(data["total_results"], 0)
        self.assertLessEqual(data["total_results"], 3)
        
        # Verify each paper has required fields
        for paper in data["papers"]:
            self.assertIn("pmid", paper)
            self.assertIn("title", paper)
            self.assertIn("authors", paper)
            self.assertIn("abstract", paper)
            self.assertIn("journal", paper)
            self.assertIn("publication_date", paper)
            self.assertIn("doi", paper)
            self.assertIn("pmcid", paper)
            self.assertIn("keywords", paper)
            self.assertIn("affiliations", paper)
            
            # Verify data types
            self.assertIsInstance(paper["pmid"], str)
            self.assertIsInstance(paper["title"], str)
            self.assertIsInstance(paper["authors"], list)
            self.assertIsInstance(paper["abstract"], str)
            self.assertIsInstance(paper["journal"], str)
            self.assertIsInstance(paper["publication_date"], str)
            self.assertIsInstance(paper["doi"], str)
            self.assertIsInstance(paper["pmcid"], str)
            self.assertIsInstance(paper["keywords"], list)
            self.assertIsInstance(paper["affiliations"], list)
    
    def test_search_pubmed_papers_with_field_tags(self):
        """Test search with PubMed field tags."""
        # Search for papers by a specific author
        result = search_pubmed_papers("Venter[Author]", max_results=2)
        
        data = json.loads(result)
        
        self.assertGreater(data["total_results"], 0)
        self.assertLessEqual(data["total_results"], 2)
        
        # At least one paper should have Venter as an author
        found_venter = False
        for paper in data["papers"]:
            authors_str = " ".join(paper["authors"]).lower()
            if "venter" in authors_str:
                found_venter = True
                break
        
        self.assertTrue(found_venter)
    
    def test_search_pubmed_papers_no_results(self):
        """Test search with query that returns no results."""
        result = search_pubmed_papers("xyzabc12345nonexistent", max_results=5)
        
        data = json.loads(result)
        
        self.assertEqual(data["query"], "xyzabc12345nonexistent")
        self.assertEqual(data["total_results"], 0)
        self.assertEqual(len(data["papers"]), 0)
    
    def test_search_pubmed_papers_with_full_text(self):
        """Test search with full text retrieval."""
        # Search for papers that might have PMC full text
        result = search_pubmed_papers("open access genomics", max_results=2, include_full_text=True)
        
        data = json.loads(result)
        
        # Check if any papers have full text
        full_text_found = False
        for paper in data["papers"]:
            if "full_text" in paper and paper["full_text"]:
                full_text_found = True
                self.assertIsInstance(paper["full_text"], str)
                self.assertGreater(len(paper["full_text"]), 100)
        
        # Note: Not all papers will have full text, so we don't assert this
        # just verify the structure if it exists
    
    def test_get_pubmed_paper_details_real_pmid(self):
        """Test the get_pubmed_paper_details function with a real PMID."""
        # Using a well-known PMID
        pmid = "2448875"
        
        result = get_pubmed_paper_details(pmid)
        data = json.loads(result)
        
        # Verify structure
        self.assertEqual(data["pmid"], pmid)
        self.assertIn("title", data)
        self.assertIn("authors", data)
        self.assertIn("abstract", data)
        self.assertIn("journal", data)
        self.assertIn("publication_date", data)
        self.assertIn("doi", data)
        self.assertIn("pmcid", data)
        self.assertIn("keywords", data)
        self.assertIn("affiliations", data)
        
        # Verify meaningful content
        self.assertGreater(len(data["title"]), 10)
        self.assertGreater(len(data["authors"]), 0)
        self.assertGreater(len(data["journal"]), 0)
        self.assertIn("AMPLIFICATION", data["title"].upper())
    
    def test_get_pubmed_paper_details_invalid_pmid(self):
        """Test get_pubmed_paper_details with invalid PMID."""
        result = get_pubmed_paper_details("999999999")
        data = json.loads(result)
        
        self.assertIn("error", data)
        self.assertEqual(data["pmid"], "999999999")
    
    def test_max_results_limiting(self):
        """Test that max_results is properly limited to 100."""
        # This would normally be tested in unit tests, but let's verify
        # it works in practice
        result = search_pubmed_papers("cancer", max_results=150)
        
        data = json.loads(result)
        
        # Should be limited to 100 results max
        self.assertLessEqual(data["total_results"], 100)
    
    def test_search_recent_papers(self):
        """Test searching for recent papers."""
        # Search for papers from recent years
        result = search_pubmed_papers("COVID-19 vaccine 2023[pdat]", max_results=3)
        
        data = json.loads(result)
        
        if data["total_results"] > 0:
            # Check that papers are from 2023
            for paper in data["papers"]:
                if paper["publication_date"]:
                    self.assertTrue(paper["publication_date"].startswith("2023"))
    
    def test_search_specific_journal(self):
        """Test searching within a specific journal."""
        # Search for papers in Nature
        result = search_pubmed_papers("Nature[journal] genomics", max_results=2)
        
        data = json.loads(result)
        
        if data["total_results"] > 0:
            # At least one paper should be from Nature
            nature_found = False
            for paper in data["papers"]:
                if "nature" in paper["journal"].lower():
                    nature_found = True
                    break
            
            self.assertTrue(nature_found)


class TestPubMedSearchRealWorldScenarios(unittest.TestCase):
    """Test real-world usage scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        time.sleep(0.5)
    
    def test_bioinformatics_literature_search(self):
        """Test searching for bioinformatics literature."""
        # A typical bioinformatics search
        result = search_pubmed_papers("CRISPR gene editing", max_results=5)
        
        data = json.loads(result)
        
        self.assertGreater(data["total_results"], 0)
        
        # Should find relevant papers
        crispr_found = False
        for paper in data["papers"]:
            title_abstract = (paper["title"] + " " + paper["abstract"]).lower()
            if "crispr" in title_abstract or "gene edit" in title_abstract:
                crispr_found = True
                break
        
        self.assertTrue(crispr_found)
    
    def test_author_search(self):
        """Test searching for papers by a specific author."""
        # Search for papers by Jennifer Doudna (CRISPR pioneer)
        result = search_pubmed_papers("Doudna[Author] CRISPR", max_results=3)
        
        data = json.loads(result)
        
        if data["total_results"] > 0:
            # Should find papers with Doudna as author
            doudna_found = False
            for paper in data["papers"]:
                authors_str = " ".join(paper["authors"]).lower()
                if "doudna" in authors_str:
                    doudna_found = True
                    break
            
            self.assertTrue(doudna_found)
    
    def test_clinical_trial_search(self):
        """Test searching for clinical trials."""
        # Search for clinical trials
        result = search_pubmed_papers("clinical trial[pt] cancer immunotherapy", max_results=3)
        
        data = json.loads(result)
        
        if data["total_results"] > 0:
            # Should find clinical trial papers
            clinical_found = False
            for paper in data["papers"]:
                title_abstract = (paper["title"] + " " + paper["abstract"]).lower()
                if "clinical trial" in title_abstract or "trial" in title_abstract:
                    clinical_found = True
                    break
            
            self.assertTrue(clinical_found)
    
    def test_mesh_term_search(self):
        """Test searching using MeSH terms."""
        # Search using MeSH terms
        result = search_pubmed_papers("Neoplasms[MeSH] immunotherapy", max_results=3)
        
        data = json.loads(result)
        
        if data["total_results"] > 0:
            # Should find cancer-related papers
            cancer_found = False
            for paper in data["papers"]:
                title_abstract = (paper["title"] + " " + paper["abstract"]).lower()
                keywords_str = " ".join(paper["keywords"]).lower()
                if any(term in title_abstract or term in keywords_str 
                      for term in ["cancer", "tumor", "neoplasm", "immunotherapy"]):
                    cancer_found = True
                    break
            
            self.assertTrue(cancer_found)


if __name__ == '__main__':
    unittest.main()