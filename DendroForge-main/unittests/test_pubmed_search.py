"""
Unit tests for PubMed search tool.

These tests use mock responses to avoid making actual API calls during testing.
"""

import json
import unittest
from unittest.mock import Mock, patch, MagicMock
import pytest

from src.tools.pubmed_search import (
    PubMedSearchTool,
    PubMedArticle,
    search_pubmed_papers,
    get_pubmed_paper_details,
)


class TestPubMedArticle(unittest.TestCase):
    """Test the PubMedArticle dataclass."""
    
    def test_pubmed_article_creation(self):
        """Test creating a PubMedArticle instance."""
        article = PubMedArticle(
            pmid="12345678",
            title="Test Article",
            authors=["John Doe", "Jane Smith"],
            abstract="This is a test abstract.",
            journal="Test Journal",
            publication_date="2023-01-15",
            doi="10.1000/test123",
            pmcid="PMC1234567",
            keywords=["test", "science"],
            affiliations=["University of Test"]
        )
        
        self.assertEqual(article.pmid, "12345678")
        self.assertEqual(article.title, "Test Article")
        self.assertEqual(len(article.authors), 2)
        self.assertEqual(article.authors[0], "John Doe")
        self.assertEqual(article.abstract, "This is a test abstract.")
        self.assertEqual(article.journal, "Test Journal")
        self.assertEqual(article.publication_date, "2023-01-15")
        self.assertEqual(article.doi, "10.1000/test123")
        self.assertEqual(article.pmcid, "PMC1234567")
        self.assertEqual(len(article.keywords), 2)
        self.assertEqual(len(article.affiliations), 1)


class TestPubMedSearchTool(unittest.TestCase):
    """Test the PubMedSearchTool class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.tool = PubMedSearchTool(email="test@example.com")
    
    def test_init_default_values(self):
        """Test tool initialization with default values."""
        tool = PubMedSearchTool()
        self.assertEqual(tool.email, "research@dendroforge.ai")
        self.assertIsNone(tool.api_key)
        self.assertEqual(tool.rate_limit, 0.34)  # No API key rate limit
    
    def test_init_with_api_key(self):
        """Test tool initialization with API key."""
        tool = PubMedSearchTool(api_key="test_key")
        self.assertEqual(tool.api_key, "test_key")
        self.assertEqual(tool.rate_limit, 0.1)  # API key rate limit
    
    @patch('src.tools.pubmed_search.Entrez')
    def test_search_pubmed_success(self, mock_entrez):
        """Test successful PubMed search."""
        # Mock the search response
        mock_handle = Mock()
        mock_entrez.esearch.return_value = mock_handle
        mock_entrez.read.return_value = {
            "IdList": ["12345678", "23456789", "34567890"]
        }
        
        results = self.tool.search_pubmed("cancer", max_results=3)
        
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0], "12345678")
        self.assertEqual(results[1], "23456789")
        self.assertEqual(results[2], "34567890")
        
        # Verify API call parameters
        mock_entrez.esearch.assert_called_once_with(
            db="pubmed",
            term="cancer",
            retmax=3,
            sort="relevance",
            usehistory="y"
        )
    
    @patch('src.tools.pubmed_search.Entrez')
    def test_search_pubmed_empty_results(self, mock_entrez):
        """Test PubMed search with no results."""
        mock_handle = Mock()
        mock_entrez.esearch.return_value = mock_handle
        mock_entrez.read.return_value = {"IdList": []}
        
        results = self.tool.search_pubmed("nonexistent_term")
        
        self.assertEqual(len(results), 0)
        self.assertEqual(results, [])
    
    @patch('src.tools.pubmed_search.Entrez')
    def test_search_pubmed_exception(self, mock_entrez):
        """Test PubMed search with API exception."""
        mock_entrez.esearch.side_effect = Exception("API Error")
        
        with self.assertRaises(Exception) as context:
            self.tool.search_pubmed("cancer")
        
        self.assertIn("PubMed search failed", str(context.exception))
    
    @patch('src.tools.pubmed_search.Entrez')
    def test_get_article_details_success(self, mock_entrez):
        """Test successful article details retrieval."""
        # Mock the article details response
        mock_handle = Mock()
        mock_entrez.efetch.return_value = mock_handle
        mock_entrez.read.return_value = {
            "PubmedArticle": [{
                "MedlineCitation": {
                    "PMID": "12345678",
                    "Article": {
                        "ArticleTitle": "Test Article Title",
                        "AuthorList": [{
                            "LastName": "Doe",
                            "ForeName": "John",
                        }],
                        "Abstract": {
                            "AbstractText": ["This is a test abstract."]
                        },
                        "Journal": {
                            "Title": "Test Journal",
                            "JournalIssue": {
                                "PubDate": {
                                    "Year": "2023",
                                    "Month": "Jan",
                                    "Day": "15"
                                }
                            }
                        }
                    },
                    "MeshHeadingList": [{
                        "DescriptorName": "test keyword"
                    }]
                },
                "PubmedData": {
                    "ArticleIdList": [{
                        "IdType": "doi",
                        "text": "10.1000/test123"
                    }]
                }
            }]
        }
        
        articles = self.tool.get_article_details(["12345678"])
        
        self.assertEqual(len(articles), 1)
        article = articles[0]
        self.assertEqual(article.pmid, "12345678")
        self.assertEqual(article.title, "Test Article Title")
        self.assertEqual(len(article.authors), 1)
        self.assertEqual(article.authors[0], "John Doe")
        self.assertEqual(article.abstract, "This is a test abstract.")
        self.assertEqual(article.journal, "Test Journal")
        self.assertEqual(article.publication_date, "2023-01-15")
    
    @patch('src.tools.pubmed_search.Entrez')
    def test_get_article_details_multiple_pmids(self, mock_entrez):
        """Test article details retrieval with multiple PMIDs."""
        mock_handle = Mock()
        mock_entrez.efetch.return_value = mock_handle
        mock_entrez.read.return_value = {
            "PubmedArticle": [
                {
                    "MedlineCitation": {
                        "PMID": "12345678",
                        "Article": {
                            "ArticleTitle": "First Article",
                            "AuthorList": [],
                            "Abstract": {"AbstractText": ["First abstract"]},
                            "Journal": {"Title": "Journal 1"}
                        }
                    },
                    "PubmedData": {"ArticleIdList": []}
                },
                {
                    "MedlineCitation": {
                        "PMID": "23456789",
                        "Article": {
                            "ArticleTitle": "Second Article",
                            "AuthorList": [],
                            "Abstract": {"AbstractText": ["Second abstract"]},
                            "Journal": {"Title": "Journal 2"}
                        }
                    },
                    "PubmedData": {"ArticleIdList": []}
                }
            ]
        }
        
        articles = self.tool.get_article_details(["12345678", "23456789"])
        
        self.assertEqual(len(articles), 2)
        self.assertEqual(articles[0].pmid, "12345678")
        self.assertEqual(articles[1].pmid, "23456789")
        self.assertEqual(articles[0].title, "First Article")
        self.assertEqual(articles[1].title, "Second Article")
    
    @patch('src.tools.pubmed_search.Entrez')
    def test_get_article_details_exception(self, mock_entrez):
        """Test article details retrieval with API exception."""
        mock_entrez.efetch.side_effect = Exception("API Error")
        
        with self.assertRaises(Exception) as context:
            self.tool.get_article_details(["12345678"])
        
        self.assertIn("Article retrieval failed", str(context.exception))
    
    def test_parse_article_record_complete(self):
        """Test parsing a complete article record."""
        record = {
            "MedlineCitation": {
                "PMID": "12345678",
                "Article": {
                    "ArticleTitle": "Complete Test Article",
                    "AuthorList": [
                        {
                            "LastName": "Doe",
                            "ForeName": "John",
                            "AffiliationInfo": [
                                {"Affiliation": "University of Test"}
                            ]
                        },
                        {
                            "CollectiveName": "Test Consortium"
                        }
                    ],
                    "Abstract": {
                        "AbstractText": ["Background: Test.", "Methods: Test.", "Results: Test."]
                    },
                    "Journal": {
                        "Title": "Test Journal",
                        "JournalIssue": {
                            "PubDate": {
                                "Year": "2023",
                                "Month": "Jan",
                                "Day": "15"
                            }
                        }
                    }
                },
                "MeshHeadingList": [
                    {"DescriptorName": "keyword1"},
                    {"DescriptorName": "keyword2"}
                ]
            },
            "PubmedData": {
                "ArticleIdList": [
                    type('MockStringElement', (), {
                        'attributes': {'IdType': 'doi'},
                        '__str__': lambda self: '10.1000/test123'
                    })(),
                    type('MockStringElement', (), {
                        'attributes': {'IdType': 'pmc'},
                        '__str__': lambda self: 'PMC1234567'
                    })()
                ]
            }
        }
        
        article = self.tool._parse_article_record(record)
        
        self.assertEqual(article.pmid, "12345678")
        self.assertEqual(article.title, "Complete Test Article")
        self.assertEqual(len(article.authors), 2)
        self.assertEqual(article.authors[0], "John Doe")
        self.assertEqual(article.authors[1], "Test Consortium")
        self.assertEqual(article.abstract, "Background: Test. Methods: Test. Results: Test.")
        self.assertEqual(article.journal, "Test Journal")
        self.assertEqual(article.publication_date, "2023-01-15")
        self.assertEqual(article.doi, "10.1000/test123")
        self.assertEqual(article.pmcid, "PMC1234567")
        self.assertEqual(len(article.keywords), 2)
        self.assertEqual(len(article.affiliations), 1)
    
    def test_parse_article_record_minimal(self):
        """Test parsing a minimal article record."""
        record = {
            "MedlineCitation": {
                "PMID": "12345678",
                "Article": {
                    "ArticleTitle": "Minimal Test Article",
                    "AuthorList": [],
                    "Journal": {"Title": "Test Journal"}
                }
            },
            "PubmedData": {"ArticleIdList": []}
        }
        
        article = self.tool._parse_article_record(record)
        
        self.assertEqual(article.pmid, "12345678")
        self.assertEqual(article.title, "Minimal Test Article")
        self.assertEqual(len(article.authors), 0)
        self.assertEqual(article.abstract, "")
        self.assertEqual(article.journal, "Test Journal")
        self.assertEqual(article.publication_date, "")
        self.assertEqual(article.doi, "")
        self.assertEqual(article.pmcid, "")
        self.assertEqual(len(article.keywords), 0)
        self.assertEqual(len(article.affiliations), 0)
    
    def test_extract_publication_date_formats(self):
        """Test different publication date formats."""
        # Test with year, month, and day
        article = {
            "Journal": {
                "JournalIssue": {
                    "PubDate": {
                        "Year": "2023",
                        "Month": "Jan",
                        "Day": "15"
                    }
                }
            }
        }
        result = self.tool._extract_publication_date(article)
        self.assertEqual(result, "2023-01-15")
        
        # Test with year and month only
        article = {
            "Journal": {
                "JournalIssue": {
                    "PubDate": {
                        "Year": "2023",
                        "Month": "Jan"
                    }
                }
            }
        }
        result = self.tool._extract_publication_date(article)
        self.assertEqual(result, "2023-01")
        
        # Test with year only
        article = {
            "Journal": {
                "JournalIssue": {
                    "PubDate": {
                        "Year": "2023"
                    }
                }
            }
        }
        result = self.tool._extract_publication_date(article)
        self.assertEqual(result, "2023")
        
        # Test with no date
        article = {"Journal": {"JournalIssue": {}}}
        result = self.tool._extract_publication_date(article)
        self.assertEqual(result, "")
    
    def test_month_to_number(self):
        """Test month name to number conversion."""
        self.assertEqual(self.tool._month_to_number("Jan"), 1)
        self.assertEqual(self.tool._month_to_number("Feb"), 2)
        self.assertEqual(self.tool._month_to_number("Dec"), 12)
        self.assertEqual(self.tool._month_to_number("1"), 1)
        self.assertEqual(self.tool._month_to_number("12"), 12)
        self.assertEqual(self.tool._month_to_number("Unknown"), 1)  # Default
    
    @patch('src.tools.enhanced_fulltext.EnhancedFullTextRetriever')
    @patch('src.tools.enhanced_fulltext.format_enhanced_fulltext')
    def test_get_pmc_full_text_success(self, mock_format, mock_retriever_class):
        """Test successful PMC full text retrieval."""
        mock_retriever = Mock()
        mock_retriever_class.return_value = mock_retriever
        
        # Mock the enhanced retriever to return structured data
        mock_retriever.get_full_text.return_value = {
            'source': 'PMC',
            'title': 'Test Article',
            'full_text': 'Full text content'
        }
        
        # Mock the formatting function
        mock_format.return_value = "=== FULL TEXT (PMC) ===\nTitle: Test Article\nFull text content"
        
        result = self.tool.get_pmc_full_text("PMC1234567")
        
        self.assertIsNotNone(result)
        self.assertIn("Test Article", result)
        self.assertIn("Full text content", result)
    
    @patch('src.tools.pubmed_search.Entrez')
    def test_get_pmc_full_text_empty_pmcid(self, mock_entrez):
        """Test PMC full text retrieval with empty PMCID."""
        result = self.tool.get_pmc_full_text("")
        self.assertIsNone(result)
        
        result = self.tool.get_pmc_full_text(None)
        self.assertIsNone(result)
    
    @patch('src.tools.enhanced_fulltext.EnhancedFullTextRetriever')
    def test_get_pmc_full_text_exception(self, mock_retriever_class):
        """Test PMC full text retrieval with exception."""
        mock_retriever = Mock()
        mock_retriever_class.return_value = mock_retriever
        
        # Mock the enhanced retriever to raise an exception
        mock_retriever.get_full_text.side_effect = Exception("PMC Error")
        
        result = self.tool.get_pmc_full_text("PMC1234567")
        self.assertIsNone(result)


class TestPubMedSearchFunctions(unittest.TestCase):
    """Test the module-level functions."""
    
    @patch('src.tools.pubmed_search.PubMedSearchTool')
    def test_search_pubmed_papers_success(self, mock_tool_class):
        """Test successful paper search."""
        # Mock the tool instance
        mock_tool = Mock()
        mock_tool_class.return_value = mock_tool
        
        # Mock search results
        mock_tool.search_pubmed.return_value = ["12345678", "23456789"]
        
        # Mock article details
        mock_articles = [
            PubMedArticle(
                pmid="12345678",
                title="First Article",
                authors=["John Doe"],
                abstract="First abstract",
                journal="Journal 1",
                publication_date="2023-01-15",
                doi="10.1000/test1",
                pmcid="PMC1234567",
                keywords=["keyword1"],
                affiliations=["University 1"]
            ),
            PubMedArticle(
                pmid="23456789",
                title="Second Article",
                authors=["Jane Smith"],
                abstract="Second abstract",
                journal="Journal 2",
                publication_date="2023-02-10",
                doi="10.1000/test2",
                pmcid="",
                keywords=["keyword2"],
                affiliations=["University 2"]
            )
        ]
        mock_tool.get_article_details.return_value = mock_articles
        
        # Call the function
        result = search_pubmed_papers("cancer", max_results=2)
        
        # Parse the result
        data = json.loads(result)
        
        # Verify the structure
        self.assertEqual(data["query"], "cancer")
        self.assertEqual(data["total_results"], 2)
        self.assertEqual(len(data["papers"]), 2)
        
        # Verify first paper
        paper1 = data["papers"][0]
        self.assertEqual(paper1["pmid"], "12345678")
        self.assertEqual(paper1["title"], "First Article")
        self.assertEqual(paper1["authors"], ["John Doe"])
        self.assertEqual(paper1["abstract"], "First abstract")
        self.assertEqual(paper1["journal"], "Journal 1")
        self.assertEqual(paper1["publication_date"], "2023-01-15")
        self.assertEqual(paper1["doi"], "10.1000/test1")
        self.assertEqual(paper1["pmcid"], "PMC1234567")
        self.assertEqual(paper1["keywords"], ["keyword1"])
        self.assertEqual(paper1["affiliations"], ["University 1"])
        
        # Verify second paper
        paper2 = data["papers"][1]
        self.assertEqual(paper2["pmid"], "23456789")
        self.assertEqual(paper2["title"], "Second Article")
    
    @patch('src.tools.pubmed_search.PubMedSearchTool')
    def test_search_pubmed_papers_no_results(self, mock_tool_class):
        """Test paper search with no results."""
        mock_tool = Mock()
        mock_tool_class.return_value = mock_tool
        mock_tool.search_pubmed.return_value = []
        
        result = search_pubmed_papers("nonexistent_term")
        data = json.loads(result)
        
        self.assertEqual(data["query"], "nonexistent_term")
        self.assertEqual(data["total_results"], 0)
        self.assertEqual(len(data["papers"]), 0)
    
    @patch('src.tools.pubmed_search.PubMedSearchTool')
    def test_search_pubmed_papers_exception(self, mock_tool_class):
        """Test paper search with exception."""
        mock_tool = Mock()
        mock_tool_class.return_value = mock_tool
        mock_tool.search_pubmed.side_effect = Exception("API Error")
        
        result = search_pubmed_papers("cancer")
        data = json.loads(result)
        
        self.assertIn("error", data)
        self.assertIn("PubMed search failed", data["error"])
        self.assertEqual(data["total_results"], 0)
        self.assertEqual(len(data["papers"]), 0)
    
    @patch('src.tools.pubmed_search.PubMedSearchTool')
    def test_search_pubmed_papers_with_full_text(self, mock_tool_class):
        """Test paper search with full text retrieval."""
        mock_tool = Mock()
        mock_tool_class.return_value = mock_tool
        
        mock_tool.search_pubmed.return_value = ["12345678"]
        mock_articles = [
            PubMedArticle(
                pmid="12345678",
                title="Test Article",
                authors=["John Doe"],
                abstract="Test abstract",
                journal="Test Journal",
                publication_date="2023-01-15",
                doi="10.1000/test",
                pmcid="PMC1234567",
                keywords=["test"],
                affiliations=["Test University"]
            )
        ]
        mock_tool.get_article_details.return_value = mock_articles
        mock_tool.get_enhanced_full_text.return_value = "Full text content"
        
        result = search_pubmed_papers("test", max_results=1, include_full_text=True)
        data = json.loads(result)
        
        self.assertEqual(len(data["papers"]), 1)
        self.assertEqual(data["papers"][0]["full_text"], "Full text content")
    
    @patch('src.tools.pubmed_search.PubMedSearchTool')
    def test_get_pubmed_paper_details_success(self, mock_tool_class):
        """Test successful individual paper retrieval."""
        mock_tool = Mock()
        mock_tool_class.return_value = mock_tool
        
        mock_articles = [
            PubMedArticle(
                pmid="12345678",
                title="Test Article",
                authors=["John Doe"],
                abstract="Test abstract",
                journal="Test Journal",
                publication_date="2023-01-15",
                doi="10.1000/test",
                pmcid="PMC1234567",
                keywords=["test"],
                affiliations=["Test University"]
            )
        ]
        mock_tool.get_article_details.return_value = mock_articles
        
        result = get_pubmed_paper_details("12345678")
        data = json.loads(result)
        
        self.assertEqual(data["pmid"], "12345678")
        self.assertEqual(data["title"], "Test Article")
        self.assertEqual(data["authors"], ["John Doe"])
        self.assertEqual(data["abstract"], "Test abstract")
        self.assertEqual(data["journal"], "Test Journal")
        self.assertEqual(data["publication_date"], "2023-01-15")
        self.assertEqual(data["doi"], "10.1000/test")
        self.assertEqual(data["pmcid"], "PMC1234567")
        self.assertEqual(data["keywords"], ["test"])
        self.assertEqual(data["affiliations"], ["Test University"])
    
    @patch('src.tools.pubmed_search.PubMedSearchTool')
    def test_get_pubmed_paper_details_no_results(self, mock_tool_class):
        """Test individual paper retrieval with no results."""
        mock_tool = Mock()
        mock_tool_class.return_value = mock_tool
        mock_tool.get_article_details.return_value = []
        
        result = get_pubmed_paper_details("12345678")
        data = json.loads(result)
        
        self.assertIn("error", data)
        self.assertIn("No article found", data["error"])
        self.assertEqual(data["pmid"], "12345678")
    
    @patch('src.tools.pubmed_search.PubMedSearchTool')
    def test_get_pubmed_paper_details_exception(self, mock_tool_class):
        """Test individual paper retrieval with exception."""
        mock_tool = Mock()
        mock_tool_class.return_value = mock_tool
        mock_tool.get_article_details.side_effect = Exception("API Error")
        
        result = get_pubmed_paper_details("12345678")
        data = json.loads(result)
        
        self.assertIn("error", data)
        self.assertIn("Failed to retrieve paper details", data["error"])
        self.assertEqual(data["pmid"], "12345678")
    
    def test_max_results_limit(self):
        """Test that max_results is properly limited."""
        with patch('src.tools.pubmed_search.PubMedSearchTool') as mock_tool_class:
            mock_tool = Mock()
            mock_tool_class.return_value = mock_tool
            mock_tool.search_pubmed.return_value = []
            
            # Test with max_results > 100
            search_pubmed_papers("test", max_results=150)
            
            # Should be limited to 100
            mock_tool.search_pubmed.assert_called_with("test", max_results=100)


if __name__ == '__main__':
    unittest.main()