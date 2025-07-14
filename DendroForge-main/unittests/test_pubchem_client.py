"""
Unit tests for the PubChem client.

These tests use mocked HTTP responses to verify the client behavior
without making actual API calls.
"""

import json
import urllib.error
from unittest.mock import Mock, patch

import pytest

from src.tools.pubchem_client import (
    PubChemClient,
    search_compounds_by_name,
    get_compound_literature,
    get_compound_details,
    search_compounds_by_topic,
    get_paper_content,
    get_papers_content,
)


def _mock_response(data):
    """Create a mock HTTP response object."""
    mock_response = Mock()
    if isinstance(data, dict):
        mock_response.read.return_value = json.dumps(data).encode()
    else:
        mock_response.read.return_value = data.encode()
    return mock_response


class TestPubChemClient:
    """Test suite for PubChem client functionality."""

    def test_search_compounds_by_name_success(self):
        """search_compounds_by_name returns compounds when API succeeds."""
        # Mock CID search response
        cid_response = {
            "IdentifierList": {
                "CID": [2244, 5090]
            }
        }

        # Mock compound details responses - need multiple calls now
        # First compound (2244) - basic properties
        basic_properties_2244 = {
            "PropertyTable": {
                "Properties": [{
                    "CID": 2244,
                    "MolecularFormula": "C9H8O4",
                    "MolecularWeight": "180.16"
                }]
            }
        }
        
        # Additional properties for 2244
        additional_properties_2244 = {
            "PropertyTable": {
                "Properties": [{
                    "CID": 2244,
                    "IUPACName": "2-acetyloxybenzoic acid",
                    "SMILES": "CC(=O)OC1=CC=CC=C1C(=O)O"
                }]
            }
        }

        # Synonyms for 2244
        synonyms_response_2244 = {
            "InformationList": {
                "Information": [{
                    "CID": 2244,
                    "Synonym": ["Aspirin", "Acetylsalicylic acid", "ASA"]
                }]
            }
        }

        with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=[
            _mock_response(cid_response),
            _mock_response(basic_properties_2244),
            _mock_response(additional_properties_2244),
            _mock_response(synonyms_response_2244),
        ]):
            results = search_compounds_by_name("aspirin", max_results=1)

        assert len(results) == 1
        assert results[0]["cid"] == 2244
        assert results[0]["molecular_formula"] == "C9H8O4"
        assert results[0]["molecular_weight"] == 180.16
        assert "Aspirin" in results[0]["synonyms"]

    def test_search_compounds_by_name_not_found(self):
        """search_compounds_by_name returns empty list when compound not found."""
        # Mock 404 response
        http_error = urllib.error.HTTPError(
            url="test", code=404, msg="Not Found", hdrs=None, fp=None
        )

        with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=http_error):
            results = search_compounds_by_name("nonexistentcompound")

        assert len(results) == 0

    def test_get_compound_details_success(self):
        """get_compound_details returns detailed compound information."""
        # Mock basic properties response
        basic_properties = {
            "PropertyTable": {
                "Properties": [{
                    "CID": 2244,
                    "MolecularFormula": "C9H8O4",
                    "MolecularWeight": "180.16"
                }]
            }
        }
        
        # Mock additional properties response
        additional_properties = {
            "PropertyTable": {
                "Properties": [{
                    "CID": 2244,
                    "IUPACName": "2-acetyloxybenzoic acid",
                    "SMILES": "CC(=O)OC1=CC=CC=C1C(=O)O"
                }]
            }
        }

        # Mock synonyms response
        synonyms_response = {
            "InformationList": {
                "Information": [{
                    "CID": 2244,
                    "Synonym": ["Aspirin", "Acetylsalicylic acid"]
                }]
            }
        }

        with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=[
            _mock_response(basic_properties),
            _mock_response(additional_properties),
            _mock_response(synonyms_response),
        ]):
            details = get_compound_details(2244)

        assert details is not None
        assert details["cid"] == 2244
        assert details["molecular_formula"] == "C9H8O4"
        assert details["molecular_weight"] == 180.16
        assert details["iupac_name"] == "2-acetyloxybenzoic acid"
        assert details["canonical_smiles"] == "CC(=O)OC1=CC=CC=C1C(=O)O"
        assert "Aspirin" in details["synonyms"]

    def test_get_compound_details_not_found(self):
        """get_compound_details returns None when compound not found."""
        # Mock 404 response
        http_error = urllib.error.HTTPError(
            url="test", code=404, msg="Not Found", hdrs=None, fp=None
        )

        with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=http_error):
            details = get_compound_details(999999)

        assert details is None

    def test_get_compound_literature_success(self):
        """get_compound_literature returns literature information."""
        literature_response = {
            "InformationList": {
                "Information": [{
                    "PubMedID": [12345678, 87654321, 11223344]
                }]
            }
        }

        with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=[
            _mock_response(literature_response),
        ]):
            literature = get_compound_literature(2244)

        assert literature["cid"] == 2244
        assert len(literature["depositor_pmids"]) == 3
        assert 12345678 in literature["depositor_pmids"]
        assert literature["total_pmids"] == 3

    def test_get_compound_literature_not_found(self):
        """get_compound_literature handles missing literature gracefully."""
        # Mock 404 response
        http_error = urllib.error.HTTPError(
            url="test", code=404, msg="Not Found", hdrs=None, fp=None
        )

        with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=http_error):
            literature = get_compound_literature(999999)

        assert literature["cid"] == 999999
        assert len(literature["depositor_pmids"]) == 0
        assert literature["total_pmids"] == 0

    def test_client_error_handling(self):
        """Client properly handles various HTTP errors."""
        client = PubChemClient()

        # Test 400 error
        http_400 = urllib.error.HTTPError(
            url="test", code=400, msg="Bad Request", hdrs=None, fp=None
        )

        with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=http_400):
            with pytest.raises(RuntimeError, match="Bad request to PubChem API"):
                client.perform_request("/test")

        # Test 503 error
        http_503 = urllib.error.HTTPError(
            url="test", code=503, msg="Service Unavailable", hdrs=None, fp=None
        )

        with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=http_503):
            with pytest.raises(RuntimeError, match="PubChem service temporarily unavailable"):
                client.perform_request("/test")

    def test_search_compounds_by_topic_diabetes(self):
        """Test searching for diabetes-related compounds."""
        # Mock the search for "metformin" (diabetes topic triggers this)
        cid_response = {
            "IdentifierList": {
                "CID": [4917]
            }
        }

        # Mock compound details for metformin
        basic_properties = {
            "PropertyTable": {
                "Properties": [{
                    "CID": 4917,
                    "MolecularFormula": "C4H11N5",
                    "MolecularWeight": "129.16"
                }]
            }
        }
        
        additional_properties = {
            "PropertyTable": {
                "Properties": [{
                    "CID": 4917,
                    "IUPACName": "3-(diaminomethylidene)-1,1-dimethylguanidine",
                    "SMILES": "CN(C)C(=NC(=N)N)N"
                }]
            }
        }

        synonyms_response = {
            "InformationList": {
                "Information": [{
                    "Synonym": ["Metformin", "Glucophage"]
                }]
            }
        }

        literature_response = {
            "InformationList": {
                "Information": [{
                    "PubMedID": [19876543, 20987654]
                }]
            }
        }

        with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=[
            _mock_response(cid_response),
            _mock_response(basic_properties),
            _mock_response(additional_properties),
            _mock_response(synonyms_response),
            _mock_response(literature_response),
        ]):
            results = search_compounds_by_topic("diabetes", max_compounds=5)

        assert len(results) == 1
        assert results[0]["cid"] == 4917
        assert "Metformin" in results[0]["synonyms"]
        # Check that basic compound information is present
        assert "molecular_formula" in results[0]
        assert results[0]["molecular_formula"] == "C4H11N5"

    def test_search_compounds_by_topic_cancer(self):
        """Test searching for cancer-related compounds."""
        # Mock the search for "doxorubicin" (cancer topic triggers this)
        cid_response = {
            "IdentifierList": {
                "CID": [31703]
            }
        }

        # Mock compound details
        basic_properties = {
            "PropertyTable": {
                "Properties": [{
                    "CID": 31703,
                    "MolecularFormula": "C27H29NO11",
                    "MolecularWeight": "543.52"
                }]
            }
        }
        
        additional_properties = {
            "PropertyTable": {
                "Properties": [{
                    "CID": 31703,
                    "IUPACName": "doxorubicin",
                    "SMILES": "COC1=C(C=C2C(=C1)C(=O)C3=C(C2=O)C=CC=C3O)O"
                }]
            }
        }

        synonyms_response = {
            "InformationList": {
                "Information": [{
                    "Synonym": ["Doxorubicin", "Adriamycin"]
                }]
            }
        }

        literature_response = {
            "InformationList": {
                "Information": [{
                    "PubMedID": [12345678]
                }]
            }
        }

        with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=[
            _mock_response(cid_response),
            _mock_response(basic_properties),
            _mock_response(additional_properties),
            _mock_response(synonyms_response),
            _mock_response(literature_response),
        ]):
            results = search_compounds_by_topic("cancer", max_compounds=5)

        assert len(results) == 1
        assert results[0]["cid"] == 31703
        assert "Doxorubicin" in results[0]["synonyms"]

    def test_rate_limiting_behavior(self):
        """Test that rate limiting works correctly."""
        client = PubChemClient(reqs_per_sec=2)

        # Mock time to control rate limiting
        with patch("src.tools.pubchem_client.time") as mock_time:
            mock_time.time.return_value = 1000.0  # Fixed time

            # Mock successful responses for basic properties only
            mock_response_data_1 = {
                "PropertyTable": {
                    "Properties": [{
                        "CID": 2244,
                        "MolecularFormula": "C9H8O4",
                        "MolecularWeight": "180.16"
                    }]
                }
            }

            mock_response_data_2 = {
                "PropertyTable": {
                    "Properties": [{
                        "CID": 5090,
                        "MolecularFormula": "C6H12O6",
                        "MolecularWeight": "180.16"
                    }]
                }
            }

            # Mock additional properties (empty responses)
            empty_properties = {
                "PropertyTable": {
                    "Properties": [{
                        "CID": 2244,
                        "IUPACName": "",
                        "SMILES": ""
                    }]
                }
            }

            # Mock 404 response for synonym requests
            http_error = urllib.error.HTTPError(
                url="test", code=404, msg="Not Found", hdrs=None, fp=None
            )

            with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=[
                _mock_response(mock_response_data_1),  # basic properties for 2244
                _mock_response(empty_properties),       # additional properties for 2244
                http_error,                            # synonyms for 2244
                _mock_response(mock_response_data_2),  # basic properties for 5090
                _mock_response(empty_properties),       # additional properties for 5090
                http_error,                            # synonyms for 5090
            ]):
                result1 = client.get_compound_details(2244)
                result2 = client.get_compound_details(5090)

                assert result1["cid"] == 2244
                assert result2["cid"] == 5090

    def test_error_handling_network_issues(self):
        """Test handling of network-related errors."""
        client = PubChemClient()

        # Test URLError (network issue)
        url_error = urllib.error.URLError("Network unreachable")

        with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=url_error):
            with pytest.raises(RuntimeError, match="Network error contacting PubChem"):
                client.perform_request("/test")

    def test_json_parsing_fallback(self):
        """Test that client falls back to text when JSON parsing fails."""
        client = PubChemClient()

        # Mock response with invalid JSON
        mock_response = Mock()
        mock_response.read.return_value = b"Invalid JSON content"

        with patch("src.tools.pubchem_client.urllib.request.urlopen", return_value=mock_response):
            result = client.perform_request("/test")

        assert result == "Invalid JSON content"

    def test_get_paper_content_success(self):
        """Test getting paper content for a valid PMID."""
        # Mock PubMed efetch response
        pubmed_xml = '''<?xml version="1.0" ?>
        <PubmedArticleSet>
            <PubmedArticle>
                <MedlineCitation>
                    <Article>
                        <ArticleTitle>Test Article Title</ArticleTitle>
                        <Abstract>
                            <AbstractText>This is a test abstract content.</AbstractText>
                        </Abstract>
                        <AuthorList>
                            <Author>
                                <LastName>Smith</LastName>
                                <ForeName>John</ForeName>
                            </Author>
                            <Author>
                                <LastName>Doe</LastName>
                                <ForeName>Jane</ForeName>
                            </Author>
                        </AuthorList>
                        <Journal>
                            <Title>Test Journal</Title>
                        </Journal>
                        <PubDate>
                            <Year>2023</Year>
                            <Month>Jan</Month>
                        </PubDate>
                        <ELocationID EIdType="doi">10.1000/test.doi</ELocationID>
                    </Article>
                    <KeywordList>
                        <Keyword>test keyword</Keyword>
                        <Keyword>sample</Keyword>
                    </KeywordList>
                    <MeshHeadingList>
                        <MeshHeading>
                            <DescriptorName>Test MeSH Term</DescriptorName>
                        </MeshHeading>
                    </MeshHeadingList>
                </MedlineCitation>
            </PubmedArticle>
        </PubmedArticleSet>'''

        with patch("src.tools.pubchem_client.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value = _mock_response(pubmed_xml)
            
            paper_content = get_paper_content(12345)
            
            assert paper_content['pmid'] == 12345
            assert paper_content['title'] == "Test Article Title"
            assert "test abstract content" in paper_content['abstract']
            assert len(paper_content['authors']) == 2
            assert "John Smith" in paper_content['authors']
            assert "Jane Doe" in paper_content['authors']
            assert paper_content['journal'] == "Test Journal"
            assert paper_content['publication_date'] == "Jan 2023"
            assert paper_content['doi'] == "10.1000/test.doi"
            assert "test keyword" in paper_content['keywords']
            assert "Test MeSH Term" in paper_content['mesh_terms']

    def test_get_paper_content_invalid_pmid(self):
        """Test getting paper content for an invalid PMID."""
        # Mock 404 response for invalid PMID
        http_error = urllib.error.HTTPError(
            url="test", code=404, msg="Not Found", hdrs=None, fp=None
        )
        
        with patch("src.tools.pubchem_client.urllib.request.urlopen", side_effect=http_error):
            paper_content = get_paper_content(999999999)
            
            assert paper_content['pmid'] == 999999999
            assert paper_content['title'] == ''
            assert paper_content['abstract'] == ''
            assert paper_content['authors'] == []

    def test_get_papers_content_multiple_pmids(self):
        """Test getting paper content for multiple PMIDs."""
        # Mock responses for two PMIDs
        pubmed_xml1 = '''<?xml version="1.0" ?>
        <PubmedArticleSet>
            <PubmedArticle>
                <MedlineCitation>
                    <Article>
                        <ArticleTitle>First Paper Title</ArticleTitle>
                        <Abstract>
                            <AbstractText>First abstract content.</AbstractText>
                        </Abstract>
                    </Article>
                </MedlineCitation>
            </PubmedArticle>
        </PubmedArticleSet>'''
        
        pubmed_xml2 = '''<?xml version="1.0" ?>
        <PubmedArticleSet>
            <PubmedArticle>
                <MedlineCitation>
                    <Article>
                        <ArticleTitle>Second Paper Title</ArticleTitle>
                        <Abstract>
                            <AbstractText>Second abstract content.</AbstractText>
                        </Abstract>
                    </Article>
                </MedlineCitation>
            </PubmedArticle>
        </PubmedArticleSet>'''

        def mock_side_effect(*args, **kwargs):
            url = str(args[0].get_full_url())
            if "12345" in url and "efetch" in url:
                return _mock_response(pubmed_xml1)
            elif "67890" in url and "efetch" in url:
                return _mock_response(pubmed_xml2)
            else:
                # For PMC link requests, return 404 (no full text available)
                raise urllib.error.HTTPError(None, 404, "Not Found", None, None)

        with patch("src.tools.pubchem_client.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = mock_side_effect
            
            papers_content = get_papers_content([12345, 67890])
            
            assert len(papers_content) == 2
            assert papers_content[0]['pmid'] == 12345
            assert papers_content[0]['title'] == "First Paper Title"
            assert papers_content[1]['pmid'] == 67890
            assert papers_content[1]['title'] == "Second Paper Title"

    def test_get_papers_content_with_errors(self):
        """Test getting paper content with some errors."""
        # Mock first successful, second fails
        pubmed_xml = '''<?xml version="1.0" ?>
        <PubmedArticleSet>
            <PubmedArticle>
                <MedlineCitation>
                    <Article>
                        <ArticleTitle>Successful Paper</ArticleTitle>
                        <Abstract>
                            <AbstractText>Success abstract.</AbstractText>
                        </Abstract>
                    </Article>
                </MedlineCitation>
            </PubmedArticle>
        </PubmedArticleSet>'''

        def mock_side_effect(*args, **kwargs):
            url = str(args[0].get_full_url())
            if "12345" in url and "efetch" in url:
                return _mock_response(pubmed_xml)
            else:
                raise urllib.error.HTTPError(None, 404, "Not Found", None, None)

        with patch("src.tools.pubchem_client.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = mock_side_effect
            
            papers_content = get_papers_content([12345, 999999])
            
            assert len(papers_content) == 2
            assert papers_content[0]['pmid'] == 12345
            assert papers_content[0]['title'] == "Successful Paper"
            assert papers_content[1]['pmid'] == 999999
            # For the failed case, we expect empty fields but no error field
            assert papers_content[1]['title'] == ''
            assert papers_content[1]['abstract'] == ''

    def test_parse_pubmed_xml_comprehensive(self):
        """Test comprehensive XML parsing."""
        client = PubChemClient()
        
        complex_xml = '''<?xml version="1.0" ?>
        <PubmedArticleSet>
            <PubmedArticle>
                <MedlineCitation>
                    <Article>
                        <ArticleTitle>Complex Article with <i>Formatting</i></ArticleTitle>
                        <Abstract>
                            <AbstractText Label="BACKGROUND">Background text here.</AbstractText>
                            <AbstractText Label="METHODS">Methods text here.</AbstractText>
                            <AbstractText Label="RESULTS">Results text here.</AbstractText>
                        </Abstract>
                        <AuthorList>
                            <Author>
                                <LastName>Johnson</LastName>
                                <ForeName>Michael</ForeName>
                            </Author>
                        </AuthorList>
                        <Journal>
                            <Title>Advanced Research Journal</Title>
                        </Journal>
                        <PubDate>
                            <Year>2023</Year>
                            <Month>December</Month>
                        </PubDate>
                        <ELocationID EIdType="doi">10.1000/advanced.doi</ELocationID>
                    </Article>
                    <KeywordList>
                        <Keyword>advanced research</Keyword>
                        <Keyword>methodology</Keyword>
                    </KeywordList>
                    <MeshHeadingList>
                        <MeshHeading>
                            <DescriptorName>Research Design</DescriptorName>
                        </MeshHeading>
                        <MeshHeading>
                            <DescriptorName>Data Analysis</DescriptorName>
                        </MeshHeading>
                    </MeshHeadingList>
                </MedlineCitation>
            </PubmedArticle>
        </PubmedArticleSet>'''
        
        parsed_info = client._parse_pubmed_xml(complex_xml)
        
        assert parsed_info['title'] == "Complex Article with <i>Formatting</i>"
        assert "Background text here" in parsed_info['abstract']
        assert "Methods text here" in parsed_info['abstract']
        assert "Results text here" in parsed_info['abstract']
        assert len(parsed_info['authors']) == 1
        assert "Michael Johnson" in parsed_info['authors']
        assert parsed_info['journal'] == "Advanced Research Journal"
        assert parsed_info['publication_date'] == "December 2023"
        assert parsed_info['doi'] == "10.1000/advanced.doi"
        assert len(parsed_info['keywords']) == 2
        assert "advanced research" in parsed_info['keywords']
        assert len(parsed_info['mesh_terms']) == 2
        assert "Research Design" in parsed_info['mesh_terms']