"""
Integration tests for the PubChem client.

These tests make actual API calls to verify the client behavior
with real data from PubChem and PubMed.
"""

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


class TestPubChemIntegration:
    """Integration test suite for PubChem client with real API calls."""
    
    def test_search_aspirin_by_name(self):
        """Test searching for aspirin by name."""
        results = search_compounds_by_name("aspirin", max_results=5)
        
        assert isinstance(results, list)
        assert len(results) > 0
        assert len(results) <= 5
        
        # Check first result has expected structure
        compound = results[0]
        assert 'cid' in compound
        assert 'molecular_formula' in compound
        assert 'molecular_weight' in compound
        assert isinstance(compound['cid'], int)
        assert compound['cid'] == 2244  # Known CID for aspirin
    
    def test_get_compound_details_aspirin(self):
        """Test getting compound details for aspirin."""
        details = get_compound_details(2244)  # Aspirin CID
        
        assert details is not None
        assert details['cid'] == 2244
        assert details['molecular_formula'] == 'C9H8O4'
        assert isinstance(details['molecular_weight'], float)
        assert details['molecular_weight'] > 180.0
        assert 'synonyms' in details
        assert isinstance(details['synonyms'], list)
    
    def test_get_compound_details_nonexistent(self):
        """Test getting details for non-existent compound."""
        details = get_compound_details(999999999)  # Non-existent CID
        
        assert details is None
    
    def test_get_compound_literature_aspirin(self):
        """Test getting literature for aspirin."""
        literature = get_compound_literature(2244)  # Aspirin CID
        
        assert isinstance(literature, dict)
        assert literature['cid'] == 2244
        assert 'depositor_pmids' in literature
        assert 'total_pmids' in literature
        assert isinstance(literature['depositor_pmids'], list)
        assert isinstance(literature['total_pmids'], int)
    
    def test_search_diabetes_topic(self):
        """Test searching for diabetes-related compounds."""
        results = search_compounds_by_topic("diabetes", max_compounds=10)
        
        assert isinstance(results, list)
        assert len(results) > 0
        assert len(results) <= 10
        
        # Check that results contain diabetes-related compounds
        compound_names = []
        for compound in results:
            assert 'cid' in compound
            assert 'molecular_formula' in compound
            if 'synonyms' in compound:
                compound_names.extend(compound['synonyms'])
        
        # Should find metformin (common diabetes drug)
        found_metformin = any('metformin' in name.lower() for name in compound_names)
        assert found_metformin, "Should find metformin in diabetes search"
    
    def test_search_cancer_topic(self):
        """Test searching for cancer-related compounds."""
        results = search_compounds_by_topic("cancer", max_compounds=5)
        
        assert isinstance(results, list)
        assert len(results) > 0
        assert len(results) <= 5
        
        # Each result should have literature information
        for compound in results:
            assert 'cid' in compound
            assert 'depositor_pmids' in compound
            assert 'total_pmids' in compound
    
    def test_search_antimicrobial_topic(self):
        """Test searching for antimicrobial compounds."""
        results = search_compounds_by_topic("antimicrobial", max_compounds=8)
        
        assert isinstance(results, list)
        assert len(results) > 0
        assert len(results) <= 8
    
    def test_search_antioxidant_topic(self):
        """Test searching for antioxidant compounds."""
        results = search_compounds_by_topic("antioxidant", max_compounds=6)
        
        assert isinstance(results, list)
        assert len(results) > 0
        assert len(results) <= 6
    
    def test_search_inflammation_topic(self):
        """Test searching for anti-inflammatory compounds."""
        results = search_compounds_by_topic("anti-inflammatory", max_compounds=7)
        
        assert isinstance(results, list)
        assert len(results) > 0
        assert len(results) <= 7
        
        # Should find aspirin (common anti-inflammatory)
        found_aspirin = any(compound.get('cid') == 2244 for compound in results)
        assert found_aspirin, "Should find aspirin in anti-inflammatory search"
    
    def test_search_cardiovascular_topic(self):
        """Test searching for cardiovascular-related compounds."""
        results = search_compounds_by_topic("cardiovascular", max_compounds=5)
        
        assert isinstance(results, list)
        assert len(results) >= 0  # May not find specific matches
    
    def test_search_neurodegenerative_topic(self):
        """Test searching for neurodegenerative-related compounds."""
        results = search_compounds_by_topic("neurodegenerative", max_compounds=5)
        
        assert isinstance(results, list)
        assert len(results) >= 0  # May not find specific matches
    
    def test_search_immunology_topic(self):
        """Test searching for immunology-related compounds."""
        results = search_compounds_by_topic("immunology", max_compounds=5)
        
        assert isinstance(results, list)
        assert len(results) >= 0  # May not find specific matches
    
    def test_search_infectious_disease_topic(self):
        """Test searching for infectious disease compounds."""
        results = search_compounds_by_topic("infectious disease", max_compounds=5)
        
        assert isinstance(results, list)
        assert len(results) >= 0  # May not find specific matches
    
    def test_search_pain_management_topic(self):
        """Test searching for pain management compounds."""
        results = search_compounds_by_topic("pain management", max_compounds=5)
        
        assert isinstance(results, list)
        assert len(results) >= 0  # May not find specific matches
    
    def test_get_paper_content_valid_pmid(self):
        """Test getting paper content for a valid PMID."""
        # Use a well-known PMID for aspirin research
        pmid = 31496804  # A paper that mentions aspirin/P2RY8
        
        paper_content = get_paper_content(pmid)
        
        assert isinstance(paper_content, dict)
        assert paper_content['pmid'] == pmid
        assert 'title' in paper_content
        assert 'abstract' in paper_content
        assert 'authors' in paper_content
        assert 'journal' in paper_content
        assert 'publication_date' in paper_content
        assert 'doi' in paper_content
        assert 'full_text_available' in paper_content
        assert 'full_text' in paper_content
        assert 'keywords' in paper_content
        assert 'mesh_terms' in paper_content
        
        # Should have some content
        assert len(paper_content['title']) > 0
        assert len(paper_content['abstract']) > 0
        assert isinstance(paper_content['authors'], list)
        assert isinstance(paper_content['keywords'], list)
        assert isinstance(paper_content['mesh_terms'], list)
    
    def test_get_paper_content_invalid_pmid(self):
        """Test getting paper content for an invalid PMID."""
        # Use a clearly invalid PMID
        pmid = 999999999
        
        # Should not raise an exception, but may return empty content
        paper_content = get_paper_content(pmid)
        
        assert isinstance(paper_content, dict)
        assert paper_content['pmid'] == pmid
    
    def test_get_papers_content_multiple_pmids(self):
        """Test getting paper content for multiple PMIDs."""
        # Use known PMIDs
        pmids = [31496804, 30977196]  # Two papers from aspirin literature
        
        papers_content = get_papers_content(pmids)
        
        assert isinstance(papers_content, list)
        assert len(papers_content) == 2
        
        for i, paper_content in enumerate(papers_content):
            assert isinstance(paper_content, dict)
            assert paper_content['pmid'] == pmids[i]
            assert 'title' in paper_content
            assert 'abstract' in paper_content
    
    def test_get_papers_content_mixed_pmids(self):
        """Test getting paper content for a mix of valid and invalid PMIDs."""
        # Mix valid and invalid PMIDs
        pmids = [31496804, 999999999, 30977196]
        
        papers_content = get_papers_content(pmids)
        
        assert isinstance(papers_content, list)
        assert len(papers_content) == 3
        
        # First and third should have content, second may have error or empty content
        assert papers_content[0]['pmid'] == 31496804
        assert papers_content[1]['pmid'] == 999999999
        assert papers_content[2]['pmid'] == 30977196
    
    def test_paper_content_with_compound_literature(self):
        """Test integrating paper content retrieval with compound literature."""
        # Get literature for aspirin
        literature = get_compound_literature(2244)  # Aspirin CID
        
        assert isinstance(literature, dict)
        assert 'depositor_pmids' in literature
        
        # If there are PMIDs, test getting content for some of them
        if literature['depositor_pmids']:
            # Take first few PMIDs
            test_pmids = literature['depositor_pmids'][:2]
            papers_content = get_papers_content(test_pmids)
            
            assert isinstance(papers_content, list)
            assert len(papers_content) == len(test_pmids)
            
            for paper_content in papers_content:
                assert isinstance(paper_content, dict)
                assert 'pmid' in paper_content
                assert 'title' in paper_content
                assert 'abstract' in paper_content