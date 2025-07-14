"""Unit tests for PDB query tool.

This module contains comprehensive tests for the PDB querying functionality,
including mock responses and real-world use cases. Tests cover various
query types, error conditions, and edge cases.
"""

import json
import time
from unittest.mock import patch, MagicMock

import pytest

from src.tools.pdb_query import (
    pdb_search,
    pdb_get_info,
    pdb_sequence_search,
    pdb_structure_search,
    PDBQueryError,
    PDBQueryClient,
)


def _mock_response(payload: object, status: int = 200):
    """Utility to craft a mock HTTPResponse-like object for urlopen."""
    mock = MagicMock()
    mock.read.return_value = json.dumps(payload).encode() if payload else b""
    mock.getcode.return_value = status
    mock.__enter__ = MagicMock(return_value=mock)
    mock.__exit__ = MagicMock(return_value=None)
    return mock


class TestPDBQueryClient:
    """Tests for the PDB query client functionality."""

    def test_pdb_id_validation(self):
        """Test PDB ID format validation."""
        client = PDBQueryClient()
        
        # Valid PDB IDs
        valid_ids = ["1ABC", "2def", "3GHI", "4j5k"]
        for pdb_id in valid_ids:
            # Should not raise an exception
            try:
                client.get_entry_info(pdb_id)
            except PDBQueryError as e:
                if "Invalid PDB ID format" in str(e):
                    pytest.fail(f"Valid PDB ID {pdb_id} was rejected")
        
        # Invalid PDB IDs
        invalid_ids = ["ABC1", "12345", "AB", "1A2B3", "1a2b3c"]
        for pdb_id in invalid_ids:
            with pytest.raises(PDBQueryError, match="Invalid PDB ID format"):
                client.get_entry_info(pdb_id)

    def test_rate_limiting(self):
        """Test rate limiting functionality."""
        client = PDBQueryClient(reqs_per_sec=2)
        
        # Mock time to control rate limiting
        with patch("src.tools.pdb_query.time") as mock_time:
            mock_time.time.return_value = 1000.0
            
            # First request should go through immediately
            client._rate_limit()
            assert len(client._request_times) == 1
            
            # Second request should go through immediately
            client._rate_limit()
            assert len(client._request_times) == 2
            
            # Third request should trigger rate limiting
            mock_time.time.return_value = 1000.1  # 0.1 seconds later
            with patch("src.tools.pdb_query.time.sleep") as mock_sleep:
                client._rate_limit()
                mock_sleep.assert_called_once()

    def test_search_basic_text_query(self):
        """Test basic text search functionality."""
        search_payload = {
            "result_set": [
                {"identifier": "1ABC", "score": 0.95},
                {"identifier": "2DEF", "score": 0.87}
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(search_payload)):
            results = client.search(query_text="COVID-19")
            
            assert len(results) == 2
            assert results[0]["pdb_id"] == "1ABC"
            assert results[1]["pdb_id"] == "2DEF"

    def test_search_organism_filter(self):
        """Test organism-based search."""
        search_payload = {
            "result_set": [
                {"identifier": "1HEM", "score": 0.95},
                {"identifier": "2HEM", "score": 0.87}
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(search_payload)):
            results = client.search(organism="Homo sapiens")
            
            assert len(results) == 2
            assert results[0]["pdb_id"] == "1HEM"

    def test_search_method_filter(self):
        """Test experimental method filter."""
        search_payload = {
            "result_set": [
                {"identifier": "1XRY", "score": 0.95}
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(search_payload)):
            results = client.search(method="X-RAY DIFFRACTION")
            
            assert len(results) == 1
            assert results[0]["pdb_id"] == "1XRY"

    def test_search_resolution_filter(self):
        """Test resolution-based filtering."""
        search_payload = {
            "result_set": [
                {"identifier": "1HIR", "score": 0.95}
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(search_payload)):
            results = client.search(resolution_max=2.0)
            
            assert len(results) == 1
            assert results[0]["pdb_id"] == "1HIR"

    def test_search_combined_filters(self):
        """Test combining multiple search criteria."""
        search_payload = {
            "result_set": [
                {"identifier": "1COM", "score": 0.95}
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(search_payload)):
            results = client.search(
                organism="Homo sapiens",
                method="X-RAY DIFFRACTION",
                resolution_max=2.0
            )
            
            assert len(results) == 1
            assert results[0]["pdb_id"] == "1COM"

    def test_get_entry_info_success(self):
        """Test successful entry information retrieval."""
        entry_payload = {
            "rcsb_id": "1ABC",
            "struct": {
                "title": "Test Structure",
                "pdbx_descriptor": "Test protein"
            },
            "rcsb_entry_info": {
                "resolution_combined": [1.8],
                "experimental_method": "X-RAY DIFFRACTION"
            }
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(entry_payload)):
            result = client.get_entry_info("1ABC")
            
            assert result["rcsb_id"] == "1ABC"
            assert result["struct"]["title"] == "Test Structure"
            assert result["rcsb_entry_info"]["resolution_combined"] == [1.8]

    def test_get_entry_info_not_found(self):
        """Test entry not found handling."""
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(None, 404)):
            result = client.get_entry_info("9ZZZ")
            
            assert result is None

    def test_sequence_search_success(self):
        """Test sequence similarity search."""
        search_payload = {
            "result_set": [
                {
                    "identifier": "1HEM",
                    "score": 0.95,
                    "services": [{
                        "service_type": "sequence",
                        "nodes": [{"identity": 0.98, "evalue": 1e-50}]
                    }]
                }
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(search_payload)):
            results = client.sequence_search("MVLSPADKTNVKAAW")
            
            assert len(results) == 1
            assert results[0]["pdb_id"] == "1HEM"
            assert results[0]["score"] == 0.95
            assert results[0]["identity"] == 0.98

    def test_sequence_search_empty_sequence(self):
        """Test sequence search with empty sequence."""
        client = PDBQueryClient()
        
        with pytest.raises(PDBQueryError, match="Sequence cannot be empty"):
            client.sequence_search("")

    def test_structure_search_success(self):
        """Test structure similarity search."""
        search_payload = {
            "result_set": [
                {"identifier": "2ABC", "score": 0.92},
                {"identifier": "3DEF", "score": 0.85}
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(search_payload)):
            results = client.structure_search("1ABC")
            
            assert len(results) == 2
            assert results[0]["pdb_id"] == "2ABC"
            assert results[0]["score"] == 0.92

    def test_empty_search_results(self):
        """Test handling of empty search results."""
        empty_payload = {"result_set": []}
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(empty_payload)):
            results = client.search(query_text="nonexistent")
            
            assert results == []

    def test_http_error_handling(self):
        """Test HTTP error handling."""
        client = PDBQueryClient()
        
        # Mock HTTP error
        mock_error = MagicMock()
        mock_error.code = 500
        mock_error.reason = "Internal Server Error"
        
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  side_effect=Exception("HTTP error 500: Internal Server Error")):
            with pytest.raises(PDBQueryError):
                client.search(query_text="test")

    def test_rate_limit_retry(self):
        """Test rate limit retry behavior."""
        client = PDBQueryClient()
        
        # Mock 429 error followed by success
        import urllib.error
        mock_error = urllib.error.HTTPError(
            url="http://test.com",
            code=429,
            msg="Too Many Requests",
            hdrs={"Retry-After": "1"},
            fp=None
        )
        
        success_payload = {"result_set": [{"identifier": "1ABC"}]}
        
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  side_effect=[mock_error, _mock_response(success_payload)]):
            with patch("src.tools.pdb_query.time.sleep") as mock_sleep:
                results = client.search(query_text="test")
                
                assert len(results) == 1
                assert results[0]["pdb_id"] == "1ABC"
                mock_sleep.assert_called_once_with(1.0)

    def test_invalid_parameters(self):
        """Test validation of invalid parameters."""
        client = PDBQueryClient()
        
        # Test empty search
        with pytest.raises(PDBQueryError, match="At least one search parameter must be provided"):
            client.search()

    # ===== Real-world versatility tests =====

    def test_covid19_related_structures(self):
        """Test search for COVID-19 related structures."""
        covid_payload = {
            "result_set": [
                {"identifier": "6M0J", "score": 0.95},  # SARS-CoV-2 main protease
                {"identifier": "6LU7", "score": 0.93},  # SARS-CoV-2 main protease
                {"identifier": "6VSB", "score": 0.91},  # SARS-CoV-2 spike protein
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(covid_payload)):
            results = client.search(query_text="COVID-19")
            
            assert len(results) == 3
            assert results[0]["pdb_id"] == "6M0J"
            assert results[1]["pdb_id"] == "6LU7"
            assert results[2]["pdb_id"] == "6VSB"

    def test_human_protein_structures(self):
        """Test search for human protein structures."""
        human_payload = {
            "result_set": [
                {"identifier": "1HEM", "score": 0.95},  # Hemoglobin
                {"identifier": "1INS", "score": 0.93},  # Insulin
                {"identifier": "1P53", "score": 0.91},  # p53
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(human_payload)):
            results = client.search(organism="Homo sapiens", limit=50)
            
            assert len(results) == 3
            assert results[0]["pdb_id"] == "1HEM"

    def test_high_resolution_xray_structures(self):
        """Test search for high-resolution X-ray structures."""
        xray_payload = {
            "result_set": [
                {"identifier": "1XRY", "score": 0.95},
                {"identifier": "2XRY", "score": 0.93},
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(xray_payload)):
            results = client.search(
                method="X-RAY DIFFRACTION",
                resolution_max=1.5
            )
            
            assert len(results) == 2
            assert results[0]["pdb_id"] == "1XRY"

    def test_nmr_structures(self):
        """Test search for NMR structures."""
        nmr_payload = {
            "result_set": [
                {"identifier": "1NMR", "score": 0.95},
                {"identifier": "2NMR", "score": 0.93},
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(nmr_payload)):
            results = client.search(method="NMR")
            
            assert len(results) == 2
            assert results[0]["pdb_id"] == "1NMR"

    def test_cryo_em_structures(self):
        """Test search for Cryo-EM structures."""
        cryo_payload = {
            "result_set": [
                {"identifier": "1CRY", "score": 0.95},
                {"identifier": "2CRY", "score": 0.93},
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(cryo_payload)):
            results = client.search(method="ELECTRON MICROSCOPY")
            
            assert len(results) == 2
            assert results[0]["pdb_id"] == "1CRY"

    def test_enzyme_structures(self):
        """Test search for enzyme structures."""
        enzyme_payload = {
            "result_set": [
                {"identifier": "1ENZ", "score": 0.95},
                {"identifier": "2ENZ", "score": 0.93},
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(enzyme_payload)):
            results = client.search(query_text="enzyme")
            
            assert len(results) == 2
            assert results[0]["pdb_id"] == "1ENZ"

    def test_membrane_protein_structures(self):
        """Test search for membrane protein structures."""
        membrane_payload = {
            "result_set": [
                {"identifier": "1MEM", "score": 0.95},
                {"identifier": "2MEM", "score": 0.93},
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(membrane_payload)):
            results = client.search(query_text="membrane protein")
            
            assert len(results) == 2
            assert results[0]["pdb_id"] == "1MEM"

    def test_antibody_structures(self):
        """Test search for antibody structures."""
        antibody_payload = {
            "result_set": [
                {"identifier": "1AB1", "score": 0.95},
                {"identifier": "2AB2", "score": 0.93},
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(antibody_payload)):
            results = client.search(query_text="antibody")
            
            assert len(results) == 2
            assert results[0]["pdb_id"] == "1AB1"

    def test_kinase_structures(self):
        """Test search for kinase structures."""
        kinase_payload = {
            "result_set": [
                {"identifier": "1KIN", "score": 0.95},
                {"identifier": "2KIN", "score": 0.93},
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(kinase_payload)):
            results = client.search(query_text="kinase")
            
            assert len(results) == 2
            assert results[0]["pdb_id"] == "1KIN"

    def test_recent_structures(self):
        """Test search for recently deposited structures."""
        recent_payload = {
            "result_set": [
                {"identifier": "8ABC", "score": 0.95},
                {"identifier": "8DEF", "score": 0.93},
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(recent_payload)):
            results = client.search(date_from="2023-01-01")
            
            assert len(results) == 2
            assert results[0]["pdb_id"] == "8ABC"

    def test_drug_target_structures(self):
        """Test search for drug target structures."""
        drug_payload = {
            "result_set": [
                {"identifier": "1DRG", "score": 0.95},
                {"identifier": "2DRG", "score": 0.93},
            ]
        }
        
        client = PDBQueryClient()
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(drug_payload)):
            results = client.search(query_text="drug target")
            
            assert len(results) == 2
            assert results[0]["pdb_id"] == "1DRG"


class TestPDBQueryFunctions:
    """Test the high-level PDB query functions."""

    def test_pdb_search_function(self):
        """Test the pdb_search function."""
        search_payload = {
            "result_set": [
                {"identifier": "1ABC", "score": 0.95}
            ]
        }
        
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(search_payload)):
            results = pdb_search(query_text="test")
            
            assert len(results) == 1
            assert results[0]["pdb_id"] == "1ABC"

    def test_pdb_get_info_function(self):
        """Test the pdb_get_info function."""
        info_payload = {
            "rcsb_id": "1ABC",
            "struct": {"title": "Test Structure"}
        }
        
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(info_payload)):
            result = pdb_get_info("1ABC")
            
            assert result["rcsb_id"] == "1ABC"
            assert result["struct"]["title"] == "Test Structure"

    def test_pdb_sequence_search_function(self):
        """Test the pdb_sequence_search function."""
        search_payload = {
            "result_set": [
                {
                    "identifier": "1HEM",
                    "score": 0.95,
                    "services": [{
                        "service_type": "sequence",
                        "nodes": [{"identity": 0.98}]
                    }]
                }
            ]
        }
        
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(search_payload)):
            results = pdb_sequence_search("MVLSPADKTNVKAAW")
            
            assert len(results) == 1
            assert results[0]["pdb_id"] == "1HEM"

    def test_pdb_structure_search_function(self):
        """Test the pdb_structure_search function."""
        search_payload = {
            "result_set": [
                {"identifier": "2ABC", "score": 0.92}
            ]
        }
        
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  return_value=_mock_response(search_payload)):
            results = pdb_structure_search("1ABC")
            
            assert len(results) == 1
            assert results[0]["pdb_id"] == "2ABC"
            assert results[0]["score"] == 0.92

    def test_error_propagation(self):
        """Test that errors are properly propagated from client to functions."""
        with patch("src.tools.pdb_query.urllib.request.urlopen", 
                  side_effect=Exception("Network error")):
            with pytest.raises(PDBQueryError):
                pdb_search(query_text="test")

    def test_parameter_validation(self):
        """Test parameter validation in high-level functions."""
        # Test invalid PDB ID
        with pytest.raises(PDBQueryError, match="Invalid PDB ID format"):
            pdb_get_info("invalid")
        
        # Test empty sequence
        with pytest.raises(PDBQueryError, match="Sequence cannot be empty"):
            pdb_sequence_search("")
        
        # Test invalid structure search PDB ID
        with pytest.raises(PDBQueryError, match="Invalid PDB ID format"):
            pdb_structure_search("invalid")