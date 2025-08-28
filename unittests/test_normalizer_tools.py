"""
Unit tests for normalizer tools.

This module contains comprehensive tests for the normalizer tools
and related functionality for metadata normalization.
"""

import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from src.tools.normalizer_tools import (
    get_ontology_mapping,
    get_available_ontologies,
    semantic_search_ontology,
    normalize_candidate_value,
    normalize_curation_result,
    load_curation_result_from_file,
    save_normalization_result,
    normalize_candidates_file,
    NormalizationError,
)
from src.models import (
    ExtractedCandidate,
    CurationResult,
    OntologyMatch,
    NormalizedCandidate,
    NormalizationResult,
)


class TestNormalizerTools:
    """Test class for normalizer tools."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.session_dir = Path(self.temp_dir)

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_get_ontology_mapping(self):
        """Test that ontology mapping returns expected field mappings."""
        mapping = get_ontology_mapping()

        assert isinstance(mapping, dict)
        assert "disease" in mapping
        assert "tissue" in mapping
        assert "organ" in mapping

        # Test disease mapping
        assert "mondo" in mapping["disease"]
        assert "efo" in mapping["disease"]

        # Test tissue mapping
        assert "uberon" in mapping["tissue"]
        assert "efo" in mapping["tissue"]

    @patch("src.tools.normalizer_tools.Path")
    def test_get_available_ontologies(self, mock_path):
        """Test getting available ontologies information."""
        # Mock the Path constructor and its chain of operations
        mock_file_path = Mock()
        mock_normalization_dir = Mock()
        mock_dict_dir = Mock()

        # Set up the chain: Path(__file__).parent.parent / "normalization" / "dictionaries"
        mock_path.return_value.parent.parent = mock_normalization_dir
        mock_normalization_dir.__truediv__ = Mock(return_value=mock_dict_dir)

        # Mock existing files
        mock_mondo_path = Mock()
        mock_mondo_path.exists.return_value = True
        mock_mondo_path.stat.return_value.st_size = 1048576  # 1MB

        mock_efo_path = Mock()
        mock_efo_path.exists.return_value = False

        # Mock the dictionary directory's __truediv__ method
        def mock_truediv(filename):
            if filename == "mondo_terms.json":
                return mock_mondo_path
            elif filename == "efo_terms.json":
                return mock_efo_path
            else:
                return Mock()

        mock_dict_dir.__truediv__ = mock_truediv

        result = get_available_ontologies()

        assert isinstance(result, dict)
        assert "mondo" in result
        assert "efo" in result

        # Test mondo is available
        assert result["mondo"]["available"] is True
        assert result["mondo"]["file_size_mb"] == 1.0

        # Test efo is not available
        assert result["efo"]["available"] is False

    @patch("src.tools.normalizer_tools.OntologySemanticSearch")
    def test_semantic_search_ontology_success(self, mock_search_class):
        """Test successful semantic search against an ontology."""
        # Mock the semantic search instance
        mock_search = Mock()
        mock_search.search.return_value = [
            ("diabetes mellitus", "MONDO:0005015", 0.95),
            ("type 2 diabetes", "MONDO:0005148", 0.90),
        ]
        mock_search_class.return_value = mock_search

        # Mock available ontologies
        with patch(
            "src.tools.normalizer_tools.get_available_ontologies"
        ) as mock_get_onts:
            mock_get_onts.return_value = {
                "mondo": {"available": True, "path": "/fake/path/mondo.json"}
            }

            result = semantic_search_ontology(
                "diabetes", "mondo", top_k=5, min_score=0.5
            )

            assert len(result) == 2
            assert isinstance(result[0], OntologyMatch)
            assert result[0].term == "diabetes mellitus"
            assert result[0].term_id == "MONDO:0005015"
            assert result[0].score == 0.95
            assert result[0].ontology == "mondo"

    def test_semantic_search_ontology_unavailable(self):
        """Test semantic search with unavailable ontology."""
        with patch(
            "src.tools.normalizer_tools.get_available_ontologies"
        ) as mock_get_onts:
            mock_get_onts.return_value = {
                "mondo": {"available": False, "path": "/fake/path/mondo.json"}
            }

            with pytest.raises(NormalizationError) as exc_info:
                semantic_search_ontology("diabetes", "mondo")

            assert "dictionary not available" in str(exc_info.value)

    def test_semantic_search_ontology_unknown(self):
        """Test semantic search with unknown ontology."""
        with patch(
            "src.tools.normalizer_tools.get_available_ontologies"
        ) as mock_get_onts:
            mock_get_onts.return_value = {}

            with pytest.raises(NormalizationError) as exc_info:
                semantic_search_ontology("diabetes", "unknown_ontology")

            assert "not recognized" in str(exc_info.value)

    @patch("src.tools.normalizer_tools.semantic_search_ontology")
    def test_normalize_candidate_value(self, mock_search):
        """Test normalizing a single candidate value."""
        # Mock semantic search results for both ontologies that Disease maps to
        mock_search.return_value = [
            OntologyMatch(
                term="diabetes mellitus",
                term_id="MONDO:0005015",
                score=0.95,
                ontology="mondo",
            )
        ]

        # Create test candidate
        candidate = ExtractedCandidate(
            value="diabetes",
            confidence=0.9,
            source="sample",
            context="Sample metadata field",
            rationale="Extracted from sample characteristics",
            prenormalized="diabetes (preliminary)",
        )

        result = normalize_candidate_value(candidate, "Disease")

        assert isinstance(result, NormalizedCandidate)
        assert result.value == "diabetes"
        assert result.confidence == 0.9
        # Disease field maps to both mondo and efo, so we get 2 matches (one from each ontology)
        assert len(result.ontology_matches) == 2
        assert result.best_match is not None
        assert result.best_match.term == "diabetes mellitus"
        assert result.normalization_confidence == 0.95

        # Verify that semantic_search_ontology was called for both mondo and efo
        assert mock_search.call_count == 2

    def test_normalize_curation_result(self):
        """Test normalizing an entire curation result."""
        # Create test curation result
        candidate = ExtractedCandidate(
            value="DLBCL",
            confidence=0.95,
            source="sample",
            context="Sample name contains DLBCL",
            rationale="Abbreviation for diffuse large B-cell lymphoma",
            prenormalized="diffuse large B-cell lymphoma",
        )

        curation_result = CurationResult(
            sample_id="GSM1000981",
            target_field="Disease",
            sample_candidates=[candidate],
            final_candidate="DLBCL",
            final_confidence=0.95,
            sources_processed=["sample"],
        )

        # Mock the normalize_candidate_value function
        with patch(
            "src.tools.normalizer_tools.normalize_candidate_value"
        ) as mock_normalize:
            mock_normalized = NormalizedCandidate(
                value="DLBCL",
                confidence=0.95,
                source="sample",
                context="Sample name contains DLBCL",
                rationale="Abbreviation for diffuse large B-cell lymphoma",
                prenormalized="diffuse large B-cell lymphoma",
                ontology_matches=[
                    OntologyMatch(
                        term="diffuse large B-cell lymphoma",
                        term_id="MONDO:0018906",
                        score=0.98,
                        ontology="mondo",
                    )
                ],
                best_match=OntologyMatch(
                    term="diffuse large B-cell lymphoma",
                    term_id="MONDO:0018906",
                    score=0.98,
                    ontology="mondo",
                ),
                normalization_confidence=0.98,
            )
            mock_normalize.return_value = mock_normalized

            result = normalize_curation_result(curation_result)

            assert isinstance(result, NormalizationResult)
            assert result.sample_id == "GSM1000981"
            assert result.target_field == "Disease"
            assert len(result.normalized_sample_candidates) == 1
            assert result.final_normalized_term == "diffuse large B-cell lymphoma"
            assert result.final_normalized_id == "MONDO:0018906"
            assert result.final_ontology == "mondo"

    def test_load_curation_result_from_file(self):
        """Test loading curation result from JSON file."""
        # Create test curation result file
        test_data = {
            "sample_id": "GSM1000981",
            "target_field": "Disease",
            "series_candidates": [],
            "sample_candidates": [
                {
                    "value": "DLBCL",
                    "confidence": 0.95,
                    "source": "sample",
                    "context": "Sample metadata",
                    "rationale": "Disease abbreviation",
                    "prenormalized": "diffuse large B-cell lymphoma",
                }
            ],
            "abstract_candidates": [],
            "final_candidate": "DLBCL",
            "final_confidence": 0.95,
            "reconciliation_needed": False,
            "reconciliation_reason": None,
            "sources_processed": ["sample"],
            "processing_notes": [],
        }

        test_file = self.session_dir / "test_candidates.json"
        with open(test_file, "w") as f:
            json.dump(test_data, f)

        result = load_curation_result_from_file(str(test_file))

        assert isinstance(result, CurationResult)
        assert result.sample_id == "GSM1000981"
        assert result.target_field == "Disease"
        assert len(result.sample_candidates) == 1

    def test_load_curation_result_file_not_found(self):
        """Test loading curation result from non-existent file."""
        with pytest.raises(NormalizationError) as exc_info:
            load_curation_result_from_file("non_existent_file.json")

        assert "Error loading curation result" in str(exc_info.value)

    def test_save_normalization_result(self):
        """Test saving normalization result to file."""
        # Create test normalization result
        candidate = ExtractedCandidate(
            value="diabetes",
            confidence=0.9,
            source="sample",
            context="Test context",
            rationale="Test rationale",
            prenormalized="test prenormalized",
        )

        normalized_candidate = NormalizedCandidate(
            value="diabetes",
            confidence=0.9,
            source="sample",
            context="Test context",
            rationale="Test rationale",
            prenormalized="test prenormalized",
            top_ontology_matches=[],
            normalization_confidence=0.8,
        )

        result = NormalizationResult(
            sample_id="GSM1000981",
            target_field="Disease",
            original_candidates=["test candidate"],
            normalized_candidates=[normalized_candidate],
            normalization_method="semantic_search",
            ontologies_searched=["mondo"],
            normalization_timestamp="2024-01-01T00:00:00",
            normalization_tool_version="1.0.0",
            normalization_success=True,
            normalization_confidence=0.8,
        )

        output_file = self.session_dir / "test_normalized.json"
        save_normalization_result(result, str(output_file))

        assert output_file.exists()

        # Verify file content
        with open(output_file, "r") as f:
            saved_data = json.load(f)

        assert saved_data["sample_id"] == "GSM1000981"
        assert saved_data["target_field"] == "Disease"

    @patch("src.tools.normalizer_tools.normalize_curation_result")
    @patch("src.tools.normalizer_tools.load_curation_result_from_file")
    def test_normalize_candidates_file(self, mock_load, mock_normalize):
        """Test normalizing candidates from a file."""
        # Mock loading curation result
        mock_curation = CurationResult(
            sample_id="GSM1000981",
            target_field="Disease",
            sample_candidates=[],
            sources_processed=["sample"],
        )
        mock_load.return_value = mock_curation

        # Mock normalization result
        mock_normalized = NormalizationResult(
            sample_id="GSM1000981",
            target_field="Disease",
            sample_candidates=[],
            normalized_sample_candidates=[],
            ontologies_searched=["mondo"],
            normalization_timestamp="2024-01-01T00:00:00",
            normalization_tool_version="1.0.0",
        )
        mock_normalize.return_value = mock_normalized

        # Create test input file
        input_file = self.session_dir / "test_candidates.json"
        input_file.touch()

        output_file = self.session_dir / "test_normalized.json"

        result = normalize_candidates_file(str(input_file), str(output_file))

        assert isinstance(result, NormalizationResult)
        assert result.sample_id == "GSM1000981"
        mock_load.assert_called_once_with(str(input_file))
        mock_normalize.assert_called_once()


class TestNormalizationModels:
    """Test class for normalization model validation."""

    def test_ontology_match_model(self):
        """Test OntologyMatch model validation."""
        match = OntologyMatch(
            term="diabetes mellitus",
            term_id="MONDO:0005015",
            score=0.95,
            ontology="mondo",
        )

        assert match.term == "diabetes mellitus"
        assert match.term_id == "MONDO:0005015"
        assert match.score == 0.95
        assert match.ontology == "mondo"

    def test_ontology_match_invalid_score(self):
        """Test OntologyMatch with invalid score."""
        with pytest.raises(ValueError):
            OntologyMatch(
                term="diabetes mellitus",
                term_id="MONDO:0005015",
                score=1.5,  # Invalid: > 1.0
                ontology="mondo",
            )

    def test_normalized_candidate_model(self):
        """Test NormalizedCandidate model validation."""
        match = OntologyMatch(
            term="diabetes mellitus",
            term_id="MONDO:0005015",
            score=0.95,
            ontology="mondo",
        )

        candidate = NormalizedCandidate(
            value="diabetes",
            confidence=0.9,
            source="sample",
            context="Test context",
            rationale="Test rationale",
            prenormalized="test prenormalized",
            top_ontology_matches=[match],
            best_match=match,
            normalization_confidence=0.95,
        )

        assert candidate.value == "diabetes"
        assert len(candidate.top_ontology_matches) == 1
        assert candidate.best_match == match
        assert candidate.normalization_confidence == 0.95

    def test_normalization_result_extends_curation_result(self):
        """Test that NormalizationResult properly extends CurationResult."""
        candidate = ExtractedCandidate(
            value="diabetes",
            confidence=0.9,
            source="sample",
            context="Test context",
            rationale="Test rationale",
            prenormalized="test prenormalized",
        )

        normalized_candidate = NormalizedCandidate(
            value="diabetes",
            confidence=0.9,
            source="sample",
            context="Test context",
            rationale="Test rationale",
            prenormalized="test prenormalized",
            top_ontology_matches=[],
            normalization_confidence=0.8,
        )

        result = NormalizationResult(
            sample_id="GSM1000981",
            target_field="Disease",
            original_candidates=["test candidate"],
            normalized_candidates=[normalized_candidate],
            normalization_method="semantic_search",
            ontologies_searched=["mondo"],
            normalization_timestamp="2024-01-01T00:00:00",
            normalization_tool_version="1.0.0",
            normalization_success=True,
            normalization_confidence=0.8,
        )

        # Test that it has the new NormalizationResult structure
        assert result.sample_id == "GSM1000981"
        assert result.target_field == "Disease"
        assert len(result.original_candidates) == 1
        assert len(result.normalized_candidates) == 1
        assert result.normalization_success is True
        assert result.normalization_confidence == 0.8
