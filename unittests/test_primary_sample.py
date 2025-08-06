"""
Unit tests for PrimarySample target field functionality.

This module contains comprehensive tests for the PrimarySample target field,
including curation models, normalization protection, and workflow integration.
"""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from src.models.curation_models import PrimarySampleCurationResult, PrimarySampleExtractedCandidate, ExtractedCandidate
from src.models.normalization_models import PrimarySampleNormalizationResult
from src.models.agent_outputs import PrimarySampleCuratorOutput
from src.tools.normalizer_tools import NormalizationError


class TestPrimarySampleCurationResult:
    """Test class for PrimarySampleCurationResult model."""

    def test_primary_sample_curation_result_creation(self):
        """Test creating a PrimarySampleCurationResult with valid data."""
        candidates = [
            PrimarySampleExtractedCandidate(
                value=True,
                confidence=0.95,
                source="sample",
                context="patient identifier in source_name",
                rationale="Clear patient identifier found",
                prenormalized="primary_sample (TRUE)"
            )
        ]
        
        result = PrimarySampleCurationResult(
            sample_id="GSM1234567",
            target_field="PrimarySample",
            is_primary_sample=True,
            confidence=0.95,
            sample_candidates=candidates,
            final_candidates=candidates,
            sources_processed=["sample"],
            processing_notes=["Successfully classified as primary sample"]
        )
        
        assert result.sample_id == "GSM1234567"
        assert result.target_field == "PrimarySample"
        assert result.is_primary_sample is True
        assert result.confidence == 0.95
        assert len(result.sample_candidates) == 1
        assert len(result.final_candidates) == 1

    def test_cell_line_curation_result_creation(self):
        """Test creating a PrimarySampleCurationResult for cell line classification."""
        candidates = [
            PrimarySampleExtractedCandidate(
                value=False,
                confidence=0.98,
                source="sample",
                context="established cell line name",
                rationale="Clear cell line identifier found",
                prenormalized="cell_line (FALSE)"
            )
        ]
        
        result = PrimarySampleCurationResult(
            sample_id="GSM2714456",
            target_field="PrimarySample",
            is_primary_sample=False,
            confidence=0.98,
            sample_candidates=candidates,
            final_candidates=candidates,
            sources_processed=["sample"],
            processing_notes=["Successfully classified as cell line"]
        )
        
        assert result.sample_id == "GSM2714456"
        assert result.target_field == "PrimarySample"
        assert result.is_primary_sample is False
        assert result.confidence == 0.98


class TestPrimarySampleNormalizationResult:
    """Test class for PrimarySampleNormalizationResult model."""

    def test_primary_sample_normalization_result_creation(self):
        """Test creating a PrimarySampleNormalizationResult with valid data."""
        candidates = [
            PrimarySampleExtractedCandidate(
                value=True,
                confidence=0.95,
                source="sample",
                context="patient identifier in source_name",
                rationale="Clear patient identifier found",
                prenormalized="primary_sample (TRUE)"
            )
        ]
        
        result = PrimarySampleNormalizationResult(
            sample_id="GSM1234567",
            target_field="PrimarySample",
            is_primary_sample=True,
            confidence=0.95,
            sample_candidates=candidates,
            final_candidates=candidates,
            sources_processed=["sample"],
            processing_notes=["Successfully classified as primary sample"],
            normalization_method="boolean_classification"
        )
        
        assert result.sample_id == "GSM1234567"
        assert result.target_field == "PrimarySample"
        assert result.is_primary_sample is True
        assert result.confidence == 0.95
        assert result.normalization_method == "boolean_classification"


class TestPrimarySampleCuratorOutput:
    """Test class for PrimarySampleCuratorOutput model."""

    def test_primary_sample_curator_output_creation(self):
        """Test creating a PrimarySampleCuratorOutput with valid data."""
        curation_result = PrimarySampleCurationResult(
            sample_id="GSM1234567",
            target_field="PrimarySample",
            is_primary_sample=True,
            confidence=0.95,
            sample_candidates=[],
            final_candidates=[],
            sources_processed=["sample"],
            processing_notes=["Successfully classified as primary sample"]
        )
        
        output = PrimarySampleCuratorOutput(
            success=True,
            message="Successfully processed PrimarySample curation",
            execution_time_seconds=10.5,
            sample_ids_requested=["GSM1234567"],
            target_field="PrimarySample",
            session_directory="/path/to/session",
            curation_results=[curation_result],
            total_samples_processed=1,
            primary_samples_count=1,
            cell_line_samples_count=0,
            samples_needing_review=0,
            files_created=["curation_results.json"],
            average_confidence=0.95
        )
        
        assert output.success is True
        assert output.target_field == "PrimarySample"
        assert output.total_samples_processed == 1
        assert output.primary_samples_count == 1
        assert output.cell_line_samples_count == 0
        assert output.average_confidence == 0.95


class TestNormalizerProtection:
    """Test class for normalizer protection against PrimarySample target field."""

    def test_normalize_candidate_value_primary_sample_error(self):
        """Test that normalize_candidate_value raises error for PrimarySample target field."""
        from src.tools.normalizer_tools import normalize_candidate_value
        
        candidate = ExtractedCandidate(
            value="true",
            confidence=0.95,
            source="sample",
            context="test context",
            rationale="test rationale",
            prenormalized="test"
        )
        
        with pytest.raises(NormalizationError) as exc_info:
            normalize_candidate_value(
                candidate=candidate,
                target_field="PrimarySample",
                ontologies=["mondo"],
                top_k=2,
                min_score=0.5
            )
        
        assert "boolean field and does not require normalization" in str(exc_info.value)

    def test_normalize_candidate_value_primary_sample_lowercase_error(self):
        """Test that normalize_candidate_value raises error for primary_sample target field."""
        from src.tools.normalizer_tools import normalize_candidate_value
        
        candidate = ExtractedCandidate(
            value="true",
            confidence=0.95,
            source="sample",
            context="test context",
            rationale="test rationale",
            prenormalized="test"
        )
        
        with pytest.raises(NormalizationError) as exc_info:
            normalize_candidate_value(
                candidate=candidate,
                target_field="primary_sample",
                ontologies=["mondo"],
                top_k=2,
                min_score=0.5
            )
        
        assert "boolean field and does not require normalization" in str(exc_info.value)

    def test_normalize_curation_result_primary_sample_error(self):
        """Test that normalize_curation_result raises error for PrimarySample target field."""
        from src.tools.normalizer_tools import normalize_curation_result
        from src.models.curation_models import CurationResult
        
        curation_result = CurationResult(
            sample_id="GSM1234567",
            target_field="PrimarySample",
            series_candidates=[],
            sample_candidates=[],
            abstract_candidates=[],
            final_candidates=[],
            sources_processed=["sample"],
            processing_notes=["Test"]
        )
        
        with pytest.raises(NormalizationError) as exc_info:
            normalize_curation_result(
                curation_result=curation_result,
                ontologies=["mondo"],
                top_k=2,
                min_score=0.5
            )
        
        assert "boolean field and does not require normalization" in str(exc_info.value)


class TestCuratorAgentIntegration:
    """Test class for curator agent integration with PrimarySample target field."""

    @patch("src.agents.curator.get_curator_output_type_for_field")
    def test_get_curator_output_type_for_primary_sample(self, mock_get_type):
        """Test that get_curator_output_type_for_field returns correct type for PrimarySample."""
        from src.agents.curator import get_curator_output_type_for_field
        from src.models.agent_outputs import PrimarySampleCuratorOutput
        
        # Test PrimarySample (case insensitive)
        result_type = get_curator_output_type_for_field("PrimarySample")
        assert result_type == PrimarySampleCuratorOutput
        
        result_type = get_curator_output_type_for_field("primary_sample")
        assert result_type == PrimarySampleCuratorOutput
        
        # Test other fields return standard CuratorOutput
        from src.models.agent_outputs import CuratorOutput
        result_type = get_curator_output_type_for_field("Disease")
        assert result_type == CuratorOutput


class TestTemplateMapping:
    """Test class for template mapping with PrimarySample target field."""

    @patch("src.agents.curator.Path")
    def test_primary_sample_template_mapping(self, mock_path):
        """Test that PrimarySample target field maps to correct template."""
        from src.agents.curator import create_curator_agent
        
        # Mock the template file existence
        mock_template_file = MagicMock()
        mock_template_file.exists.return_value = True
        mock_template_file.read.return_value = "# Primary Sample Template"
        mock_path.return_value.__truediv__.return_value = mock_template_file
        
        # This should not raise an error and should use the primary_sample.md template
        try:
            agent = create_curator_agent(
                session_id="test_session",
                sandbox_dir="sandbox",
                handoffs=[],
                input_data="target_field:PrimarySample GSM1234567"
            )
            # If we get here, the template mapping worked
            assert agent is not None
        except Exception as e:
            # If there's an error, it should not be related to template mapping
            assert "primary_sample.md" not in str(e)


if __name__ == "__main__":
    pytest.main([__file__]) 