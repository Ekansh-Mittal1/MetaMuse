"""
Unit tests for curator tools.

This module contains comprehensive tests for the CuratorTools class
and related functionality for metadata curation.
"""

import json
import tempfile
import shutil
from pathlib import Path

from src.tools.curator_tools import (
    CuratorTools,
    load_sample_data_impl,
    extract_metadata_candidates_impl,
    reconcile_candidates_impl,
    save_curation_results_impl,
)


class TestCuratorTools:
    """Test class for CuratorTools."""

    def setup_method(self, mock_openai_client):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.session_dir = Path(self.temp_dir)
        self.tools = CuratorTools(str(self.session_dir))

        # Create test mapping file
        mapping_data = {
            "mapping": {
                "GSE29282": {
                    "sample_ids": ["GSM1000981", "GSM1000984"],
                    "sample_count": 2,
                    "series_directory": "GSE29282",
                }
            },
            "reverse_mapping": {"GSM1000981": "GSE29282", "GSM1000984": "GSE29282"},
            "total_series": 1,
            "total_samples": 2,
        }

        with open(self.session_dir / "series_sample_mapping.json", "w") as f:
            json.dump(mapping_data, f)

        # Create series directory and test files
        self.series_dir = self.session_dir / "GSE29282"
        self.series_dir.mkdir()

        # Create test linked_data.json
        linked_data = {
            "sample_id": "GSM1000981",
            "series_id": "GSE29282",
            "directory": str(self.series_dir),
            "cleaned_files": [
                str(self.series_dir / "cleaned" / "GSE29282_metadata_cleaned.json"),
                str(
                    self.series_dir / "cleaned" / "PMID_23911289_metadata_cleaned.json"
                ),
            ],
            "sample_metadata": {
                "gsm_id": "GSM1000981",
                "attributes": {
                    "title": "DLBCL cell line treatment",
                    "source_name_ch1": "Human DLBCL cell line",
                    "organism_ch1": "Homo sapiens",
                    "characteristics_ch1": "treatment: siNT, cell line: OCI-LY1",
                    "description": "mRNA sequencing of diffuse large B cell lymphoma cells",
                },
            },
        }

        with open(self.series_dir / "GSM1000981_linked_data.json", "w") as f:
            json.dump(linked_data, f)

        # Create cleaned files directory and test files
        cleaned_dir = self.series_dir / "cleaned"
        cleaned_dir.mkdir()

        # Series metadata
        series_metadata = {
            "gse_id": "GSE29282",
            "attributes": {
                "title": "BCL6 mechanism in B cells and DLBCL",
                "summary": "Study of BCL6 in normal and malignant B-cells including diffuse large B cell lymphoma",
                "overall_design": "Investigation of lymphoma cell lines",
            },
        }

        with open(cleaned_dir / "GSE29282_metadata_cleaned.json", "w") as f:
            json.dump(series_metadata, f)

        # Abstract metadata
        abstract_metadata = {
            "pmid": 23911289,
            "title": "BCL6 mechanism in B cells for lymphoma development",
            "abstract": "The BCL6 transcriptional repressor is required for diffuse large B cell lymphomas (DLBCLs). This study investigates lymphoma formation mechanisms.",
        }

        with open(cleaned_dir / "PMID_23911289_metadata_cleaned.json", "w") as f:
            json.dump(abstract_metadata, f)

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_load_sample_data_success(self):
        """Test successful loading of sample data."""
        result = self.tools.load_sample_data("GSM1000981")

        assert result.success is True
        assert "Successfully loaded data for sample GSM1000981" in result.message
        assert result.data is not None
        assert result.data["sample_id"] == "GSM1000981"
        assert result.data["series_id"] == "GSE29282"
        assert "linked_data" in result.data
        assert "cleaned_files" in result.data
        assert len(result.data["cleaned_files"]) == 2

    def test_load_sample_data_not_found(self):
        """Test loading data for non-existent sample."""
        result = self.tools.load_sample_data("GSM9999999")

        assert result.success is False
        assert "not found in mapping" in result.message

    def test_load_sample_data_missing_mapping(self):
        """Test loading data when mapping file is missing."""
        (self.session_dir / "series_sample_mapping.json").unlink()
        result = self.tools.load_sample_data("GSM1000981")

        assert result.success is False
        assert "Mapping file not found" in result.message

    def test_extract_metadata_candidates_disease(self):
        """Test extraction of disease candidates using LLM."""
        # First load sample data
        load_result = self.tools.load_sample_data("GSM1000981")
        assert load_result.success is True

        # Extract disease candidates
        result = self.tools.extract_metadata_candidates(load_result.data, "Disease")

        assert result.success is True
        assert result.candidates is not None

        # Check format of candidates (should be dictionaries with value, confidence, context, prenormalized)
        if len(result.candidates) > 0:
            for file_candidates in result.candidates.values():
                for candidate in file_candidates:
                    assert isinstance(candidate, dict)
                    assert "value" in candidate
                    assert "confidence" in candidate
                    assert "context" in candidate
                    assert isinstance(candidate["confidence"], (int, float))
                    assert 0.0 <= candidate["confidence"] <= 1.0

    def test_extract_metadata_candidates_generic(self):
        """Test extraction of generic field candidates using LLM."""
        load_result = self.tools.load_sample_data("GSM1000981")
        assert load_result.success is True

        result = self.tools.extract_metadata_candidates(load_result.data, "Treatment")

        assert result.success is True
        # Check format of candidates if any are found
        if len(result.candidates) > 0:
            for file_candidates in result.candidates.values():
                for candidate in file_candidates:
                    assert isinstance(candidate, dict)
                    assert "value" in candidate
                    assert "confidence" in candidate
                    assert "context" in candidate

    def test_llm_extraction_with_mock_data(self):
        """Test LLM extraction with mock data structure."""
        # Create mock data structure that doesn't require LLM call
        mock_data = {
            "attributes": {
                "title": "Test sample with DLBCL cell line",
                "description": "Study of diffuse large B cell lymphoma",
            }
        }

        # Test the flattening function still works
        flattened = self.tools._flatten_to_text(mock_data)
        assert "dlbcl" in flattened.lower() or "lymphoma" in flattened.lower()

    def test_template_loading(self):
        """Test that extraction templates can be loaded."""
        from src.tools.curator_tools import load_extraction_template

        # Test that Disease template exists and loads
        template = load_extraction_template("Disease")
        assert template is not None
        assert len(template) > 0
        assert "Disease" in template or "disease" in template

    def test_reconcile_candidates_dummy(self):
        """Test dummy reconciliation function."""
        candidates_by_file = {
            "file1.json": [
                {
                    "value": "dlbcl",
                    "confidence": 0.9,
                    "context": "cell line study",
                    "prenormalized": "diffuse large B-cell lymphoma (MONDO:0018906)",
                },
                {
                    "value": "lymphoma",
                    "confidence": 0.8,
                    "context": "cancer type",
                    "prenormalized": "lymphoma (MONDO:0005062)",
                },
            ],
            "file2.json": [
                {
                    "value": "DLBCL",
                    "confidence": 0.95,
                    "context": "disease name",
                    "prenormalized": "diffuse large B-cell lymphoma (MONDO:0018906)",
                },
                {
                    "value": "diffuse large B cell lymphoma",
                    "confidence": 0.9,
                    "context": "full name",
                    "prenormalized": "diffuse large B-cell lymphoma (MONDO:0018906)",
                },
            ],
        }

        result = self.tools.reconcile_candidates(candidates_by_file, "Disease")

        assert result.success is True
        assert result.data["reconciliation_status"] == "reconciliation required"
        assert result.data["target_field"] == "Disease"
        assert result.data["candidates_by_file"] == candidates_by_file
        assert result.data["total_files_processed"] == 2
        assert result.data["total_candidates"] == 4

    def test_reconcile_candidates_empty(self):
        """Test dummy reconciliation with no candidates."""
        candidates_by_file = {}

        result = self.tools.reconcile_candidates(candidates_by_file, "Disease")

        assert result.success is True
        assert result.data["reconciliation_status"] == "reconciliation required"
        assert result.data["total_files_processed"] == 0
        assert result.data["total_candidates"] == 0

    def test_reconcile_candidates_placeholder(self):
        """Test placeholder reconciliation function."""
        conflicting_data = {"conflict": "test"}

        result = self.tools.reconcile_candidates_placeholder(
            "GSM1000981", "Disease", conflicting_data
        )

        assert result.success is True
        assert result.data["final_candidate"] == "NEEDS_MANUAL_REVIEW"
        assert result.data["confidence"] == "manual_review_required"

    def test_save_curator_results(self):
        """Test saving curator results."""
        results_data = {
            "target_field": "Disease",
            "final_candidate": "dlbcl",
            "confidence": "high",
        }

        result = self.tools.save_curator_results("GSM1000981", results_data)

        assert result.success is True
        assert result.files_created is not None
        assert len(result.files_created) == 1

        # Verify file was created and contains correct data
        output_file = Path(result.files_created[0])
        assert output_file.exists()

        with open(output_file, "r") as f:
            saved_data = json.load(f)

        assert saved_data["sample_id"] == "GSM1000981"
        assert saved_data["target_field"] == "Disease"

    def test_flatten_to_text(self):
        """Test flattening nested data to text."""
        test_data = {
            "level1": "value1",
            "nested": {"level2": "value2", "deep": {"level3": "value3"}},
            "list_field": ["item1", "item2"],
        }

        flattened = self.tools._flatten_to_text(test_data)

        assert "level1: value1" in flattened
        assert "level2: value2" in flattened
        assert "level3: value3" in flattened
        assert "item1" in flattened
        assert "item2" in flattened


class TestCuratorToolsImplementations:
    """Test class for implementation functions."""

    def setup_method(self, mock_openai_client):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.session_dir = str(self.temp_dir)

        # Create minimal test structure
        session_path = Path(self.temp_dir)

        mapping_data = {"reverse_mapping": {"GSM1000981": "GSE29282"}}
        with open(session_path / "series_sample_mapping.json", "w") as f:
            json.dump(mapping_data, f)

        series_dir = session_path / "GSE29282"
        series_dir.mkdir()

        linked_data = {
            "sample_id": "GSM1000981",
            "series_id": "GSE29282",
            "cleaned_files": [],
            "sample_metadata": {"gsm_id": "GSM1000981"},
        }

        with open(series_dir / "GSM1000981_linked_data.json", "w") as f:
            json.dump(linked_data, f)

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_load_sample_data_impl(self):
        """Test load_sample_data_impl function."""
        result = load_sample_data_impl("GSM1000981", self.session_dir)

        assert result["success"] is True
        assert "data" in result
        assert result["data"]["sample_id"] == "GSM1000981"

    def test_extract_metadata_candidates_impl(self):
        """Test extract_metadata_candidates_impl function."""
        # First load sample data
        load_result = load_sample_data_impl("GSM1000981", self.session_dir)
        assert load_result["success"] is True

        result = extract_metadata_candidates_impl(
            load_result["data"], "Disease", self.session_dir
        )

        assert result["success"] is True
        assert "candidates" in result

    def test_reconcile_candidates_impl(self):
        """Test reconcile_candidates_impl function."""
        candidates_by_file = {
            "file1.json": ["candidate1"],
            "file2.json": ["candidate1"],
        }

        result = reconcile_candidates_impl(
            candidates_by_file, "Disease", self.session_dir
        )

        assert result["success"] is True
        assert "data" in result

    def test_save_curation_results_impl(self):
        """Test save_curation_results_impl function."""
        from src.models.curation_models import CurationResult

        # Create a test CurationResult
        curation_result = CurationResult(
            sample_id="GSM1000981",
            target_field="Disease",
            final_candidate="test",
            final_confidence=0.8,
        )

        result = save_curation_results_impl([curation_result], self.session_dir)

        assert result["success"] is True
        assert "files_created" in result


def test_integration_workflow(mock_openai_client):
    """Integration test for the complete curation workflow."""
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        session_dir = Path(temp_dir)
        tools = CuratorTools(str(session_dir))

        # Set up test data structure
        mapping_data = {"reverse_mapping": {"GSM1000981": "GSE29282"}}
        with open(session_dir / "series_sample_mapping.json", "w") as f:
            json.dump(mapping_data, f)

        series_dir = session_dir / "GSE29282"
        series_dir.mkdir()
        cleaned_dir = series_dir / "cleaned"
        cleaned_dir.mkdir()

        # Create test files with disease mentions
        linked_data = {
            "sample_id": "GSM1000981",
            "series_id": "GSE29282",
            "cleaned_files": [str(cleaned_dir / "test_metadata.json")],
            "sample_metadata": {
                "gsm_id": "GSM1000981",
                "attributes": {
                    "description": "Study of diffuse large B cell lymphoma progression"
                },
            },
        }

        with open(series_dir / "GSM1000981_linked_data.json", "w") as f:
            json.dump(linked_data, f)

        cleaned_metadata = {
            "title": "DLBCL research study",
            "abstract": "Investigation of lymphoma mechanisms",
        }

        with open(cleaned_dir / "test_metadata.json", "w") as f:
            json.dump(cleaned_metadata, f)

        # Run complete workflow
        # 1. Load data
        load_result = tools.load_sample_data("GSM1000981")
        assert load_result.success is True

        # 2. Extract candidates
        extract_result = tools.extract_metadata_candidates(load_result.data, "Disease")
        assert extract_result.success is True

        # 3. Reconcile candidates
        reconcile_result = tools.reconcile_candidates(
            extract_result.candidates, "Disease"
        )
        assert reconcile_result.success is True

        # 4. Save results
        save_result = tools.save_curator_results("GSM1000981", reconcile_result.data)
        assert save_result.success is True

        # Verify output file
        output_file = Path(save_result.files_created[0])
        assert output_file.exists()

        with open(output_file, "r") as f:
            final_results = json.load(f)

        assert final_results["sample_id"] == "GSM1000981"
        assert "curation_results" in final_results


if __name__ == "__main__":
    # Run basic integration test
    test_integration_workflow()
    print("✅ Basic integration test passed!")
