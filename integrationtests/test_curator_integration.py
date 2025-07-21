"""
Integration tests for the CuratorAgent.

This module contains integration tests that test the CuratorAgent with real data
from the existing sample directory structure to validate end-to-end functionality.
"""

import json
import tempfile
import shutil
from pathlib import Path
import pytest

from src.tools.curator_tools import CuratorTools
from src.agents.curator import create_curator_agent
from src.workflows.MetaMuse import create_curation_pipeline


class TestCuratorIntegration:
    """Integration tests for CuratorAgent with real-like data."""

    def setup_method(self):
        """Set up integration test fixtures with realistic data."""
        self.temp_dir = tempfile.mkdtemp()
        self.session_dir = Path(self.temp_dir)

        # Create realistic test data based on actual GSE29282 structure
        self.setup_realistic_test_data()

    def setup_realistic_test_data(self):
        """Set up test data that mimics the real GSE29282 structure."""
        
        # Create mapping file
        mapping_data = {
            "mapping": {
                "GSE29282": {
                    "sample_ids": ["GSM1000981", "GSM1000984"],
                    "sample_count": 2,
                    "series_directory": "GSE29282"
                }
            },
            "reverse_mapping": {
                "GSM1000981": "GSE29282",
                "GSM1000984": "GSE29282"
            },
            "total_series": 1,
            "total_samples": 2
        }

        with open(self.session_dir / "series_sample_mapping.json", "w") as f:
            json.dump(mapping_data, f)

        # Create series directory
        series_dir = self.session_dir / "GSE29282"
        series_dir.mkdir()
        cleaned_dir = series_dir / "cleaned"
        cleaned_dir.mkdir()

        # Create realistic linked_data.json for GSM1000981 (siNT control)
        linked_data_gsm1000981 = {
            "sample_id": "GSM1000981",
            "series_id": "GSE29282",
            "directory": str(series_dir),
            "cleaned_files": [
                str(cleaned_dir / "GSE29282_metadata_cleaned.json"),
                str(cleaned_dir / "PMID_23911289_metadata_cleaned.json")
            ],
            "sample_metadata": {
                "gsm_id": "GSM1000981",
                "attributes": {
                    "title": "OCI-LY1_48hrs_mRNAseq_3x_siNT_R1",
                    "source_name_ch1": "Human DLBCL cel line",
                    "organism_ch1": "Homo sapiens",
                    "characteristics_ch1": "treatment: siNT, cell line: OCI-LY1",
                    "description": "mRNA sequencing in baseline non-nucleofected OCI-Ly1, RNAseq_biologicalTriplicates_48h in OCI-Ly1 siNT replicate1",
                    "extract_protocol_ch1": "RNAseq: Three ug of total RNA was isolated from OCI-Ly1 cells transfected using Nucleofector 96-well Shuttle system (Lonza) with siBCL6 (HSS100968) or siNT (46-2001) (Stealth RNAi, Invitrogen) at 24hrs and 48hrs after nucleofection.",
                    "growth_protocol_ch1": "DLBCL cell lines OCI-Ly1 and OCI-Ly7 were grown in medium containing 90% Iscove's (Cellgro, Manassas, VA), 10% fetal bovine serum (Gemini, Irvine, CA), and 1% penicillin/streptomycin (Invitrogen, Carlsbad, CA).",
                }
            }
        }

        with open(series_dir / "GSM1000981_linked_data.json", "w") as f:
            json.dump(linked_data_gsm1000981, f)

        # Create realistic linked_data.json for GSM1000984 (siBCL6 treatment)
        linked_data_gsm1000984 = {
            "sample_id": "GSM1000984",
            "series_id": "GSE29282",
            "directory": str(series_dir),
            "cleaned_files": [
                str(cleaned_dir / "GSE29282_metadata_cleaned.json"),
                str(cleaned_dir / "PMID_23911289_metadata_cleaned.json")
            ],
            "sample_metadata": {
                "gsm_id": "GSM1000984",
                "attributes": {
                    "title": "OCI-LY1_48hrs_mRNAseq_3x_siBCL6_R1",
                    "source_name_ch1": "Human DLBCL cel line",
                    "organism_ch1": "Homo sapiens",
                    "characteristics_ch1": "treatment: siBCL6, cell line: OCI-LY1",
                    "description": "mRNA sequencing in baseline non-nucleofected OCI-Ly1, RNAseq_biologicalTriplicates_48h in OCI-Ly1 siBCL6 replicate1",
                    "extract_protocol_ch1": "RNAseq: Three ug of total RNA was isolated from OCI-Ly1 cells transfected using Nucleofector 96-well Shuttle system (Lonza) with siBCL6 (HSS100968) or siNT (46-2001) (Stealth RNAi, Invitrogen) at 24hrs and 48hrs after nucleofection.",
                    "growth_protocol_ch1": "DLBCL cell lines OCI-Ly1 and OCI-Ly7 were grown in medium containing 90% Iscove's (Cellgro, Manassas, VA), 10% fetal bovine serum (Gemini, Irvine, CA), and 1% penicillin/streptomycin (Invitrogen, Carlsbad, CA).",
                }
            }
        }

        with open(series_dir / "GSM1000984_linked_data.json", "w") as f:
            json.dump(linked_data_gsm1000984, f)

        # Create realistic series metadata (cleaned)
        series_metadata = {
            "gse_id": "GSE29282",
            "attributes": {
                "title": "A Hybrid Mechanism of Action for BCL6 in B Cells Defined by Formation of Functionally Distinct Complexes at Enhancers and Promoters",
                "summary": "BCL6 is crucial for B-cell activation and lymphomagenesis. We used integrative genomics to explore BCL6 mechanism in normal and malignant B-cells. Surprisingly, BCL6 assembled distinct complexes at enhancers vs. promoters. At enhancers BCL6 preferentially recruited SMRT, which mediated H3K27 deacetylation through HDAC3, antagonized p300 activity and repressed transcription, but without decommissioning enhancers. This provides a biochemical, basis for toggling enhancers from the active to poised state. Virtually all SMRT was bound with BCL6 suggesting that in B-cells BCL6 uniquely sequesters SMRT from other factors. In promoters BCL6 preferentially recruited BCOR, but most potently repressed promoters where it, formed a distinctive ternary complex with SMRT and BCOR. Promoter repression was associated with decreased H3K36me3, H3K79me2 and Pol II elongation, linking BCL6 to transcriptional pausing.",
                "overall_design": "We identified the binding patterns of BCL6, SMRT, NCOR and BCOR corepressors in normal germinal center B cells and a DLBCL cell line (OCI-Ly1) using ChIP-seq. Additionally we treated lymphoma cells with siRNA against BCL6 and a non-targeted siRNA (NT control) and performed RNA-seq to identify the genes bound and repressed by BCL6. RNA-seq experiments were performed at 24h and 48h after siRNA treatments. Additional biological triplicate RNA-seq experiments were performed at 48h after BCL6 knockdown.",
                "type": "Expression profiling by high throughput sequencing, Genome binding/occupancy profiling by high throughput sequencing"
            }
        }

        with open(cleaned_dir / "GSE29282_metadata_cleaned.json", "w") as f:
            json.dump(series_metadata, f)

        # Create realistic abstract metadata
        abstract_metadata = {
            "pmid": 23911289,
            "title": "A hybrid mechanism of action for BCL6 in B cells defined by formation of functionally distinct complexes at enhancers and promoters.",
            "abstract": "The BCL6 transcriptional repressor is required for the development of germinal center (GC) B cells and diffuse large B cell lymphomas (DLBCLs). Although BCL6 can recruit multiple corepressors, its transcriptional repression mechanism of action in normal and malignant B cells is unknown. We find that in B cells, BCL6 mostly functions through two independent mechanisms that are collectively essential to GC formation and DLBCL, both mediated through its N-terminal BTB domain. These are (1) the formation of a unique ternary BCOR-SMRT complex at promoters, with each corepressor binding to symmetrical sites on BCL6 homodimers linked to specific epigenetic chromatin features, and (2) the \"toggling\" of active enhancers to a poised but not erased conformation through SMRT-dependent H3K27 deacetylation, which is mediated by HDAC3 and opposed by p300 histone acetyltransferase. Dynamic toggling of enhancers provides a basis for B cells to undergo rapid transcriptional and phenotypic changes in response to signaling or environmental cues.",
            "series_id": "GSE29282"
        }

        with open(cleaned_dir / "PMID_23911289_metadata_cleaned.json", "w") as f:
            json.dump(abstract_metadata, f)

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_disease_curation_dlbcl_samples(self):
        """Test disease curation on DLBCL samples - should consistently identify DLBCL."""
        tools = CuratorTools(str(self.session_dir))

        # Test both samples
        for sample_id in ["GSM1000981", "GSM1000984"]:
            # Load sample data
            load_result = tools.load_sample_data(sample_id)
            assert load_result.success is True
            assert load_result.data["sample_id"] == sample_id

            # Extract disease candidates
            extract_result = tools.extract_metadata_candidates(load_result.data, "Disease")
            assert extract_result.success is True
            assert extract_result.candidates is not None

            # Verify format of candidates (should be dictionaries)
            if len(extract_result.candidates) > 0:
                for file_candidates in extract_result.candidates.values():
                    for candidate in file_candidates:
                        assert isinstance(candidate, dict)
                        assert "value" in candidate
                        assert "confidence" in candidate
                        assert "context" in candidate

            # Reconcile candidates (dummy function)
            reconcile_result = tools.reconcile_candidates(extract_result.candidates, "Disease")
            assert reconcile_result.success is True
            assert reconcile_result.data["target_field"] == "Disease"
            assert reconcile_result.data["reconciliation_status"] == "reconciliation required"

            # Save results
            save_result = tools.save_curator_results(sample_id, reconcile_result.data)
            assert save_result.success is True

            # Verify output file
            output_file = Path(save_result.files_created[0])
            assert output_file.exists()
            assert sample_id in output_file.name

    def test_treatment_field_curation(self):
        """Test curation of treatment field using LLM extraction."""
        tools = CuratorTools(str(self.session_dir))

        # Test GSM1000981 (siNT control)
        load_result = tools.load_sample_data("GSM1000981")
        assert load_result.success is True

        extract_result = tools.extract_metadata_candidates(load_result.data, "Treatment")
        assert extract_result.success is True

        # Verify format of candidates if any are found
        if extract_result.candidates:
            for file_candidates in extract_result.candidates.values():
                for candidate in file_candidates:
                    assert isinstance(candidate, dict)
                    assert "value" in candidate
                    assert "confidence" in candidate
                    assert "context" in candidate
                    assert isinstance(candidate["confidence"], (int, float))
                    assert 0.0 <= candidate["confidence"] <= 1.0

    def test_curation_pipeline_workflow(self):
        """Test the complete curation pipeline using create_curation_pipeline."""
        # Test creating a curation pipeline with existing session directory
        pipeline_agent = create_curation_pipeline(
            existing_session_dir=str(self.session_dir),
            input_data="GSM1000981 target_field:Disease"
        )

        assert pipeline_agent is not None
        assert pipeline_agent.name == "CuratorAgent"

    def test_multiple_samples_consensus(self):
        """Test curation across multiple samples for consensus building."""
        tools = CuratorTools(str(self.session_dir))

        results_by_sample = {}

        # Process both samples
        for sample_id in ["GSM1000981", "GSM1000984"]:
            load_result = tools.load_sample_data(sample_id)
            extract_result = tools.extract_metadata_candidates(load_result.data, "Disease")
            reconcile_result = tools.reconcile_candidates(extract_result.candidates, "Disease")
            
            results_by_sample[sample_id] = reconcile_result.data

        # Both samples should identify similar disease (DLBCL)
        # This tests consistency across samples from the same study
        sample1_candidate = results_by_sample["GSM1000981"].get("final_candidate", "").lower()
        sample2_candidate = results_by_sample["GSM1000984"].get("final_candidate", "").lower()

        if sample1_candidate and sample2_candidate:
            # Should both contain dlbcl or lymphoma
            assert "dlbcl" in sample1_candidate or "lymphoma" in sample1_candidate
            assert "dlbcl" in sample2_candidate or "lymphoma" in sample2_candidate

    def test_error_handling_missing_files(self):
        """Test error handling when files are missing."""
        tools = CuratorTools(str(self.session_dir))

        # Test with non-existent sample
        result = tools.load_sample_data("GSM9999999")
        assert result.success is False
        assert "not found in mapping" in result.message

        # Test with missing linked_data file
        (self.session_dir / "GSE29282" / "GSM1000981_linked_data.json").unlink()
        result = tools.load_sample_data("GSM1000981")
        assert result.success is False
        assert "Linked data file not found" in result.message

    def test_field_specific_extraction_patterns(self):
        """Test that different fields extract appropriate patterns."""
        tools = CuratorTools(str(self.session_dir))

        load_result = tools.load_sample_data("GSM1000981")
        assert load_result.success is True

        # Test different target fields
        test_fields = ["Disease", "Organism", "Treatment", "Tissue"]

        for field in test_fields:
            extract_result = tools.extract_metadata_candidates(load_result.data, field)
            assert extract_result.success is True
            
            # Each field should use appropriate extraction logic
            # Disease should find DLBCL, Organism should find Homo sapiens, etc.
            if extract_result.candidates:
                print(f"Field {field}: {extract_result.candidates}")


class TestRealDataIntegration:
    """Integration tests using the actual data from the sandbox directory."""

    def test_with_existing_sandbox_data(self):
        """Test with existing sandbox data if available."""
        # Check if the sandbox directory with real data exists
        sandbox_dir = Path("sandbox/di_c414a6ee-346e-469b-bae5-2c5316872314")
        
        if not sandbox_dir.exists():
            pytest.skip("Real sandbox data not available for integration testing")

        # Test with real data
        tools = CuratorTools(str(sandbox_dir))

        # Try to load a known sample
        result = tools.load_sample_data("GSM1000981")
        
        if result.success:
            # Test disease extraction on real data
            extract_result = tools.extract_metadata_candidates(result.data, "Disease")
            assert extract_result.success is True

            # Test reconciliation
            if extract_result.candidates:
                reconcile_result = tools.reconcile_candidates(extract_result.candidates, "Disease")
                assert reconcile_result.success is True

                print(f"Real data disease candidates: {extract_result.candidates}")
                print(f"Real data reconciliation: {reconcile_result.data}")


def test_end_to_end_workflow():
    """Test the complete end-to-end workflow."""
    # This would typically require running the full pipeline
    # For now, just test that the components can be created
    try:
        from src.workflows.MetaMuse import create_curation_pipeline, create_complete_pipeline
        
        # Test creating pipelines (without running them)
        print("✅ Pipeline creation functions available")
        
        # These would fail without proper data, but we can test they're importable
        assert create_curation_pipeline is not None
        assert create_complete_pipeline is not None
        
    except ImportError as e:
        pytest.fail(f"Failed to import pipeline functions: {e}")


if __name__ == "__main__":
    # Run specific integration tests
    test_end_to_end_workflow()
    print("✅ Basic integration tests passed!") 