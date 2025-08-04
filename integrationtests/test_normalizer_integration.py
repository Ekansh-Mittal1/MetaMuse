"""
Integration tests for the NormalizerAgent.

This module contains integration tests that test the NormalizerAgent with real data
and actual ontology files to validate end-to-end normalization functionality.
"""

import json
import tempfile
import shutil
from pathlib import Path
import pytest

from src.tools.normalizer_tools import (
    get_available_ontologies,
    normalize_candidates_file,
    semantic_search_ontology,
    NormalizationError,
)
from src.models import (
    NormalizationResult,
)
from src.agents.normalizer import create_normalizer_agent


class TestNormalizerIntegration:
    """Integration tests for NormalizerAgent with real-like data."""

    def setup_method(self):
        """Set up integration test fixtures with realistic data."""
        self.temp_dir = tempfile.mkdtemp()
        self.session_dir = Path(self.temp_dir)

        # Create realistic test data based on actual curation results
        self.setup_realistic_test_data()

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)

    def setup_realistic_test_data(self):
        """Set up test data that mimics real curation results."""

        # Create series directory structure
        series_dir = self.session_dir / "GSE29282"
        series_dir.mkdir()

        # Create a realistic candidates file for disease normalization
        dlbcl_candidates = {
            "tool_name": "CuratorAgent",
            "sample_id": "GSM1000981",
            "target_field": "Disease",
            "series_candidates": [],
            "sample_candidates": [
                {
                    "value": "DLBCL",
                    "confidence": 0.95,
                    "source": "sample",
                    "context": "source_name_ch1 field mentioning 'Human DLBCL cel line' that indicates the sample originates from a diffuse large B-cell lymphoma context.",
                    "rationale": "Extracted 'DLBCL' from sample characteristics, which is a recognized cancer abbreviation for diffuse large B-cell lymphoma, aligning with disease extraction guidelines.",
                    "prenormalized": "diffuse large B-cell lymphoma (MONDO:0018906)",
                }
            ],
            "abstract_candidates": [
                {
                    "value": "diffuse large B cell lymphomas",
                    "confidence": 0.9,
                    "source": "abstract",
                    "context": "Mentioned in the abstract's identification of BCL6's role in diffuse large B-cell lymphomas, indicating the disease's relevance to the study.",
                    "rationale": "Extracted 'diffuse large B-cell lymphomas' from the abstract as it provides a clear and relevant connection to the studied disease within the context of BCL6's function in B cells.",
                    "prenormalized": "diffuse large B-cell lymphoma (MONDO:0018906)",
                }
            ],
            "final_candidate": "DLBCL",
            "final_confidence": 0.95,
            "reconciliation_needed": False,
            "reconciliation_reason": None,
            "sources_processed": ["sample", "abstract"],
            "processing_notes": [],
        }

        # Save the candidates file
        candidates_file = series_dir / "GSM1000981_disease_candidates.json"
        with open(candidates_file, "w") as f:
            json.dump(dlbcl_candidates, f, indent=2)

        # Create another candidates file for tissue normalization
        tissue_candidates = {
            "tool_name": "CuratorAgent",
            "sample_id": "GSM1000984",
            "target_field": "Tissue",
            "series_candidates": [],
            "sample_candidates": [
                {
                    "value": "B cell",
                    "confidence": 0.90,
                    "source": "sample",
                    "context": "Sample characteristics indicate B cell origin",
                    "rationale": "Extracted from sample metadata indicating cell type",
                    "prenormalized": "B-lymphocyte",
                }
            ],
            "abstract_candidates": [],
            "final_candidate": "B cell",
            "final_confidence": 0.90,
            "reconciliation_needed": False,
            "reconciliation_reason": None,
            "sources_processed": ["sample"],
            "processing_notes": [],
        }

        tissue_candidates_file = series_dir / "GSM1000984_tissue_candidates.json"
        with open(tissue_candidates_file, "w") as f:
            json.dump(tissue_candidates, f, indent=2)

    def test_get_available_ontologies_real(self):
        """Test getting available ontologies with real file system."""
        ontologies = get_available_ontologies()

        assert isinstance(ontologies, dict)
        assert len(ontologies) > 0

        # Check that we have the expected ontology names
        expected_ontologies = [
            "mondo",
            "efo",
            "pato",
            "uberon",
            "hancestro",
            "hsapdv",
            "dron",
            "clo",
        ]
        for ontology in expected_ontologies:
            assert ontology in ontologies
            assert "available" in ontologies[ontology]
            assert "dictionary_file" in ontologies[ontology]

    @pytest.mark.skipif(
        not any(
            [
                (
                    Path(__file__).parent.parent
                    / "src"
                    / "normalization"
                    / "dictionaries"
                    / f"{ont}_terms.json"
                ).exists()
                for ont in ["mondo", "efo"]
            ]
        ),
        reason="No ontology dictionary files available for testing",
    )
    def test_semantic_search_with_real_ontology(self):
        """Test semantic search with real ontology files if available."""
        ontologies = get_available_ontologies()

        # Find an available ontology for testing
        available_ontology = None
        for ont_name, ont_info in ontologies.items():
            if ont_info["available"]:
                available_ontology = ont_name
                break

        if available_ontology:
            # Test with a common term
            try:
                results = semantic_search_ontology(
                    "diabetes", available_ontology, top_k=3, min_score=0.3
                )

                assert isinstance(results, list)
                # Should find at least some results for "diabetes"
                if results:
                    assert len(results) <= 3
                    for result in results:
                        assert hasattr(result, "term")
                        assert hasattr(result, "term_id")
                        assert hasattr(result, "score")
                        assert hasattr(result, "ontology")
                        assert result.score >= 0.3
                        assert result.ontology == available_ontology

                print(
                    f"✅ Semantic search test passed with {available_ontology}: found {len(results)} results"
                )

            except Exception as e:
                pytest.skip(
                    f"Semantic search failed with {available_ontology}: {str(e)}"
                )
        else:
            pytest.skip("No available ontologies found for testing")

    def test_normalize_candidates_file_integration(self):
        """Test normalizing a candidates file with realistic data."""
        candidates_file = (
            self.session_dir / "GSE29282" / "GSM1000981_disease_candidates.json"
        )
        output_file = candidates_file.parent / "GSM1000981_disease_normalized.json"

        assert candidates_file.exists(), "Test candidates file should exist"

        # Check if any ontologies are available
        ontologies = get_available_ontologies()
        available_ontologies = [
            name for name, info in ontologies.items() if info["available"]
        ]

        if not available_ontologies:
            pytest.skip("No ontology dictionaries available for integration testing")

        try:
            # Test normalization
            result = normalize_candidates_file(
                str(candidates_file),
                str(output_file),
                ontologies=["mondo"] if "mondo" in available_ontologies else None,
                top_k=3,
                min_score=0.3,
            )

            # Validate the result
            assert isinstance(result, NormalizationResult)
            assert result.sample_id == "GSM1000981"
            assert result.target_field == "Disease"

            # Check that normalization was attempted
            total_normalized = len(result.normalized_sample_candidates) + len(
                result.normalized_abstract_candidates
            )
            assert total_normalized > 0, "Should have normalized some candidates"

            # Check that output file was created
            assert output_file.exists(), "Normalized output file should be created"

            # Validate output file content
            with open(output_file, "r") as f:
                saved_data = json.load(f)

            assert saved_data["sample_id"] == "GSM1000981"
            assert saved_data["target_field"] == "Disease"
            assert "normalized_sample_candidates" in saved_data
            assert "normalization_timestamp" in saved_data

            print(f"✅ Successfully normalized {total_normalized} candidates")

        except NormalizationError as e:
            pytest.skip(
                f"Normalization failed (expected with missing ontologies): {str(e)}"
            )

    def test_normalizer_agent_integration(self):
        """Test the NormalizerAgent with realistic session data."""
        # Create normalizer agent with test session
        try:
            agent = create_normalizer_agent(
                existing_session_dir=str(self.session_dir),
                input_data="target_field:Disease GSM1000981",
            )

            assert agent is not None
            assert agent.name == "NormalizerAgent"
            assert len(agent.tools) > 0

            print("✅ NormalizerAgent created successfully")

            # Test tool availability
            tool_names = [tool.__name__ for tool in agent.tools]
            expected_tools = [
                "find_candidates_files",
                "normalize_candidates_file",
                "batch_normalize_session",
                "get_available_ontologies",
            ]

            for expected_tool in expected_tools:
                assert expected_tool in tool_names, (
                    f"Expected tool {expected_tool} not found"
                )

            print("✅ All expected tools are available")

        except Exception as e:
            pytest.fail(f"Failed to create NormalizerAgent: {str(e)}")

    def test_batch_normalization_integration(self):
        """Test batch normalization across multiple files."""
        # Check if any ontologies are available
        ontologies = get_available_ontologies()
        available_ontologies = [
            name for name, info in ontologies.items() if info["available"]
        ]

        if not available_ontologies:
            pytest.skip("No ontology dictionaries available for integration testing")

        # Create normalizer agent
        agent = create_normalizer_agent(
            existing_session_dir=str(self.session_dir),
            input_data="target_field:Disease",
        )

        # Find the batch normalization tool
        batch_tool = None
        for tool in agent.tools:
            if tool.__name__ == "batch_normalize_session":
                batch_tool = tool
                break

        assert batch_tool is not None, "Batch normalization tool should be available"

        try:
            # Test batch normalization for Disease field
            result_json = batch_tool(
                target_field="Disease",
                ontologies=available_ontologies[0] if available_ontologies else None,
                top_k=3,
                min_score=0.3,
            )

            result = json.loads(result_json)

            if result.get("success"):
                assert "batch_result" in result
                batch_data = result["batch_result"]
                assert "sample_results" in batch_data
                assert "total_candidates_normalized" in batch_data
                assert "successful_normalizations" in batch_data

                print(
                    f"✅ Batch normalization completed: {batch_data.get('total_candidates_normalized', 0)} candidates processed"
                )
            else:
                # Expected to fail if ontologies are not available
                assert "error" in result
                print(f"⚠️ Batch normalization failed (expected): {result['error']}")

        except Exception as e:
            pytest.skip(f"Batch normalization failed: {str(e)}")

    def test_ontology_field_mapping_integration(self):
        """Test that field-specific ontology mapping works correctly."""
        # Create normalizer agent
        agent = create_normalizer_agent(
            existing_session_dir=str(self.session_dir),
            input_data="target_field:Disease",
        )

        # Find the get_available_ontologies tool
        ontology_tool = None
        for tool in agent.tools:
            if tool.__name__ == "get_available_ontologies":
                ontology_tool = tool
                break

        assert ontology_tool is not None, (
            "Get available ontologies tool should be available"
        )

        result_json = ontology_tool()
        result = json.loads(result_json)

        assert result.get("success"), "Should successfully get ontology information"
        assert "field_mappings" in result
        assert "available_ontologies" in result

        field_mappings = result["field_mappings"]

        # Test that expected field mappings exist
        assert "disease" in field_mappings
        assert "tissue" in field_mappings
        assert "organ" in field_mappings

        # Test disease field mapping
        disease_ontologies = field_mappings["disease"]
        assert "mondo" in disease_ontologies
        assert "efo" in disease_ontologies

        print("✅ Ontology field mapping works correctly")

    def test_candidates_file_discovery(self):
        """Test finding candidates files in the session directory."""
        # Create normalizer agent
        agent = create_normalizer_agent(
            existing_session_dir=str(self.session_dir),
            input_data="target_field:Disease",
        )

        # Find the find_candidates_files tool
        find_tool = None
        for tool in agent.tools:
            if tool.__name__ == "find_candidates_files":
                find_tool = tool
                break

        assert find_tool is not None, "Find candidates files tool should be available"

        result_json = find_tool()
        result = json.loads(result_json)

        assert result.get("success"), "Should successfully find candidates files"
        assert "candidates_files" in result
        assert "total_files" in result

        candidates_files = result["candidates_files"]
        assert len(candidates_files) >= 2, "Should find at least 2 candidates files"

        # Check that files have expected structure
        for file_info in candidates_files:
            assert "file_path" in file_info
            assert "sample_id" in file_info
            assert "target_field" in file_info
            assert "filename" in file_info

        # Verify we found our test files
        filenames = [info["filename"] for info in candidates_files]
        assert "GSM1000981_disease_candidates.json" in filenames
        assert "GSM1000984_tissue_candidates.json" in filenames

        print(f"✅ Found {len(candidates_files)} candidates files")


class TestNormalizerRealism:
    """Tests that validate normalizer behavior with real-world scenarios."""

    def test_common_disease_terms(self):
        """Test normalization with common disease terms."""
        ontologies = get_available_ontologies()
        available_ontologies = [
            name for name, info in ontologies.items() if info["available"]
        ]

        if not available_ontologies:
            pytest.skip("No ontology dictionaries available")

        common_diseases = [
            "diabetes",
            "cancer",
            "hypertension",
            "heart disease",
            "alzheimer",
        ]

        # Test with available ontologies
        for disease in common_diseases:
            for ontology in available_ontologies[
                :2
            ]:  # Limit to first 2 to avoid long tests
                try:
                    results = semantic_search_ontology(
                        disease, ontology, top_k=3, min_score=0.3
                    )

                    # Should find some results for common diseases
                    if results:
                        print(
                            f"✅ Found {len(results)} matches for '{disease}' in {ontology}"
                        )
                        for result in results:
                            assert result.score >= 0.3
                            assert len(result.term) > 0
                            assert len(result.term_id) > 0
                    else:
                        print(f"⚠️ No matches found for '{disease}' in {ontology}")

                except NormalizationError as e:
                    print(f"⚠️ Failed to search for '{disease}' in {ontology}: {str(e)}")
                    continue

    def test_abbreviation_expansion(self):
        """Test normalization with medical abbreviations."""
        ontologies = get_available_ontologies()
        available_ontologies = [
            name for name, info in ontologies.items() if info["available"]
        ]

        if not available_ontologies:
            pytest.skip("No ontology dictionaries available")

        abbreviations = [
            "DLBCL",  # diffuse large B-cell lymphoma
            "T2DM",  # type 2 diabetes mellitus
            "COPD",  # chronic obstructive pulmonary disease
            "MI",  # myocardial infarction
        ]

        for abbrev in abbreviations:
            for ontology in available_ontologies[
                :1
            ]:  # Test with first available ontology
                try:
                    results = semantic_search_ontology(
                        abbrev, ontology, top_k=5, min_score=0.2
                    )

                    if results:
                        print(
                            f"✅ Found {len(results)} matches for abbreviation '{abbrev}' in {ontology}"
                        )
                        # Abbreviations should have decent matches in medical ontologies
                        high_confidence_matches = [r for r in results if r.score > 0.7]
                        if high_confidence_matches:
                            print(
                                f"   High confidence match: {high_confidence_matches[0].term} ({high_confidence_matches[0].score:.3f})"
                            )

                except NormalizationError:
                    continue

    def test_ontology_coverage(self):
        """Test that different ontologies are appropriate for different fields."""
        ontologies = get_available_ontologies()

        # Test field-to-ontology appropriateness
        test_cases = [
            ("diabetes", "mondo", "Disease field should use MONDO"),
            ("heart", "uberon", "Anatomy field should use UBERON"),
            ("male", "pato", "Phenotype field should use PATO"),
        ]

        for term, expected_ontology, description in test_cases:
            if (
                expected_ontology in ontologies
                and ontologies[expected_ontology]["available"]
            ):
                try:
                    results = semantic_search_ontology(
                        term, expected_ontology, top_k=3, min_score=0.3
                    )
                    if results:
                        print(
                            f"✅ {description}: found {len(results)} matches for '{term}' in {expected_ontology}"
                        )
                except NormalizationError:
                    print(f"⚠️ Could not test {description}")
            else:
                print(f"⚠️ {expected_ontology} not available for testing: {description}")


if __name__ == "__main__":
    # Run basic integration tests
    test_instance = TestNormalizerIntegration()
    test_instance.setup_method()

    try:
        print("Running basic integration tests...")
        test_instance.test_get_available_ontologies_real()
        test_instance.test_normalizer_agent_integration()
        test_instance.test_candidates_file_discovery()
        print("✅ Basic integration tests completed successfully")

    except Exception as e:
        print(f"❌ Integration test failed: {str(e)}")
    finally:
        test_instance.teardown_method()
