#!/usr/bin/env python3
"""
Test script to verify CuratorOutput changes work correctly.
"""

from src.models.agent_outputs import CuratorOutput
from src.models.curation_models import CurationResult, ExtractedCandidate


def test_curator_output_with_curation_results():
    """Test that CuratorOutput can handle CurationResult objects."""

    # Create a sample CurationResult
    curation_result = CurationResult(
        sample_id="GSM1000981",
        target_field="Disease",
        series_candidates=[
            ExtractedCandidate(
                value="breast cancer",
                confidence=0.85,
                source="series",
                context="Found in series title",
                rationale="Direct mention in title",
                prenormalized="breast carcinoma (MONDO:0007254)",
            )
        ],
        final_candidate="breast cancer",
        final_confidence=0.85,
        sources_processed=["series"],
        processing_notes=["Successfully processed"],
    )

    # Create CuratorOutput with the CurationResult
    curator_output = CuratorOutput(
        success=True,
        message="Successfully curated Disease for 1 samples",
        execution_time_seconds=10.5,
        sample_ids_requested=["GSM1000981"],
        target_field="Disease",
        session_directory="/test/session",
        curation_results=[curation_result],
        total_samples_processed=1,
        successful_curations=1,
        samples_needing_review=0,
        average_confidence=0.85,
    )

    # Test that it can be serialized to JSON
    output_dict = curator_output.model_dump()
    print("✅ CuratorOutput created successfully with CurationResult objects")
    print(f"✅ curation_results field type: {type(curator_output.curation_results)}")
    print(f"✅ Number of curation results: {len(curator_output.curation_results)}")
    print(f"✅ First result sample_id: {curator_output.curation_results[0].sample_id}")
    print(
        f"✅ First result target_field: {curator_output.curation_results[0].target_field}"
    )
    print(
        f"✅ First result final_candidate: {curator_output.curation_results[0].final_candidate}"
    )

    # Test JSON serialization
    json_str = curator_output.model_dump_json()
    print("✅ CuratorOutput can be serialized to JSON")

    # Test JSON deserialization
    parsed_output = CuratorOutput.model_validate_json(json_str)
    print("✅ CuratorOutput can be deserialized from JSON")
    print(
        f"✅ Parsed output has {len(parsed_output.curation_results)} curation results"
    )

    return True


if __name__ == "__main__":
    try:
        test_curator_output_with_curation_results()
        print("\n🎉 All tests passed! CuratorOutput changes work correctly.")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
