#!/usr/bin/env python3
"""
Simple test script to verify PrimarySampleExtractedCandidate boolean constraint.
"""

from src.models.curation_models import PrimarySampleExtractedCandidate, PrimarySampleCurationResult
from src.models.normalization_models import PrimarySampleNormalizationResult
from src.tools.normalizer_tools import NormalizationError, normalize_candidate_value
from src.models.curation_models import ExtractedCandidate

def test_primary_sample_extracted_candidate():
    """Test that PrimarySampleExtractedCandidate enforces boolean values."""
    print("Testing PrimarySampleExtractedCandidate boolean constraint...")
    
    # Test valid boolean values
    try:
        candidate_true = PrimarySampleExtractedCandidate(
            value=True,
            confidence=0.95,
            source="sample",
            context="patient identifier",
            rationale="Clear patient identifier found",
            prenormalized="primary_sample (TRUE)"
        )
        print("✓ True value accepted")
        
        candidate_false = PrimarySampleExtractedCandidate(
            value=False,
            confidence=0.98,
            source="sample",
            context="cell line name",
            rationale="Clear cell line identifier found",
            prenormalized="cell_line (FALSE)"
        )
        print("✓ False value accepted")
        
    except Exception as e:
        print(f"✗ Error creating PrimarySampleExtractedCandidate: {e}")
        return False
    
    # Test invalid string value (should fail)
    try:
        candidate_invalid = PrimarySampleExtractedCandidate(
            value="true",  # String instead of boolean
            confidence=0.95,
            source="sample",
            context="test",
            rationale="test",
            prenormalized="test"
        )
        print("✗ String value should have been rejected")
        return False
    except Exception as e:
        print("✓ String value correctly rejected")
    
    return True

def test_primary_sample_curation_result():
    """Test that PrimarySampleCurationResult works with boolean candidates."""
    print("\nTesting PrimarySampleCurationResult...")
    
    try:
        candidates = [
            PrimarySampleExtractedCandidate(
                value=True,
                confidence=0.95,
                source="sample",
                context="patient identifier",
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
        
        print("✓ PrimarySampleCurationResult created successfully")
        print(f"  - Sample ID: {result.sample_id}")
        print(f"  - Is Primary Sample: {result.is_primary_sample}")
        print(f"  - Confidence: {result.confidence}")
        print(f"  - Candidates: {len(result.sample_candidates)}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error creating PrimarySampleCurationResult: {e}")
        return False

def test_normalizer_protection():
    """Test that normalizer correctly rejects PrimarySample target field."""
    print("\nTesting normalizer protection...")
    
    try:
        candidate = ExtractedCandidate(
            value="true",
            confidence=0.95,
            source="sample",
            context="test context",
            rationale="test rationale",
            prenormalized="test"
        )
        
        # This should raise an error
        normalize_candidate_value(
            candidate=candidate,
            target_field="PrimarySample",
            ontologies=["mondo"],
            top_k=2,
            min_score=0.5
        )
        
        print("✗ Normalizer should have rejected PrimarySample target field")
        return False
        
    except NormalizationError as e:
        print("✓ Normalizer correctly rejected PrimarySample target field")
        print(f"  - Error message: {e}")
        return True
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

def main():
    """Run all tests."""
    print("Testing PrimarySample target field implementation...\n")
    
    tests = [
        test_primary_sample_extracted_candidate,
        test_primary_sample_curation_result,
        test_normalizer_protection
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! PrimarySample implementation is working correctly.")
    else:
        print("❌ Some tests failed. Please check the implementation.")

if __name__ == "__main__":
    main() 