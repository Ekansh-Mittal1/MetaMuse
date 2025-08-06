#!/usr/bin/env python3
"""
Test script for error tracking functionality.
"""

import asyncio
import json
from pathlib import Path
from src.workflows.batch_samples import BatchSamplesProcessor

async def test_error_tracking():
    """Test the error tracking functionality."""
    
    # Create a test processor
    processor = BatchSamplesProcessor(
        output_dir="test_batch",
        sample_count=5,
        batch_size=2,
        target_fields=["disease", "tissue"]
    )
    
    # Test tracking different types of errors
    print("Testing error tracking functionality...")
    
    # Test batch error tracking
    processor.track_batch_error(
        batch_num=1,
        error="Test batch error",
        samples=["GSM1234567", "GSM1234568"],
        stage="test_stage"
    )
    
    # Test target field error tracking
    processor.track_target_field_error(
        target_field="disease",
        error="Test target field error",
        samples=["GSM1234567", "GSM1234568"],
        stage="curation"
    )
    
    # Test sample error tracking
    processor.track_sample_error(
        sample_id="GSM1234567",
        error="Test sample error",
        stage="normalization"
    )
    
    # Test stage error tracking
    processor.track_stage_error(
        stage="data_intake",
        error="Test stage error",
        affected_items=["all_samples"]
    )
    
    # Generate and save error summary
    error_summary = processor.generate_error_summary()
    processor.save_error_summary()
    
    print("Error summary generated:")
    print(json.dumps(error_summary, indent=2))
    
    # Check if files were created
    test_dir = Path("test_batch")
    if test_dir.exists():
        print(f"\nFiles created in {test_dir}:")
        for file in test_dir.rglob("*.json"):
            print(f"  - {file.name}")
    
    print("\n✅ Error tracking test completed successfully!")

if __name__ == "__main__":
    asyncio.run(test_error_tracking()) 