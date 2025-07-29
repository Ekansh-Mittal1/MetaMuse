#!/usr/bin/env python3
"""
Debug script for testing hybrid pipeline stochastic behavior.
"""

import sys
import os

sys.path.append(os.path.abspath("."))


def test_hybrid_pipeline():
    """Test the hybrid pipeline with debug output."""
    try:
        print("🔧 Starting hybrid pipeline debug test...")

        # Import the hybrid pipeline function
        from src.workflows.hybrid_pipeline import run_hybrid_pipeline

        # Test parameters
        input_text = "GSM1000981 target_field=Disease"
        model = "openai/gpt-4o-mini"

        print(f"📝 Input: {input_text}")
        print(f"🤖 Model: {model}")
        print("=" * 60)

        # Run the hybrid pipeline
        result = run_hybrid_pipeline(input_text, model=model)

        print("=" * 60)
        print("🔧 Pipeline completed")
        print(f"✅ Success: {result.success}")
        print(f"📝 Message: {result.message}")

        if result.data:
            print(f"📊 Sample IDs: {result.data.get('sample_ids_for_curation', [])}")
            print(f"📁 Session: {result.data.get('session_directory', 'N/A')}")

        return result

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return None


if __name__ == "__main__":
    test_hybrid_pipeline()
