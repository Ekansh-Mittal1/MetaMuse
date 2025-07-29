#!/usr/bin/env python3
"""
Comprehensive analysis of stochastic behavior in hybrid pipeline.
"""

import sys
import os
import time
import json
from pathlib import Path

sys.path.append(os.path.abspath("."))


def run_multiple_tests(num_tests=3):
    """Run multiple tests to analyze stochastic behavior."""
    try:
        from src.workflows.hybrid_pipeline import run_hybrid_pipeline

        print("🔬 STOCHASTIC BEHAVIOR ANALYSIS")
        print("=" * 60)
        print(f"Running {num_tests} tests to analyze repeated tool call behavior")
        print()

        results = []

        for i in range(num_tests):
            print(f"🔧 TEST {i + 1}/{num_tests}")
            print("-" * 40)

            try:
                start_time = time.time()

                # Test parameters
                input_text = "GSM1000981 target_field=Disease"
                model = "openai/gpt-4o-mini"

                print(f"📝 Input: {input_text}")
                print(f"🤖 Model: {model}")
                print(f"⏰ Starting at: {time.strftime('%H:%M:%S')}")

                # Run the hybrid pipeline
                result = run_hybrid_pipeline(input_text, model=model)

                end_time = time.time()
                duration = end_time - start_time

                test_result = {
                    "test_number": i + 1,
                    "success": result.success if result else False,
                    "duration_seconds": duration,
                    "message": result.message if result else "Failed to get result",
                    "session_dir": result.data.get("session_directory")
                    if result and result.data
                    else None,
                    "sample_ids": result.data.get("sample_ids_for_curation", [])
                    if result and result.data
                    else [],
                }

                results.append(test_result)

                print(f"✅ Test {i + 1} completed in {duration:.1f}s")
                print(f"📊 Success: {test_result['success']}")
                print(f"📁 Session: {test_result['session_dir']}")
                print()

                # Check if output files were created
                if test_result["session_dir"]:
                    session_path = Path(test_result["session_dir"])
                    if session_path.exists():
                        output_files = list(
                            session_path.glob("*_disease_candidates.json")
                        )
                        print(f"📄 Output files: {len(output_files)}")
                        if output_files:
                            print(f"📄 File created: {output_files[0].name}")
                    print()

                # Small delay between tests
                if i < num_tests - 1:
                    print("⏳ Waiting 2 seconds before next test...")
                    time.sleep(2)

            except Exception as e:
                print(f"❌ Test {i + 1} failed: {e}")
                import traceback

                traceback.print_exc()
                results.append(
                    {
                        "test_number": i + 1,
                        "success": False,
                        "error": str(e),
                        "duration_seconds": time.time() - start_time
                        if "start_time" in locals()
                        else 0,
                    }
                )
                print()

        # Summary
        print("📊 ANALYSIS SUMMARY")
        print("=" * 60)

        successful_tests = [r for r in results if r["success"]]
        failed_tests = [r for r in results if not r["success"]]

        print(f"✅ Successful tests: {len(successful_tests)}/{num_tests}")
        print(f"❌ Failed tests: {len(failed_tests)}/{num_tests}")

        if successful_tests:
            avg_duration = sum(r["duration_seconds"] for r in successful_tests) / len(
                successful_tests
            )
            print(f"⏱️  Average duration: {avg_duration:.1f}s")

        # Save results
        results_file = f"stochastic_analysis_{int(time.time())}.json"
        with open(results_file, "w") as f:
            json.dump(
                {
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "num_tests": num_tests,
                    "results": results,
                    "summary": {
                        "successful": len(successful_tests),
                        "failed": len(failed_tests),
                        "avg_duration": avg_duration if successful_tests else 0,
                    },
                },
                f,
                indent=2,
            )

        print(f"📄 Results saved to: {results_file}")

        return results

    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback

        traceback.print_exc()
        return []


if __name__ == "__main__":
    run_multiple_tests()
