#!/usr/bin/env python3
"""Generate a unified errors report from evaluation results."""

import argparse
import json
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd


def collect_false_results(evaluation_dir: str) -> List[Dict[str, Any]]:
    """Collect all false curation results from evaluation JSONs."""
    eval_dir = Path(evaluation_dir)
    false_results = []
    
    for eval_file in eval_dir.glob("*_evaluation.json"):
        try:
            with eval_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            
            sample_id = data.get("sample_id", "")
            series_id = data.get("series_id", "")
            
            for field_eval in data.get("fields", []):
                if field_eval.get("is_curated_correct") is False:
                    false_results.append({
                        "sample_id": sample_id,
                        "series_id": series_id,
                        "field_name": field_eval.get("field_name", ""),
                        "curated_value": field_eval.get("curated_value", ""),
                        "reason": field_eval.get("curated_reason", ""),
                        "evaluation_file": str(eval_file.name),
                    })
                    
        except Exception as e:
            print(f"Error processing {eval_file}: {e}")
    
    return false_results


def generate_errors_report(evaluation_dir: str, output_path: str) -> None:
    """Generate unified errors report."""
    false_results = collect_false_results(evaluation_dir)
    
    if not false_results:
        print("No false curation results found!")
        return
    
    # Create DataFrame for easier analysis
    df = pd.DataFrame(false_results)
    
    # Generate summary statistics
    summary = {
        "total_false_results": len(false_results),
        "samples_with_errors": df["sample_id"].nunique(),
        "fields_with_errors": df["field_name"].value_counts().to_dict(),
        "common_error_patterns": {},
    }
    
    # Analyze common error patterns
    reason_patterns = {}
    for reason in df["reason"]:
        if "empty string" in reason.lower():
            reason_patterns["empty_string_instead_of_none_reported"] = reason_patterns.get("empty_string_instead_of_none_reported", 0) + 1
        elif "incorrect" in reason.lower() and "generic" in reason.lower():
            reason_patterns["too_generic_curation"] = reason_patterns.get("too_generic_curation", 0) + 1
        elif "missing" in reason.lower() or "should be" in reason.lower():
            reason_patterns["missing_expected_value"] = reason_patterns.get("missing_expected_value", 0) + 1
        else:
            reason_patterns["other"] = reason_patterns.get("other", 0) + 1
    
    summary["common_error_patterns"] = reason_patterns
    
    # Create detailed report
    report = {
        "summary": summary,
        "detailed_errors": false_results,
        "recommendations": [
            "Review empty string cases - most should be 'None reported'",
            "Check if curation guidelines are being followed consistently",
            "Consider updating extraction templates for frequently problematic fields",
        ]
    }
    
    # Save report
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    
    print(f"Generated errors report: {output_path}")
    print(f"Total false results: {len(false_results)}")
    print(f"Samples with errors: {df['sample_id'].nunique()}")
    print(f"Fields with most errors: {dict(df['field_name'].value_counts().head())}")


def main():
    parser = argparse.ArgumentParser(description="Generate unified errors report from evaluation results")
    parser.add_argument("evaluation_dir", help="Directory containing evaluation JSON files")
    parser.add_argument("--output", "-o", default="errors_report.json", help="Output file for errors report")
    
    args = parser.parse_args()
    
    generate_errors_report(args.evaluation_dir, args.output)


if __name__ == "__main__":
    main()

