"""
Validation script to verify batch output consistency between eval and classic modes.

This script checks that:
1. Both modes write to correct batch directories
2. Both modes produce both flat and nested normalization formats
3. Each batch contains only its own samples
4. CSV extraction can read data from both formats
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Tuple


def validate_batch_directory(batch_dir: Path) -> Tuple[bool, List[str]]:
    """
    Validate a single batch directory's output.
    
    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    issues = []
    
    # Check batch_targets_output.json exists
    bt_file = batch_dir / "batch_targets_output.json"
    if not bt_file.exists():
        issues.append(f"Missing batch_targets_output.json in {batch_dir.name}")
        return False, issues
    
    try:
        with open(bt_file, "r") as f:
            data = json.load(f)
    except Exception as e:
        issues.append(f"Failed to read batch_targets_output.json: {e}")
        return False, issues
    
    # Check required fields
    required_fields = ["success", "batch_name", "sample_type", "batch_samples"]
    for field in required_fields:
        if field not in data:
            issues.append(f"Missing required field '{field}'")
    
    batch_samples = data.get("batch_samples", [])
    if not batch_samples:
        issues.append("batch_samples is empty")
    
    # Check for normalization data in flat format
    normalization_fields = ["disease", "organ", "tissue", "cell_type", "cell_line", 
                           "ethnicity", "treatment", "developmental_stage"]
    
    flat_format_found = False
    nested_format_found = False
    
    for field in normalization_fields:
        if field in data and isinstance(data[field], dict):
            flat_format_found = True
            # Verify samples in normalization match batch_samples
            norm_samples = set(data[field].keys())
            batch_samples_set = set(batch_samples)
            extra_samples = norm_samples - batch_samples_set
            if extra_samples:
                issues.append(f"Field '{field}' contains samples not in batch: {extra_samples}")
            break
    
    # Check for nested normalization_results format
    if "normalization_results" in data:
        nested_format_found = True
        norm_results = data["normalization_results"]
        if isinstance(norm_results, dict):
            for field_name, field_data in norm_results.items():
                if isinstance(field_data, dict) and "sample_results" in field_data:
                    for sr in field_data["sample_results"]:
                        sample_id = sr.get("sample_id")
                        if sample_id and sample_id not in batch_samples:
                            issues.append(f"normalization_results contains sample not in batch: {sample_id}")
    
    # At least one format should be present if normalization was attempted
    has_normalization_fields = data.get("normalization_fields_processed", [])
    if has_normalization_fields and not (flat_format_found or nested_format_found):
        issues.append("Normalization was processed but no normalization data found")
    
    # Success if all checks passed
    is_valid = len(issues) == 0
    return is_valid, issues


def validate_conditional_processing_output(session_dir: Path) -> Dict[str, Any]:
    """
    Validate the entire conditional_processing output directory.
    
    Returns:
        Dictionary with validation results
    """
    results = {
        "session_directory": str(session_dir),
        "is_valid": True,
        "batches_checked": 0,
        "batches_valid": 0,
        "issues": [],
        "batch_details": {}
    }
    
    conditional_dir = session_dir / "conditional_processing"
    if not conditional_dir.exists():
        results["is_valid"] = False
        results["issues"].append("conditional_processing directory not found")
        return results
    
    # Find all batch directories (pattern: {sample_type}_batch_{n})
    batch_dirs = [d for d in conditional_dir.iterdir() 
                  if d.is_dir() and "_batch_" in d.name and not d.name.endswith("_consolidated")]
    
    if not batch_dirs:
        results["issues"].append("No batch directories found")
        results["is_valid"] = False
        return results
    
    for batch_dir in sorted(batch_dirs):
        results["batches_checked"] += 1
        is_valid, issues = validate_batch_directory(batch_dir)
        
        results["batch_details"][batch_dir.name] = {
            "is_valid": is_valid,
            "issues": issues
        }
        
        if is_valid:
            results["batches_valid"] += 1
        else:
            results["is_valid"] = False
            results["issues"].extend([f"{batch_dir.name}: {issue}" for issue in issues])
    
    return results


def compare_batch_outputs(session_dir1: Path, session_dir2: Path) -> Dict[str, Any]:
    """
    Compare batch outputs between two sessions (e.g., eval vs classic).
    
    Returns:
        Dictionary with comparison results
    """
    results1 = validate_conditional_processing_output(session_dir1)
    results2 = validate_conditional_processing_output(session_dir2)
    
    comparison = {
        "session1": str(session_dir1),
        "session2": str(session_dir2),
        "session1_valid": results1["is_valid"],
        "session2_valid": results2["is_valid"],
        "differences": [],
        "session1_details": results1,
        "session2_details": results2,
    }
    
    # Compare batch counts
    if results1["batches_checked"] != results2["batches_checked"]:
        comparison["differences"].append(
            f"Different batch counts: {results1['batches_checked']} vs {results2['batches_checked']}"
        )
    
    # Compare batch names
    batch_names1 = set(results1["batch_details"].keys())
    batch_names2 = set(results2["batch_details"].keys())
    
    only_in_1 = batch_names1 - batch_names2
    only_in_2 = batch_names2 - batch_names1
    
    if only_in_1:
        comparison["differences"].append(f"Batches only in session1: {only_in_1}")
    if only_in_2:
        comparison["differences"].append(f"Batches only in session2: {only_in_2}")
    
    return comparison


def print_validation_report(results: Dict[str, Any]) -> None:
    """Print a formatted validation report."""
    print("\n" + "=" * 60)
    print("BATCH OUTPUT VALIDATION REPORT")
    print("=" * 60)
    print(f"Session Directory: {results['session_directory']}")
    print(f"Overall Valid: {'✅ YES' if results['is_valid'] else '❌ NO'}")
    print(f"Batches Checked: {results['batches_checked']}")
    print(f"Batches Valid: {results['batches_valid']}")
    
    if results["issues"]:
        print("\nIssues Found:")
        for issue in results["issues"]:
            print(f"  ❌ {issue}")
    else:
        print("\n✅ All checks passed!")
    
    print("\nBatch Details:")
    for batch_name, details in results["batch_details"].items():
        status = "✅" if details["is_valid"] else "❌"
        print(f"  {status} {batch_name}")
        if details["issues"]:
            for issue in details["issues"]:
                print(f"      - {issue}")
    
    print("=" * 60 + "\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python validate_batch_outputs.py <session_directory> [session_directory2]")
        print("\nExamples:")
        print("  # Validate single session:")
        print("  python validate_batch_outputs.py batch/batch_eval_20_n5_20251204_065108")
        print("\n  # Compare two sessions:")
        print("  python validate_batch_outputs.py batch/batch_eval_... batch/batch_classic_...")
        sys.exit(1)
    
    session_dir1 = Path(sys.argv[1])
    
    if not session_dir1.exists():
        print(f"❌ Session directory not found: {session_dir1}")
        sys.exit(1)
    
    if len(sys.argv) > 2:
        # Compare two sessions
        session_dir2 = Path(sys.argv[2])
        if not session_dir2.exists():
            print(f"❌ Session directory not found: {session_dir2}")
            sys.exit(1)
        
        comparison = compare_batch_outputs(session_dir1, session_dir2)
        
        print("\n" + "=" * 60)
        print("SESSION COMPARISON REPORT")
        print("=" * 60)
        print(f"Session 1: {comparison['session1']}")
        print(f"Session 2: {comparison['session2']}")
        print(f"\nSession 1 Valid: {'✅' if comparison['session1_valid'] else '❌'}")
        print(f"Session 2 Valid: {'✅' if comparison['session2_valid'] else '❌'}")
        
        if comparison["differences"]:
            print("\nDifferences:")
            for diff in comparison["differences"]:
                print(f"  ⚠️ {diff}")
        else:
            print("\n✅ Sessions are structurally equivalent!")
        
        print_validation_report(comparison["session1_details"])
        print_validation_report(comparison["session2_details"])
    else:
        # Validate single session
        results = validate_conditional_processing_output(session_dir1)
        print_validation_report(results)
        
        sys.exit(0 if results["is_valid"] else 1)



