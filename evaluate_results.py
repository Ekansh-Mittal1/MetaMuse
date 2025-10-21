#!/usr/bin/env python3
"""
Script to compare two batch_results.csv files using semantic similarity via Gemini 2.5 Pro.

This script:
1. Loads two CSV files (ground truth and test)
2. For each sample, compares *_final_candidate fields using Gemini for semantic similarity
3. Treats "None reported" as incorrect (should be "healthy [control]")
4. Outputs comparison results as JSON with percentages per field
5. Parallelizes API requests for faster processing
"""

import argparse
import asyncio
import csv
import json
import sys
from pathlib import Path
from typing import Dict, List
from datetime import datetime

from dotenv import load_dotenv
from pydantic import BaseModel, Field
from tqdm import tqdm
from src.evaluation.gemini_client import GeminiClient


# Target fields to compare (only final_candidate fields)
TARGET_FIELDS = [
    "disease_final_candidate",
    "tissue_final_candidate",
    "organ_final_candidate",
    "cell_line_final_candidate",
    "cell_type_final_candidate",
    "developmental_stage_final_candidate",
    "ethnicity_final_candidate",
    "gender_final_candidate",
    "age_final_candidate",
    "assay_type_final_candidate",
    "treatment_final_candidate",
]


class FieldComparison(BaseModel):
    """Pydantic model for Gemini's field comparison output."""
    
    are_semantically_equivalent: bool = Field(
        description="Whether the two values are semantically equivalent"
    )
    confidence: float = Field(
        description="Confidence score between 0.0 and 1.0"
    )
    reasoning: str = Field(
        description="Brief explanation of why they are or aren't equivalent"
    )


class SampleComparison(BaseModel):
    """Comparison results for a single sample."""
    
    sample_id: str
    field_name: str
    ground_truth_value: str
    test_value: str
    match: bool
    confidence: float
    reasoning: str
    ground_truth_is_none_reported: bool
    test_is_none_reported: bool


def load_csv_data(csv_path: Path) -> Dict[str, Dict[str, str]]:
    """
    Load CSV file and return a dictionary mapping sample_id to row data.
    
    Parameters
    ----------
    csv_path : Path
        Path to the CSV file
        
    Returns
    -------
    Dict[str, Dict[str, str]]
        Dictionary with sample_id as key and row data as value
    """
    data = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        # Strip fieldnames to handle files with spaces around commas
        if reader.fieldnames:
            reader.fieldnames = [field.strip() for field in reader.fieldnames]
        
        for row in reader:
            sample_id = row.get("sample_id", "").strip()
            if sample_id:
                data[sample_id] = row
    
    return data


def normalize_value(value: str) -> str:
    """
    Normalize a field value for comparison.
    
    Parameters
    ----------
    value : str
        The value to normalize
        
    Returns
    -------
    str
        Normalized value
    """
    if not value:
        return ""
    return value.strip()


def is_none_reported(value: str) -> bool:
    """
    Check if a value is considered "None reported".
    
    Parameters
    ----------
    value : str
        The value to check
        
    Returns
    -------
    bool
        True if the value is "None reported" or empty
    """
    normalized = value.strip().lower()
    return normalized in ["", "none reported", "none", "n/a", "na"]


async def compare_field_values_async(
    ground_truth: str,
    test_value: str,
    field_name: str,
    client: GeminiClient,
    semaphore: asyncio.Semaphore
) -> FieldComparison:
    """
    Use Gemini to semantically compare two field values (async version).
    
    Parameters
    ----------
    ground_truth : str
        The ground truth value
    test_value : str
        The test value to compare
    field_name : str
        Name of the field being compared
    client : GeminiClient
        Gemini client for API calls
    semaphore : asyncio.Semaphore
        Semaphore to control concurrency
        
    Returns
    -------
    FieldComparison
        Comparison result from Gemini
    """
    # Normalize values
    gt_norm = normalize_value(ground_truth)
    test_norm = normalize_value(test_value)
    
    # Special handling for None reported cases
    gt_is_none = is_none_reported(gt_norm)
    test_is_none = is_none_reported(test_norm)
    
    # If ground truth is "None reported", it should be "healthy [control]"
    if gt_is_none:
        gt_norm = "healthy [control]"
    
    # If test is "None reported", it's automatically wrong
    if test_is_none and not gt_is_none:
        return FieldComparison(
            are_semantically_equivalent=False,
            confidence=1.0,
            reasoning="Test value is 'None reported' but ground truth has a specific value"
        )
    
    # If both are empty/none after normalization, they match
    if not gt_norm and not test_norm:
        return FieldComparison(
            are_semantically_equivalent=True,
            confidence=1.0,
            reasoning="Both values are empty"
        )
    
    # Exact match check first (faster)
    if gt_norm.lower() == test_norm.lower():
        return FieldComparison(
            are_semantically_equivalent=True,
            confidence=1.0,
            reasoning="Exact string match (case-insensitive)"
        )
    
    # Use Gemini for semantic comparison
    system_prompt = f"""You are an expert in biomedical metadata curation. Your task is to determine if two values for the field '{field_name}' are semantically equivalent.

Consider:
- Scientific synonyms (e.g., "lung cancer" = "pulmonary carcinoma")
- Different phrasings of the same concept
- Abbreviations vs full terms (e.g., "COVID-19" = "coronavirus disease 2019")
- Control/healthy designations (e.g., "control", "healthy", "normal")

Be strict but fair. Only mark as equivalent if they truly represent the same biological/clinical concept.

Return your response as JSON with:
- are_semantically_equivalent (boolean)
- confidence (float between 0.0 and 1.0)
- reasoning (string explaining your decision)"""

    user_prompt = f"""Compare these two values for '{field_name}':

Ground Truth: "{gt_norm}"
Test Value: "{test_norm}"

Are they semantically equivalent?"""
    
    async with semaphore:  # Control concurrent API calls
        try:
            # Run the synchronous API call in a thread pool
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: client.generate_structured_json(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    output_model=FieldComparison,
                    temperature=0.0
                )
            )
            return result
        except Exception as e:
            # Fallback to non-match on error
            return FieldComparison(
                are_semantically_equivalent=False,
                confidence=0.0,
                reasoning=f"Error during comparison: {str(e)}"
            )


async def compare_csv_files_async(
    ground_truth_path: Path,
    test_path: Path,
    output_path: Path,
    model_name: str = "google/gemini-2.5-pro",
    max_workers: int = 10
) -> Dict:
    """
    Compare two CSV files and generate comparison report (async with parallelization).
    
    Parameters
    ----------
    ground_truth_path : Path
        Path to the ground truth CSV file
    test_path : Path
        Path to the test CSV file
    output_path : Path
        Path to save the output JSON file
    model_name : str
        Model name to use for comparisons
    max_workers : int
        Maximum number of concurrent API requests (default: 10)
        
    Returns
    -------
    Dict
        Comparison results
    """
    print(f"🔬 Loading ground truth: {ground_truth_path}")
    ground_truth_data = load_csv_data(ground_truth_path)
    
    print(f"🔬 Loading test data: {test_path}")
    test_data = load_csv_data(test_path)
    
    print(f"📊 Ground truth samples: {len(ground_truth_data)}")
    print(f"📊 Test samples: {len(test_data)}")
    
    # Initialize Gemini client
    print(f"🤖 Initializing Gemini client with model: {model_name}")
    print(f"⚡ Using {max_workers} concurrent workers for API calls")
    client = GeminiClient(model_name=model_name)
    
    # Create semaphore for controlling concurrency
    semaphore = asyncio.Semaphore(max_workers)
    
    # Find common samples
    common_sample_ids = set(ground_truth_data.keys()) & set(test_data.keys())
    missing_in_test = set(ground_truth_data.keys()) - set(test_data.keys())
    extra_in_test = set(test_data.keys()) - set(ground_truth_data.keys())
    
    print(f"📊 Common samples: {len(common_sample_ids)}")
    if missing_in_test:
        print(f"⚠️  Samples in ground truth but not in test: {len(missing_in_test)}")
    if extra_in_test:
        print(f"⚠️  Samples in test but not in ground truth: {len(extra_in_test)}")
    
    # Compare each field for each sample - PARALLELIZED
    all_comparisons: List[SampleComparison] = []
    field_stats: Dict[str, Dict] = {}
    
    print(f"\n{'='*80}")
    print(f"🚀 Starting comparisons: {len(TARGET_FIELDS)} fields × {len(common_sample_ids)} samples = {len(TARGET_FIELDS) * len(common_sample_ids)} total comparisons")
    print(f"{'='*80}")
    
    for field_idx, field_name in enumerate(TARGET_FIELDS, 1):
        print(f"\n🔍 [{field_idx}/{len(TARGET_FIELDS)}] Comparing field: {field_name}")
        field_stats[field_name] = {
            "total": 0,
            "matches": 0,
            "mismatches": 0,
            "percentage": 0.0
        }
        
        # Create comparison tasks for all samples (parallel execution)
        comparison_tasks = []
        sample_ids_ordered = sorted(common_sample_ids)
        
        for sample_id in sample_ids_ordered:
            gt_row = ground_truth_data[sample_id]
            test_row = test_data[sample_id]
            
            gt_value = gt_row.get(field_name, "")
            test_value = test_row.get(field_name, "")
            
            # Create comparison task
            task = compare_field_values_async(
                gt_value, test_value, field_name, client, semaphore
            )
            comparison_tasks.append((sample_id, gt_value, test_value, task))
        
        # Execute all comparisons in parallel with progress bar
        # Create tasks list
        tasks = [task for _, _, _, task in comparison_tasks]
        
        # Use tqdm to track progress with detailed description
        # Create a progress bar
        pbar = tqdm(
            total=len(tasks),
            desc=f"  [{field_idx}/{len(TARGET_FIELDS)}] {field_name:40s}",
            unit=" comparison",
            ncols=100,
            colour='green'
        )
        
        # Wrapper to update progress bar as tasks complete
        async def task_with_progress(task):
            result = await task
            pbar.update(1)
            return result
        
        # Execute all tasks with progress tracking
        comparison_results = await asyncio.gather(*[task_with_progress(t) for t in tasks])
        pbar.close()
        
        # Process results
        for idx, ((sample_id, gt_value, test_value, _), comparison) in enumerate(zip(comparison_tasks, comparison_results), 1):
            # Track if values were "None reported"
            gt_is_none = is_none_reported(gt_value)
            test_is_none = is_none_reported(test_value)
            
            # Store result
            sample_comparison = SampleComparison(
                sample_id=sample_id,
                field_name=field_name,
                ground_truth_value=gt_value,
                test_value=test_value,
                match=comparison.are_semantically_equivalent,
                confidence=comparison.confidence,
                reasoning=comparison.reasoning,
                ground_truth_is_none_reported=gt_is_none,
                test_is_none_reported=test_is_none
            )
            
            all_comparisons.append(sample_comparison)
            
            # Update stats
            field_stats[field_name]["total"] += 1
            if comparison.are_semantically_equivalent:
                field_stats[field_name]["matches"] += 1
            else:
                field_stats[field_name]["mismatches"] += 1
        
        # Calculate percentage
        total = field_stats[field_name]["total"]
        if total > 0:
            field_stats[field_name]["percentage"] = (
                field_stats[field_name]["matches"] / total * 100
            )
        
        print(f"  ✅ Matches: {field_stats[field_name]['matches']}/{total} ({field_stats[field_name]['percentage']:.2f}%)")
    
    # Generate final report
    report = {
        "comparison_metadata": {
            "ground_truth_file": str(ground_truth_path),
            "test_file": str(test_path),
            "model_used": model_name,
            "timestamp": datetime.now().isoformat(),
            "total_samples_compared": len(common_sample_ids),
            "samples_only_in_ground_truth": len(missing_in_test),
            "samples_only_in_test": len(extra_in_test)
        },
        "field_statistics": field_stats,
        "overall_accuracy": sum(s["matches"] for s in field_stats.values()) / 
                           sum(s["total"] for s in field_stats.values()) * 100 
                           if sum(s["total"] for s in field_stats.values()) > 0 else 0.0,
        "sample_comparisons": [comp.model_dump() for comp in all_comparisons],
        "mismatched_samples": [
            comp.model_dump() for comp in all_comparisons if not comp.match
        ]
    }
    
    # Save to JSON
    print(f"\n💾 Saving results to: {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    
    # Print summary
    print("\n" + "="*80)
    print("📊 COMPARISON SUMMARY")
    print("="*80)
    print(f"Overall Accuracy: {report['overall_accuracy']:.2f}%")
    print("\nPer-Field Accuracy:")
    for field_name, stats in field_stats.items():
        print(f"  {field_name:40s}: {stats['percentage']:6.2f}% ({stats['matches']}/{stats['total']})")
    print("="*80)
    
    return report


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Compare two batch_results.csv files using semantic similarity (parallelized)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python evaluate_results.py ground_truth.csv test_results.csv
  python evaluate_results.py ground_truth.csv test_results.csv -o comparison_report.json
  python evaluate_results.py ground_truth.csv test_results.csv --model google/gemini-2.5-flash
  python evaluate_results.py ground_truth.csv test_results.csv --max-workers 20
        """
    )
    
    parser.add_argument(
        "ground_truth",
        type=str,
        help="Path to the ground truth CSV file"
    )
    
    parser.add_argument(
        "test_file",
        type=str,
        help="Path to the test CSV file to compare"
    )
    
    parser.add_argument(
        "-o", "--output",
        type=str,
        default="comparison_results.json",
        help="Path to save the comparison results JSON (default: comparison_results.json)"
    )
    
    parser.add_argument(
        "--model",
        type=str,
        default="google/gemini-2.5-pro",
        help="Model to use for semantic comparison (default: google/gemini-2.5-pro)"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of concurrent API requests (default: 10)"
    )
    
    args = parser.parse_args()
    
    # Validate input files
    ground_truth_path = Path(args.ground_truth)
    test_path = Path(args.test_file)
    output_path = Path(args.output)
    
    if not ground_truth_path.exists():
        print(f"❌ Error: Ground truth file not found: {ground_truth_path}")
        sys.exit(1)
    
    if not test_path.exists():
        print(f"❌ Error: Test file not found: {test_path}")
        sys.exit(1)
    
    # Load environment variables
    load_dotenv()
    
    # Run comparison (async)
    try:
        asyncio.run(compare_csv_files_async(
            ground_truth_path=ground_truth_path,
            test_path=test_path,
            output_path=output_path,
            model_name=args.model,
            max_workers=args.max_workers
        ))
        print("\n✅ Comparison completed successfully!")
        return 0
    except Exception as e:
        print(f"\n❌ Error during comparison: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

