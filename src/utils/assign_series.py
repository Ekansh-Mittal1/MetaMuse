"""
Script to assign series IDs to sample IDs from Age.txt file.

This script reads sample IDs from Age.txt, randomly samples 1000 of them,
and uses ingestion tools to extract series IDs for each sample, creating
a dataframe with both sample_id and series_id.
"""

import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import json
from typing import List, Dict, Any
import traceback
from tqdm import tqdm
import random

# Import ingestion tools
from src.tools.ingestion_tools import get_gsm_metadata


def read_sample_ids_from_file(file_path: str, sample_size: int = 1000) -> List[str]:
    """
    Read sample IDs from the Age.txt file and randomly sample a subset.

    Parameters
    ----------
    file_path : str
        Path to the Age.txt file containing sample IDs
    sample_size : int
        Number of sample IDs to randomly select (default: 1000)

    Returns
    -------
    List[str]
        List of randomly sampled sample IDs
    """
    try:
        with open(file_path, "r") as f:
            # Read lines and strip whitespace, filter out empty lines
            all_sample_ids = [line.strip() for line in f.readlines() if line.strip()]

        print(f"📋 Read {len(all_sample_ids)} total sample IDs from {file_path}")

        # Randomly sample the specified number of IDs
        if len(all_sample_ids) <= sample_size:
            sampled_ids = all_sample_ids
            print(
                f"📊 Using all {len(sampled_ids)} sample IDs (less than requested {sample_size})"
            )
        else:
            sampled_ids = random.sample(all_sample_ids, sample_size)
            print(
                f"📊 Randomly sampled {len(sampled_ids)} sample IDs from {len(all_sample_ids)} total"
            )

        return sampled_ids

    except FileNotFoundError:
        print(f"❌ Error: File not found: {file_path}")
        return []
    except Exception as e:
        print(f"❌ Error reading file {file_path}: {e}")
        return []


def get_series_id_for_sample(sample_id: str) -> str:
    """
    Get series ID for a given sample ID using ingestion tools.

    Parameters
    ----------
    sample_id : str
        The sample ID (e.g., GSM1000981)

    Returns
    -------
    str
        Series ID or None if not found
    """
    try:
        # Use ingestion tools to get GSM metadata directly without saving files
        metadata = get_gsm_metadata(sample_id)

        # Extract series ID from metadata
        attributes = metadata.get("attributes", {})
        series_id = (
            attributes.get("series_id")
            or attributes.get("gse_id")
            or attributes.get("Series ID")
        )

        if series_id:
            # Handle multiple series IDs (separated by commas)
            series_ids = [sid.strip() for sid in series_id.split(",") if sid.strip()]

            # Validate series IDs
            valid_series_ids = []
            for sid in series_ids:
                if sid.upper().startswith("GSE") and sid[3:].isdigit():
                    valid_series_ids.append(sid.upper())

            if valid_series_ids:
                return valid_series_ids[0]  # Return the first valid series ID

        return None

    except Exception as e:
        # Silently handle errors and return None
        return None


def create_sample_series_dataframe(sample_ids: List[str]) -> pd.DataFrame:
    """
    Create a dataframe with sample IDs and their corresponding series IDs.

    Parameters
    ----------
    sample_ids : List[str]
        List of sample IDs

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: sample_id, series_id
    """
    results = []

    # Use tqdm for progress tracking
    for sample_id in tqdm(sample_ids, desc="Processing samples"):
        series_id = get_series_id_for_sample(sample_id)

        results.append({"sample_id": sample_id, "series_id": series_id})

    # Create DataFrame
    df = pd.DataFrame(results)

    # Count results
    found_count = df["series_id"].notna().sum()
    missing_count = len(sample_ids) - found_count

    print(f"\n📈 Results Summary:")
    print(f"   Total samples: {len(sample_ids)}")
    print(f"   Series IDs found: {found_count}")
    print(f"   Series IDs missing: {missing_count}")

    return df


def main():
    """
    Main function to read Age.txt and create sample-series dataframe.
    """
    # File paths
    age_file_path = "/teamspace/studios/this_studio/Age.txt"
    session_dir = "/teamspace/studios/this_studio"

    print("🚀 Starting sample-series assignment process...")
    print(f"📁 Reading sample IDs from: {age_file_path}")

    # Read and randomly sample 1000 sample IDs from Age.txt
    sample_ids = read_sample_ids_from_file(age_file_path, sample_size=1000)

    if not sample_ids:
        print("❌ No sample IDs found. Exiting.")
        return

    # Create dataframe with sample IDs and series IDs
    df = create_sample_series_dataframe(sample_ids)

    # Save results
    output_file = Path(session_dir) / "sample_series_mapping_1000.csv"
    df.to_csv(output_file, index=False)
    print(f"💾 Saved results to: {output_file}")

    # Show summary statistics
    if not df.empty:
        print(f"\n📊 Summary:")
        print(f"   Unique series IDs found: {df['series_id'].nunique()}")
        print(
            f"   Most common series ID: {df['series_id'].mode().iloc[0] if not df['series_id'].mode().empty else 'None'}"
        )

        # Show series ID distribution
        series_counts = df["series_id"].value_counts()
        print(f"\n📈 Series ID distribution:")
        for series_id, count in series_counts.head(5).items():
            print(f"   {series_id}: {count} samples")

        # Print total unique series IDs
        unique_series_count = df["series_id"].nunique()
        print(f"\n🎯 Total unique series IDs: {unique_series_count}")


if __name__ == "__main__":
    main()
