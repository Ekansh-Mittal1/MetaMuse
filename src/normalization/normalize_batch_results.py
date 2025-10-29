#!/usr/bin/env python3
"""
Batch Results CSV Normalizer

This script reads a batch_results.csv file, extracts curated values for disease, tissue,
and organ fields, runs the normalizer agent on them, and outputs a new CSV file with
normalized ontology terms and IDs.

Usage:
    python src/normalization/normalize_batch_results.py <csv_file_path> [options]

Example:
    python src/normalization/normalize_batch_results.py batch/batch_20251015_195716/batch_results.csv --min-score 0.5
"""

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
from tqdm import tqdm

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.tools.normalizer_tools import semantic_search_candidates_impl, get_default_ontologies_for_field
from src.models.curation_models import (
    CurationResult,
    ExtractedCandidate,
    DiseaseCurationResult,
    DiseaseExtractedCandidate,
    DiseaseCondition
)


def parse_disease_value(curated_value: str) -> tuple[str, DiseaseCondition]:
    """
    Parse a disease curated value to extract disease name and condition.
    
    Parameters
    ----------
    curated_value : str
        Curated disease value (e.g., "control [healthy]", "breast cancer")
        
    Returns
    -------
    tuple[str, DiseaseCondition]
        Tuple of (disease_name, condition)
    """
    value_lower = curated_value.lower()
    
    # Check for control patterns
    if "control" in value_lower or "healthy" in value_lower:
        # Extract disease name if present (e.g., "control [breast cancer]")
        if "[" in curated_value and "]" in curated_value:
            # Format: "control [disease]"
            start = curated_value.index("[") + 1
            end = curated_value.index("]")
            disease_name = curated_value[start:end].strip()
            if disease_name.lower() == "healthy":
                disease_name = "healthy"
        else:
            disease_name = "healthy"
        return disease_name, DiseaseCondition.CONTROL
    else:
        # It's a disease
        return curated_value, DiseaseCondition.DISEASED


def create_curation_result_from_csv_row(
    sample_id: str,
    target_field: str,
    curated_value: str
):
    """
    Create a CurationResult object from a CSV row's curated value.
    
    Parameters
    ----------
    sample_id : str
        Sample identifier
    target_field : str
        Target field name (disease, tissue, organ)
    curated_value : str
        Curated value from CSV
        
    Returns
    -------
    CurationResult or DiseaseCurationResult
        Properly structured result object based on target field
    """
    # Handle disease fields specially
    if target_field.lower() == "disease":
        disease_name, condition = parse_disease_value(curated_value)
        
        # Create DiseaseExtractedCandidate
        candidate = DiseaseExtractedCandidate(
            value=disease_name,
            condition=condition,
            confidence=1.0,
            source="csv_batch_results",
            context=f"Curated value from batch_results.csv: {curated_value}",
            rationale="Pre-curated disease value from batch processing"
        )
        
        # Create DiseaseCurationResult
        return DiseaseCurationResult(
            tool_name="BatchResultsCSV",
            sample_id=sample_id,
            target_field="disease",
            disease_name=disease_name,
            condition=condition,
            confidence=1.0,
            series_candidates=[],
            sample_candidates=[candidate],
            abstract_candidates=[],
            final_candidates=[candidate],
            reconciliation_needed=False,
            sources_processed=["csv_batch_results"],
            processing_notes=["Loaded from batch_results.csv for normalization"]
        )
    else:
        # Regular fields (tissue, organ, etc.)
        candidate = ExtractedCandidate(
            value=curated_value,
            confidence=1.0,
            source="csv_batch_results",
            context=f"Curated value from batch_results.csv: {curated_value}",
            rationale=f"Pre-curated {target_field} value from batch processing",
            prenormalized=""
        )
        
        # Create CurationResult with the candidate in final_candidates
        return CurationResult(
            tool_name="BatchResultsCSV",
            sample_id=sample_id,
            target_field=target_field,
            series_candidates=[],
            sample_candidates=[candidate],
            abstract_candidates=[],
            final_candidates=[candidate],
            final_candidate=curated_value,
            final_confidence=1.0,
            reconciliation_needed=False,
            sources_processed=["csv_batch_results"],
            processing_notes=["Loaded from batch_results.csv for normalization"]
        )


def read_and_extract_curated_values(csv_path: Path) -> pd.DataFrame:
    """
    Read CSV file and extract curated values.
    
    Parameters
    ----------
    csv_path : Path
        Path to batch_results.csv file
        
    Returns
    -------
    pd.DataFrame
        DataFrame with curated values
    """
    print(f"📖 Reading CSV file: {csv_path}")
    
    # Read CSV with proper handling of whitespace in column names
    df = pd.read_csv(csv_path, skipinitialspace=True)
    
    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    
    # Verify required columns exist
    required_columns = ['sample_id', 'disease_final_candidate', 'tissue_final_candidate', 'organ_final_candidate']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    print(f"✅ Successfully read {len(df)} rows")
    print(f"📋 Columns found: {', '.join(df.columns.tolist())}")
    
    return df


def create_temp_curation_files(
    df: pd.DataFrame,
    target_fields: List[str],
    temp_dir: Path
) -> Dict[str, Path]:
    """
    Create temporary curation result JSON files for each target field.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with curated values
    target_fields : List[str]
        List of target fields to process (disease, tissue, organ)
    temp_dir : Path
        Temporary directory for files
        
    Returns
    -------
    Dict[str, Path]
        Mapping of target field to file path
    """
    print(f"\n📝 Creating temporary curation result files in: {temp_dir}")
    
    temp_files = {}
    
    for field in tqdm(target_fields, desc="Processing fields", unit="field"):
        column_name = f"{field}_final_candidate"
        
        if column_name not in df.columns:
            print(f"⚠️  Column '{column_name}' not found, skipping {field}")
            continue
        
        # Filter rows with non-empty curated values for this field
        field_df = df[df[column_name].notna() & (df[column_name] != '')]
        
        if len(field_df) == 0:
            print(f"⚠️  No curated values found for {field}, skipping")
            continue
        
        # Create CurationResult objects for each sample
        curation_results = []
        for _, row in tqdm(field_df.iterrows(), total=len(field_df), desc=f"  Creating {field} entries", unit="sample", leave=False):
            sample_id = str(row['sample_id'])
            curated_value = str(row[column_name]).strip()
            
            if curated_value:  # Only process non-empty values
                curation_result = create_curation_result_from_csv_row(
                    sample_id=sample_id,
                    target_field=field,
                    curated_value=curated_value
                )
                curation_results.append(curation_result.model_dump())
        
        # Save to temporary file
        if curation_results:
            file_path = temp_dir / f"curation_results_{field}.json"
            
            # Wrap in the expected structure for semantic_search_candidates_impl
            output_data = {
                "curation_results": curation_results
            }
            
            with open(file_path, 'w') as f:
                json.dump(output_data, f, indent=2)
            
            temp_files[field] = file_path
            print(f"✅ Created {field}: {len(curation_results)} samples → {file_path.name}")
        else:
            print(f"⚠️  No valid curation results for {field}")
    
    return temp_files


def run_normalization(
    temp_files: Dict[str, Path],
    min_score: float,
    top_k: int
) -> Dict[str, Any]:
    """
    Run normalizer agent on temporary curation files.
    
    Parameters
    ----------
    temp_files : Dict[str, Path]
        Mapping of target field to curation file path
    min_score : float
        Minimum similarity score threshold
    top_k : int
        Number of top matches to return
        
    Returns
    -------
    Dict[str, Any]
        Normalization results by field
    """
    print(f"\n🔬 Running normalization (min_score={min_score}, top_k={top_k})")
    
    normalization_results = {}
    
    with tqdm(temp_files.items(), desc="Normalizing fields", unit="field") as pbar:
        for field, file_path in pbar:
            pbar.set_description(f"Normalizing {field}")
            
            # Get appropriate ontologies for this field
            ontologies = get_default_ontologies_for_field(field)
            tqdm.write(f"  Using ontologies for {field}: {', '.join(ontologies)}")
            
            try:
                # Run semantic search
                batch_result = semantic_search_candidates_impl(
                    curation_results_file=str(file_path),
                    target_field=field,
                    ontologies=ontologies,
                    top_k=top_k,
                    min_score=min_score
                )
                
                normalization_results[field] = batch_result
                
                # Count successful normalizations
                successful = sum(
                    1 for sample_entry in batch_result.sample_results
                    if sample_entry.result.normalization_success
                )
                total = len(batch_result.sample_results)
                
                tqdm.write(f"  ✅ Normalized {successful}/{total} samples for {field}")
                
            except Exception as e:
                tqdm.write(f"  ❌ Error normalizing {field}: {str(e)}")
                import traceback
                traceback.print_exc()
                normalization_results[field] = None
    
    return normalization_results


def extract_normalized_values(
    normalization_results: Dict[str, Any]
) -> Dict[str, Dict[str, Dict[str, str]]]:
    """
    Extract normalized terms and IDs from normalization results.
    
    Parameters
    ----------
    normalization_results : Dict[str, Any]
        Normalization results by field
        
    Returns
    -------
    Dict[str, Dict[str, Dict[str, str]]]
        Nested dict: {field: {sample_id: {term: ..., id: ...}}}
    """
    print("\n📊 Extracting normalized values...")
    
    extracted = {}
    
    for field, batch_result in normalization_results.items():
        if batch_result is None:
            print(f"  ⚠️  Skipping {field}: no results")
            continue
        
        field_results = {}
        
        for sample_entry in batch_result.sample_results:
            sample_id = sample_entry.sample_id
            result = sample_entry.result
            
            # Extract normalized term and ID
            normalized_term = result.final_normalized_term or ""
            normalized_id = result.final_normalized_id or ""
            
            field_results[sample_id] = {
                'term': normalized_term,
                'id': normalized_id
            }
        
        extracted[field] = field_results
        print(f"  ✅ Extracted {len(field_results)} results for {field}")
    
    return extracted


def create_output_csv(
    df: pd.DataFrame,
    normalized_values: Dict[str, Dict[str, Dict[str, str]]],
    output_path: Path
) -> None:
    """
    Create output CSV with normalized values.
    
    Parameters
    ----------
    df : pd.DataFrame
        Original DataFrame
    normalized_values : Dict[str, Dict[str, Dict[str, str]]]
        Extracted normalized values
    output_path : Path
        Output file path
    """
    print(f"\n💾 Creating output CSV: {output_path}")
    
    # Create a copy of the original dataframe
    output_df = df.copy()
    
    # Add/update normalized columns for each field
    for field in ['disease', 'tissue', 'organ']:
        term_col = f"{field}_normalized_term"
        id_col = f"{field}_normalized_id"
        
        # Initialize columns if they don't exist
        if term_col not in output_df.columns:
            output_df[term_col] = ""
        if id_col not in output_df.columns:
            output_df[id_col] = ""
        
        # Fill in normalized values if available
        if field in normalized_values:
            for sample_id, values in normalized_values[field].items():
                mask = output_df['sample_id'] == sample_id
                output_df.loc[mask, term_col] = values['term']
                output_df.loc[mask, id_col] = values['id']
    
    # Save to CSV
    output_df.to_csv(output_path, index=False)
    
    print("✅ Output CSV created successfully")
    print(f"   Total rows: {len(output_df)}")
    
    # Print summary statistics
    print("\n📈 Normalization Summary:")
    for field in ['disease', 'tissue', 'organ']:
        term_col = f"{field}_normalized_term"
        if term_col in output_df.columns:
            normalized_count = (output_df[term_col] != "").sum()
            total_count = len(output_df)
            percentage = (normalized_count / total_count * 100) if total_count > 0 else 0
            print(f"   {field.capitalize()}: {normalized_count}/{total_count} ({percentage:.1f}%)")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Normalize disease, tissue, and organ values in batch_results.csv files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/normalization/normalize_batch_results.py batch/batch_20251015_195716/batch_results.csv
  python src/normalization/normalize_batch_results.py batch_results.csv --min-score 0.5 --top-k 3
        """
    )
    
    parser.add_argument(
        'csv_file',
        type=str,
        help='Path to batch_results.csv file'
    )
    
    parser.add_argument(
        '--min-score',
        type=float,
        default=0.5,
        help='Minimum similarity score threshold (default: 0.5)'
    )
    
    parser.add_argument(
        '--top-k',
        type=int,
        default=2,
        help='Number of top matches to return per ontology (default: 2)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output file path (default: <input>_normalized.csv)'
    )
    
    parser.add_argument(
        '--fields',
        type=str,
        default='disease,tissue,organ',
        help='Comma-separated list of fields to normalize (default: disease,tissue,organ)'
    )
    
    args = parser.parse_args()
    
    # Validate input file
    csv_path = Path(args.csv_file)
    if not csv_path.exists():
        print(f"❌ Error: File not found: {csv_path}")
        sys.exit(1)
    
    # Determine output file path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = csv_path.parent / f"{csv_path.stem}_normalized.csv"
    
    # Parse fields
    target_fields = [f.strip() for f in args.fields.split(',')]
    
    print("=" * 80)
    print("🔬 Batch Results CSV Normalizer")
    print("=" * 80)
    print(f"Input file:    {csv_path}")
    print(f"Output file:   {output_path}")
    print(f"Target fields: {', '.join(target_fields)}")
    print(f"Min score:     {args.min_score}")
    print(f"Top K:         {args.top_k}")
    print("=" * 80)
    
    try:
        # Step 1: Read CSV
        df = read_and_extract_curated_values(csv_path)
        
        # Step 2: Create temporary directory and curation files
        with tempfile.TemporaryDirectory(prefix='normalize_batch_') as temp_dir:
            temp_dir_path = Path(temp_dir)
            temp_files = create_temp_curation_files(df, target_fields, temp_dir_path)
            
            if not temp_files:
                print("\n❌ No valid curated values found to normalize")
                sys.exit(1)
            
            # Step 3: Run normalization
            normalization_results = run_normalization(
                temp_files,
                min_score=args.min_score,
                top_k=args.top_k
            )
            
            # Step 4: Extract normalized values
            normalized_values = extract_normalized_values(normalization_results)
            
            # Step 5: Create output CSV
            create_output_csv(df, normalized_values, output_path)
        
        print("\n" + "=" * 80)
        print("✅ Normalization complete!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

