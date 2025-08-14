import requests
import time
import argparse

import tempfile    
import zipfile
import pandas as pd
import numpy as np
import os

# Import the convert_csv_to_parquet function
try:
    # Try relative import first (when used as a module)
    from .csv_to_parquet import convert_csv_to_parquet
except ImportError:
    # Fall back to absolute import (when run standalone)
    from csv_to_parquet import convert_csv_to_parquet

def main(input_file, output_file, species):
    # Read the input file and extract sample IDs
    df_input = pd.read_parquet(input_file)
    gsm_ids = df_input['sample_id'].tolist()
    print(f"Found {len(gsm_ids)} sample IDs in input file")
    
    url = "https://maayanlab.cloud/sigpy/data/samples"
    data = {
        "gsm_ids": gsm_ids,
        "species": species
    }
    response = requests.post(url, json=data)

    task_id = response.json()['task_id']
    print("Task ID:", task_id)

    url = f"https://maayanlab.cloud/sigpy/data/samples/status/{task_id}"

    status = "PENDING"
    while status == "PENDING":
        response = requests.get(url)
        status = response.json()['status']
        print("Status:", status)
        time.sleep(10)

    url = f"https://maayanlab.cloud/sigpy/data/samples/download/{task_id}"
    response = requests.get(url)
    
    # Save to temporary file
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_file:
        temp_file.write(response.content)
        temp_zip_path = temp_file.name
    
    # Extract and convert to parquet
    with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
        # Extract all files to a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_ref.extractall(temp_dir)
            
            # Find the main data file (assuming it's a CSV or similar)
            extracted_files = zip_ref.namelist()
            data_file = None
            for file in extracted_files:
                if file.endswith('.csv') or file.endswith('.tsv') or file.endswith('.txt'):
                    data_file = file
                    break
            
            if data_file:
                file_path = f"{temp_dir}/{data_file}"
                
                # Use the convert_csv_to_parquet function instead of manual conversion
                print(f"Converting extracted data file: {data_file}")
                
                # Determine separator based on file extension
                separator = '\t' if data_file.endswith('.tsv') else ','
                
                # Convert the extracted file to parquet using our utility function
                success = convert_csv_to_parquet(
                    input_file=file_path,
                    output_file=output_file,
                    separator=separator,
                    index_col=0,  # First column as index (genes)
                    optimize_dtypes=True,
                    compression='snappy'
                )
                
                if success:
                    # Read the generated parquet file to add assay_type column
                    df_transposed = pd.read_parquet(output_file)
                    
                    # Add assay_type column based on total counts per sample
                    row_sums = df_transposed.sum(axis=1)
                    df_transposed['assay_type'] = row_sums.apply(lambda x: 'single_cell' if x < 1_000_000 else 'bulk')
                    
                    print(f"Added assay_type column based on total counts per sample")
                    
                    # Save the final version with assay_type column
                    df_transposed.to_parquet(output_file, index=True)
                    print(f"Matrix saved as {output_file}")
                    print(f"Data shape: {df_transposed.shape} (samples x genes)")
                else:
                    print("❌ Failed to convert data file to parquet")
                    return False

            else:
                print("No suitable data file found in the zip archive")
                return False
    
    # Clean up temporary zip file
    os.unlink(temp_zip_path)
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download GEO sample counts data')
    parser.add_argument('--input_file', help='Input parquet file containing GSM IDs')
    parser.add_argument('--output_file', help='Output .parquet file for the count matrix')
    parser.add_argument('--species', default='human', help='Species (default: human)')
    args = parser.parse_args()

    # Validate file extensions
    if args.input_file and not args.input_file.endswith('.parquet'):
        raise ValueError(f"Input file must have .parquet extension, got: {args.input_file}")
    
    if args.output_file and not args.output_file.endswith('.parquet'):
        raise ValueError(f"Output file must have .parquet extension, got: {args.output_file}")
    
    success = main(input_file=args.input_file, output_file=args.output_file, species=args.species)
    
    if success:
        print("✅ GEO counts download and conversion completed successfully!")
    else:
        print("❌ GEO counts download and conversion failed!")
        exit(1)