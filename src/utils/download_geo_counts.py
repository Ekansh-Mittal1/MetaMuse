import requests
import time
import argparse
import math
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed

import tempfile    
import zipfile
import pandas as pd
import numpy as np
import os
from pathlib import Path
from tqdm import tqdm

# Import the convert_csv_to_parquet function
try:
    # Try relative import first (when used as a module)
    from .csv_to_parquet import convert_csv_to_parquet
except ImportError:
    # Fall back to absolute import (when run standalone)
    from csv_to_parquet import convert_csv_to_parquet


def read_sample_ids(input_file):
    """
    Read sample IDs from either a parquet file or a text file.
    
    Parameters:
    -----------
    input_file : str
        Path to input file (parquet or txt)
    
    Returns:
    --------
    list
        List of sample IDs
    """
    input_path = Path(input_file)
    
    if input_path.suffix.lower() == '.parquet':
        # Read from parquet file
        df_input = pd.read_parquet(input_file)
        if 'sample_id' not in df_input.columns:
            raise ValueError("Parquet file must contain a 'sample_id' column")
        gsm_ids = df_input['sample_id'].tolist()
        
    elif input_path.suffix.lower() in ['.txt', '.csv']:
        # Read from text file (one sample ID per line)
        with open(input_file, 'r') as f:
            gsm_ids = [line.strip() for line in f if line.strip()]
        
    else:
        raise ValueError(f"Unsupported file format: {input_path.suffix}. Use .parquet, .txt, or .csv")
    
    return gsm_ids


def download_batch_with_retry(gsm_ids, species, batch_num=None, total_batches=None, max_retries=3, base_delay=10):
    """
    Download data for a batch of sample IDs with retry logic.
    
    Parameters:
    -----------
    gsm_ids : list
        List of sample IDs for this batch
    species : str
        Species name
    batch_num : int, optional
        Batch number for logging
    total_batches : int, optional
        Total number of batches for logging
    max_retries : int
        Maximum number of retry attempts
    base_delay : int
        Base delay in seconds for exponential backoff
    
    Returns:
    --------
    tuple
        (success: bool, data: bytes or None, error: str or None)
    """
    batch_info = f"batch {batch_num}/{total_batches}" if batch_num is not None else "batch"
    
    for attempt in range(max_retries + 1):
        if attempt > 0:
            delay = base_delay * (2 ** (attempt - 1))  # Exponential backoff
            time.sleep(delay)
        
        try:
            success, data, error = download_batch(gsm_ids, species, batch_num, total_batches)
            
            if success:
                return True, data, None
            else:
                # Check if it's a retryable error
                if any(retryable_error in error.lower() for retryable_error in 
                       ['timeout', '504', '503', '502', '500', 'connection', 'network']):
                    if attempt < max_retries:
                        continue
                    else:
                        return False, None, f"Failed after {max_retries + 1} attempts: {error}"
                else:
                    # Non-retryable error
                    return False, None, error
                    
        except Exception as e:
            if attempt < max_retries:
                continue
            else:
                return False, None, str(e)
    
    return False, None, f"Failed after {max_retries + 1} attempts"


def download_batch(gsm_ids, species, batch_num=None, total_batches=None):
    """
    Download data for a batch of sample IDs.
    
    Parameters:
    -----------
    gsm_ids : list
        List of sample IDs for this batch
    species : str
        Species name
    batch_num : int, optional
        Batch number for logging
    total_batches : int, optional
        Total number of batches for logging
    
    Returns:
    --------
    tuple
        (success: bool, data: bytes or None, error: str or None)
    """
    try:
        url = "https://maayanlab.cloud/sigpy/data/samples"
        data = {
            "gsm_ids": gsm_ids,
            "species": species
        }
        response = requests.post(url, json=data)

        task_id = response.json()['task_id']

        url = f"https://maayanlab.cloud/sigpy/data/samples/status/{task_id}"

        status = "PENDING"
        while status in ["PENDING", "PROCESSING"]:
            response = requests.get(url)
            
            status = response.json()['status']
            time.sleep(60)

        if status not in ["COMPLETED", "SUCCESS"]:
            return False, None, f"Task failed with status: {status}"

        url = f"https://maayanlab.cloud/sigpy/data/samples/download/{task_id}"
        response = requests.get(url)
        
        if response.status_code != 200:
            return False, None, f"Download failed with status {response.status_code}"

        return True, response.content, None
        
    except Exception as e:
        return False, None, str(e)


def process_downloaded_data(zip_content, output_file):
    """
    Process downloaded zip content and convert to parquet.
    
    Parameters:
    -----------
    zip_content : bytes
        Zip file content
    output_file : str
        Output parquet file path
    
    Returns:
    --------
    bool
        Success status
    """
    # Save to temporary file
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_file:
        temp_file.write(zip_content)
        temp_zip_path = temp_file.name
    
    try:
        # Extract and convert to parquet
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            # Extract all files to a temporary directory
            with tempfile.TemporaryDirectory() as temp_dir:
                zip_ref.extractall(temp_dir)
                
                # Find the main data file
                extracted_files = zip_ref.namelist()
                data_file = None
                for file in extracted_files:
                    if file.endswith('.csv') or file.endswith('.tsv') or file.endswith('.txt'):
                        data_file = file
                        break
                
                if data_file:
                    file_path = f"{temp_dir}/{data_file}"
                    
                    # Use the convert_csv_to_parquet function
                    
                    # Determine separator based on file extension
                    separator = '\t' if data_file.endswith('.tsv') else ','
                    
                    # Convert the extracted file to parquet
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
                        
                        # Save the final version with assay_type column
                        df_transposed.to_parquet(output_file, index=True)
                        return True
                    else:
                        return False

                else:
                    return False
    finally:
        # Clean up temporary zip file
        os.unlink(temp_zip_path)


def main(input_file, output_file, species, batch_size=5000, max_workers=None, max_retries=3, base_delay=10, memory_efficient=False, max_memory_gb=8):
    """
    Main function to download GEO sample counts data.
    
    Parameters:
    -----------
    input_file : str
        Input file path (parquet or txt)
    output_file : str
        Output parquet file path
    species : str
        Species name
    batch_size : int
        Maximum number of samples per batch
    max_workers : int, optional
        Maximum number of concurrent workers
    max_retries : int
        Maximum number of retry attempts
    base_delay : int
        Base delay in seconds for exponential backoff
    memory_efficient : bool
        Use memory-efficient incremental combination
    max_memory_gb : float
        Maximum memory usage in GB when using memory-efficient mode
    """

    # Read sample IDs
    gsm_ids = read_sample_ids(input_file)
    
    if not gsm_ids:
        print("❌ No sample IDs found in input file")
        return False
    
    total_samples = len(gsm_ids)
    
    # Determine if batching is needed
    if total_samples <= batch_size:
        # Single batch
        success, data, error = download_batch_with_retry(gsm_ids, species, max_retries=max_retries, base_delay=base_delay)
        
        if not success:
            print(f"❌ Download failed: {error}")
            return False
        
        return process_downloaded_data(data, output_file)
    
    else:
        # Multiple batches needed
        num_batches = math.ceil(total_samples / batch_size)
        
        # Memory-efficient mode: adjust batch size based on available memory
        if memory_efficient:
            try:
                import psutil
                available_memory_gb = psutil.virtual_memory().available / (1024**3)
                
                # Estimate memory per sample (conservative estimate)
                estimated_memory_per_sample_mb = 0.1  # 0.1 MB per sample
                max_samples_per_batch = int((max_memory_gb * 1024) / estimated_memory_per_sample_mb)
                
                if max_samples_per_batch < batch_size:
                    old_batch_size = batch_size
                    batch_size = max_samples_per_batch
                    num_batches = math.ceil(total_samples / batch_size)
                
            except ImportError:
                pass
        
                    # Set default max_workers if not specified
        if max_workers is None:
            max_workers = min(num_batches, 10)  # Cap at 10 concurrent downloads to avoid overwhelming the API
    
        print(f"Processing {total_samples} samples in {num_batches} batches with {max_workers} workers")
        
        # Create temporary directory for batch files
        temp_dir = tempfile.mkdtemp()
        batch_files = []
        
        try:
            # Process batches concurrently
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all batch tasks with small delays to prevent overwhelming the server
                futures = {}
                for i in tqdm(range(num_batches), desc="Submitting batch tasks"):
                    start_idx = i * batch_size
                    end_idx = min((i + 1) * batch_size, total_samples)
                    batch_ids = gsm_ids[start_idx:end_idx]
                    
                    future = executor.submit(download_batch_with_retry, batch_ids, species, i+1, num_batches, max_retries, base_delay)
                    futures[future] = i+1
                    
                    # Add small delay between submissions to prevent overwhelming the server
                    if i < num_batches - 1:  # Don't delay after the last submission
                        time.sleep(2)
                
                # Process completed futures
                completed_batches = 0
                for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading batches"):
                    batch_num = futures[future]
                    completed_batches += 1
                    
                    try:
                        success, data, error = future.result()
                        if not success:
                            print(f"❌ Batch {batch_num} failed: {error}")
                            return False
                        
                        # Save batch data to temporary file
                        batch_file = os.path.join(temp_dir, f"batch_{batch_num}.parquet")
                        if process_downloaded_data(data, batch_file):
                            batch_files.append(batch_file)
                        else:
                            print(f"❌ Batch {batch_num} processing failed")
                            return False
                            
                    except Exception as e:
                        print(f"❌ An unexpected error occurred during batch {batch_num}: {e}")
                        return False
            
            # Combine all batch files
            if memory_efficient:
                # Memory-efficient incremental combination
                
                if len(batch_files) == 1:
                    # Single batch file - just move it
                    import shutil
                    shutil.move(batch_files[0], output_file)
                    return True
                
                # Start with first batch
                combined_df = pd.read_parquet(batch_files[0])
                
                # Incrementally add other batches
                for i, batch_file in enumerate(tqdm(batch_files[1:], desc="Combining batches")):
                    batch_df = pd.read_parquet(batch_file)
                    
                    # Concatenate incrementally
                    combined_df = pd.concat([combined_df, batch_df], axis=0, ignore_index=False)
                    
                    # Remove duplicates if any (keep first occurrence)
                    if combined_df.index.duplicated().any():
                        combined_df = combined_df[~combined_df.index.duplicated(keep='first')]
                    
                    # Free memory from batch dataframe
                    del batch_df
                    
                    # Optional: Save intermediate results every 10 batches to prevent memory buildup
                    if (i + 2) % 10 == 0:
                        intermediate_file = output_file.replace('.parquet', f'_intermediate_{i+2}.parquet')
                        combined_df.to_parquet(intermediate_file, index=True)
                
                # Save final combined data
                combined_df.to_parquet(output_file, index=True)
                
                # Clean up intermediate files
                for i in range(10, len(batch_files), 10):
                    intermediate_file = output_file.replace('.parquet', f'_intermediate_{i}.parquet')
                    try:
                        os.remove(intermediate_file)
                    except:
                        pass
                
                return True
                
            else:
                # Original memory-intensive method
                
                # Read and combine all batch files
                combined_dfs = []
                for batch_file in tqdm(batch_files, desc="Reading batch files"):
                    df = pd.read_parquet(batch_file)
                    combined_dfs.append(df)
                
                # Concatenate all dataframes
                combined_df = pd.concat(combined_dfs, axis=0, ignore_index=False)
                
                # Remove duplicates if any (keep first occurrence)
                combined_df = combined_df[~combined_df.index.duplicated(keep='first')]
                
                # Save combined data
                combined_df.to_parquet(output_file, index=True)
                
                return True
            
        finally:
            # Clean up temporary files
            for batch_file in batch_files:
                try:
                    os.remove(batch_file)
                except:
                    pass
            try:
                os.rmdir(temp_dir)
            except:
                pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download GEO sample counts data')
    parser.add_argument('--input', required=True, help='Input file containing sample IDs (parquet, txt, or csv)')
    parser.add_argument('--output', required=True, help='Output .parquet file for the count matrix')
    parser.add_argument('--species', default='human', help='Species (default: human)')
    parser.add_argument('--batch_size', type=int, default=5000, help='Maximum samples per batch (default: 5000)')
    parser.add_argument('--max_workers', type=int, default=None, help='Maximum concurrent workers (default: min(num_batches, 10))')
    parser.add_argument('--max_retries', type=int, default=3, help='Maximum number of retry attempts for failed batches (default: 3)')
    parser.add_argument('--base_delay', type=int, default=60, help='Base delay in seconds for exponential backoff (default: 10)')
    parser.add_argument('--memory-efficient', action='store_true', help='Use memory-efficient incremental combination (recommended for large datasets)')
    parser.add_argument('--max-memory-gb', type=float, default=8.0, help='Maximum memory usage in GB when using memory-efficient mode (default: 8.0)')
    args = parser.parse_args()

    # Validate file extensions
    input_path = Path(args.input)
    if input_path.suffix.lower() not in ['.parquet', '.txt', '.csv']:
        raise ValueError(f"Input file must have .parquet, .txt, or .csv extension, got: {input_path.suffix}")
    
    if not args.output.endswith('.parquet'):
        raise ValueError(f"Output file must have .parquet extension, got: {args.output}")
    
    print(f"Starting GEO counts download for {Path(args.input).name}...")
    success = main(
        input_file=args.input, 
        output_file=args.output, 
        species=args.species,
        batch_size=args.batch_size,
        max_workers=args.max_workers,
        max_retries=args.max_retries,
        base_delay=args.base_delay,
        memory_efficient=args.memory_efficient,
        max_memory_gb=args.max_memory_gb
    )
    
    if success:
        print(f"✅ GEO counts download completed successfully! Output: {args.output}")
    else:
        print("❌ GEO counts download failed!")
        exit(1)
