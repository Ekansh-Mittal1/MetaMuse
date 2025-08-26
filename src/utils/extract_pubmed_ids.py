#!/usr/bin/env python3
"""
Script to extract PubMed IDs from GSE IDs using GEOmetadb SQLite database.

This script will:
1. Read GSE IDs from a text file
2. Use GEOmetadb SQLite to extract PubMed IDs from series metadata
3. Use HTTP API as fallback if SQLite fails
4. Save all PubMed IDs to a text file
5. Save GSE IDs without data to a separate file

Run with: python src/utils/extract_pubmed_ids.py --input gse_ids.txt --output pubmed_ids.txt
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Set, Optional
import logging
import traceback
import time
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.tools.sqlite_ingestion_tools import get_geometadb_manager, download_geometadb
from src.tools.sqlite_manager import GEOmetadbManager

# Set up logging - only show warnings and errors
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Rate limiting configuration
HTTP_RATE_LIMIT_DELAY = float(os.getenv('HTTP_RATE_LIMIT_DELAY', '0.1'))  # seconds between requests
MAX_REQUESTS_PER_MINUTE = int(os.getenv('MAX_REQUESTS_PER_MINUTE', '600'))


class RateLimiter:
    """Rate limiter for HTTP API calls to respect NCBI rate limits."""
    
    def __init__(self, delay_seconds: float = None, max_per_minute: int = None):
        """
        Initialize rate limiter.
        
        Parameters
        ----------
        delay_seconds : float
            Minimum delay between requests in seconds
        max_per_minute : int
            Maximum requests per minute
        """
        self.delay_seconds = delay_seconds or HTTP_RATE_LIMIT_DELAY
        self.max_per_minute = max_per_minute or MAX_REQUESTS_PER_MINUTE
        self.last_request_time = 0
        self.request_times = []
    
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        
        # Remove requests older than 1 minute
        self.request_times = [t for t in self.request_times if current_time - t < 60]
        
        # Check if we've exceeded the rate limit
        if len(self.request_times) >= self.max_per_minute:
            # Wait until we can make another request
            sleep_time = 60 - (current_time - self.request_times[0])
            if sleep_time > 0:
                print(f"⏳ Rate limit reached, waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
                current_time = time.time()
        
        # Wait minimum delay between requests
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.delay_seconds:
            sleep_time = self.delay_seconds - time_since_last
            time.sleep(sleep_time)
            current_time = time.time()
        
        # Record this request
        self.last_request_time = current_time
        self.request_times.append(current_time)


def read_gse_ids(input_file: str) -> List[str]:
    """
    Read GSE IDs from a text file.
    
    Parameters
    ----------
    input_file : str
        Path to the input text file containing GSE IDs
        
    Returns
    -------
    List[str]
        List of GSE IDs
    """
    try:
        with open(input_file, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        gse_ids = []
        for line in lines:
            # Split by comma and handle multiple GSE IDs per line
            if ',' in line:
                # Handle comma-delimited GSE IDs
                line_gse_ids = [gse_id.strip() for gse_id in line.split(',') if gse_id.strip()]
                gse_ids.extend(line_gse_ids)
            else:
                # Single GSE ID per line
                gse_ids.append(line)
        
        # Filter to ensure they look like GSE IDs and remove duplicates
        gse_ids = [gse_id for gse_id in gse_ids if gse_id.startswith('GSE')]
        gse_ids = list(set(gse_ids))  # Remove duplicates
        
        return gse_ids
        
    except Exception as e:
        print(f"❌ Error reading GSE IDs from {input_file}: {e}")
        print(f"Full traceback: {traceback.format_exc()}")
        raise





def get_pubmed_ids_from_series(manager: GEOmetadbManager, gse_id: str, use_http_fallback: bool = True, rate_limiter: RateLimiter = None) -> List[str]:
    """
    Get PubMed IDs associated with a series (GSE) ID.
    
    Parameters
    ----------
    manager : GEOmetadbManager
        The SQLite database manager
    gse_id : str
        The GSE ID to look up
    use_http_fallback : bool
        Whether to use HTTP fallback if no PubMed ID found in SQLite
    rate_limiter : RateLimiter, optional
        Rate limiter instance to control HTTP request timing
        
    Returns
    -------
    List[str]
        List of associated PubMed IDs
    """
    try:
        # Get GSE metadata which includes PubMed ID
        metadata = manager.get_gse_metadata(gse_id)
        
        if "error" in metadata:
            if use_http_fallback:
                return get_pubmed_ids_from_series_http_fallback(gse_id, rate_limiter)
            else:
                return []
        
        # Extract PubMed ID from metadata
        pubmed_id = metadata.get('pubmed_id', '')
        if pubmed_id and str(pubmed_id).strip():
            # Convert to string and clean up
            pubmed_id_str = str(pubmed_id).strip()
            return [pubmed_id_str]
        elif use_http_fallback:
            # No PubMed ID found in SQLite, try HTTP fallback
            return get_pubmed_ids_from_series_http_fallback(gse_id, rate_limiter)
        else:
            return []
        
    except Exception as e:
        if use_http_fallback:
            return get_pubmed_ids_from_series_http_fallback(gse_id, rate_limiter)
        else:
            return []


def get_pubmed_ids_from_series_http_fallback(gse_id: str, rate_limiter: RateLimiter = None) -> List[str]:
    """
    Get PubMed IDs from GSE using HTTP API fallback.
    
    Parameters
    ----------
    gse_id : str
        The GSE ID to look up
    rate_limiter : RateLimiter, optional
        Rate limiter instance to control request timing
        
    Returns
    -------
    List[str]
        List of associated PubMed IDs
    """
    try:
        import tempfile
        import json
        
        # Get email and API key from environment (already loaded by load_dotenv)
        email = os.getenv("NCBI_EMAIL")
        api_key = os.getenv("NCBI_API_KEY")
        
        if not email:
            return []
        
        # Apply rate limiting if provided
        if rate_limiter:
            rate_limiter.wait_if_needed()
        
        # Create temporary session directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Import HTTP-based implementation
            from src.tools.ingestion_tools import extract_gse_metadata_impl
            
            # Extract GSE metadata via HTTP
            result_path = extract_gse_metadata_impl(
                gse_id=gse_id,
                session_dir=temp_dir,
                email=email,
                api_key=api_key
            )
            
            # Parse the result to extract PubMed IDs
            if os.path.exists(result_path):
                with open(result_path, 'r') as f:
                    metadata = json.load(f)
                
                # Extract PubMed ID from metadata
                attributes = metadata.get('attributes', {})
                pubmed_id = (
                    attributes.get('pubmed_id')
                    or attributes.get('PubMed ID')
                    or attributes.get('pubmed')
                    or metadata.get('pubmed_id', '')
                )
                
                if pubmed_id and str(pubmed_id).strip():
                    pubmed_id_str = str(pubmed_id).strip()
                    return [pubmed_id_str]
                else:
                    return []
            else:
                return []
                
    except ImportError as e:
        return []
    except Exception as e:
        return []


def extract_pubmed_ids_sqlite(gse_ids: List[str], db_path: str = "data/GEOmetadb.sqlite", use_http_fallback: bool = True, rate_limit_delay: float = None, max_requests_per_minute: int = None) -> tuple[Set[str], Set[str]]:
    """
    Extract PubMed IDs using SQLite database with optional HTTP fallback.
    
    Parameters
    ----------
    gse_ids : List[str]
        List of GSE IDs to process
    db_path : str
        Path to the SQLite database
    use_http_fallback : bool
        Whether to use HTTP fallback when no PubMed IDs found in SQLite
    rate_limit_delay : float, optional
        Custom delay between HTTP requests in seconds
    max_requests_per_minute : int, optional
        Custom maximum HTTP requests per minute
        
    Returns
    -------
    tuple[Set[str], Set[str]]
        Tuple of (pubmed_ids, gse_ids_without_data)
    """
    pubmed_ids = set()
    gse_ids_without_data = set()
    
    # Initialize rate limiter if HTTP fallback is enabled
    rate_limiter = None
    if use_http_fallback:
        rate_limiter = RateLimiter(rate_limit_delay, max_requests_per_minute)
        delay_display = rate_limit_delay if rate_limit_delay else HTTP_RATE_LIMIT_DELAY
        max_req_display = max_requests_per_minute if max_requests_per_minute else MAX_REQUESTS_PER_MINUTE
        print(f"🚦 Rate limiting enabled: {delay_display}s delay, max {max_req_display}/min")
    
    try:
        with get_geometadb_manager(db_path) as manager:
            # Use tqdm for progress tracking with running tally
            pbar = tqdm(gse_ids, desc="Processing GSE IDs", unit="gse")
            for gse_id in pbar:
                # Get PubMed IDs directly from GSE
                pmid_list = get_pubmed_ids_from_series(manager, gse_id, use_http_fallback, rate_limiter)
                
                if pmid_list:
                    pubmed_ids.update(pmid_list)
                else:
                    # No PubMed IDs found for this GSE
                    gse_ids_without_data.add(gse_id)
                
                # Update progress bar with running tally
                pbar.set_postfix({
                    'PubMed IDs': len(pubmed_ids),
                    'No Data': len(gse_ids_without_data)
                })
        
        return pubmed_ids, gse_ids_without_data
        
    except Exception as e:
        print(f"❌ Error during SQLite extraction: {e}")
        print(f"Full traceback: {traceback.format_exc()}")
        return pubmed_ids, gse_ids_without_data





def save_pubmed_ids(pubmed_ids: Set[str], output_file: str):
    """
    Save PubMed IDs to a text file.
    
    Parameters
    ----------
    pubmed_ids : Set[str]
        Set of PubMed IDs to save
    output_file : str
        Path to the output file
    """
    try:
        with open(output_file, 'w') as f:
            for pmid in sorted(pubmed_ids):
                f.write(f"{pmid}\n")
        
    except Exception as e:
        print(f"❌ Error saving PubMed IDs to {output_file}: {e}")
        print(f"Full traceback: {traceback.format_exc()}")
        raise


def save_samples_without_data(samples_without_data: Set[str], output_file: str):
    """
    Save samples without data to a text file.
    
    Parameters
    ----------
    samples_without_data : Set[str]
        Set of GSM IDs that had no data
    output_file : str
        Path to the output file
    """
    try:
        with open(output_file, 'w') as f:
            for gsm_id in sorted(samples_without_data):
                f.write(f"{gsm_id}\n")
        
    except Exception as e:
        print(f"❌ Error saving samples without data to {output_file}: {e}")
        print(f"Full traceback: {traceback.format_exc()}")
        raise


def main():
    """Main function to extract PubMed IDs from GSE IDs."""
    parser = argparse.ArgumentParser(description="Extract PubMed IDs from GSE IDs using GEOmetadb")
    parser.add_argument("--input", "-i", default="archs4_samples/archs4_gse_ids.txt", help="Input text file containing GSE IDs (default: archs4_samples/archs4_gse_ids.txt)")
    parser.add_argument("--output", "-o", default="archs4_samples/archs4_pubmed_ids.txt", help="Output text file for PubMed IDs (default: archs4_samples/archs4_pubmed_ids.txt)")
    parser.add_argument("--db-path", default="data/GEOmetadb.sqlite", help="Path to GEOmetadb SQLite database")
    parser.add_argument("--force-download", action="store_true", help="Force download of database if it doesn't exist")
    parser.add_argument("--no-http-fallback", action="store_true", help="Disable HTTP API fallback (use SQLite only)")
    parser.add_argument("--rate-limit-delay", type=float, help="Delay between HTTP requests in seconds (default: 0.1)")
    parser.add_argument("--max-requests-per-minute", type=int, help="Maximum HTTP requests per minute (default: 60)")
    
    args = parser.parse_args()
    
    print("🚀 Starting PubMed ID extraction...")
    
    # Check if input file exists
    if not os.path.exists(args.input):
        print(f"❌ Input file {args.input} does not exist")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Read GSE IDs
        print(f"📖 Reading GSE IDs from {args.input}...")
        gse_ids = read_gse_ids(args.input)
        if not gse_ids:
            print("❌ No valid GSE IDs found in input file")
            sys.exit(1)
        print(f"✅ Read {len(gse_ids):,} GSE IDs")
        
        # Check if database exists, download if necessary
        if not os.path.exists(args.db_path) or args.force_download:
            print("📥 Downloading GEOmetadb SQLite database...")
            download_geometadb(args.db_path)
            print("✅ Database download completed")
        else:
            print("✅ Using existing database")
        
        # Extract PubMed IDs using SQLite with optional HTTP fallback
        print("🔍 Extracting PubMed IDs from database...")
        
        # Override rate limiting settings if provided via command line
        rate_limit_delay = args.rate_limit_delay if args.rate_limit_delay is not None else HTTP_RATE_LIMIT_DELAY
        max_requests_per_minute = args.max_requests_per_minute if args.max_requests_per_minute is not None else MAX_REQUESTS_PER_MINUTE
        
        pubmed_ids, gse_ids_without_data = extract_pubmed_ids_sqlite(gse_ids, args.db_path, use_http_fallback=not args.no_http_fallback, rate_limit_delay=rate_limit_delay, max_requests_per_minute=max_requests_per_minute)
        
        # Save results
        print("💾 Saving results...")
        if pubmed_ids:
            save_pubmed_ids(pubmed_ids, args.output)
            print(f"✅ Successfully extracted {len(pubmed_ids):,} unique PubMed IDs")
        else:
            print("⚠️  No PubMed IDs found")
            # Create empty output file
            with open(args.output, 'w') as f:
                pass
            print(f"📄 Created empty output file: {args.output}")
        
        # Save GSE IDs without data (always generate this file for cross-verification)
        output_dir = Path(args.output).parent
        output_name = Path(args.output).stem
        gse_ids_without_data_file = output_dir / f"{output_name}_no_data.txt"
        
        if gse_ids_without_data:
            save_samples_without_data(gse_ids_without_data, str(gse_ids_without_data_file))
            print(f"📝 Saved {len(gse_ids_without_data):,} GSE IDs without data to {gse_ids_without_data_file}")
        else:
            # Create empty file for cross-verification
            with open(gse_ids_without_data_file, 'w') as f:
                pass
            print(f"📝 Created empty file for GSE IDs without data: {gse_ids_without_data_file}")
            print("✅ All GSE IDs had data")
        
        print("🎉 PubMed ID extraction completed successfully!")
        
    except Exception as e:
        print(f"❌ Unexpected error during extraction: {e}")
        print(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
