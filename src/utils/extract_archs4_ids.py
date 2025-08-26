#!/usr/bin/env python3
"""
Script to extract GSM and GSE IDs from the ARCHS4 dataset using archs4py.

This script will:
1. Try to download metadata-only files first (fastest option)
2. If that fails, download the latest ARCHS4 data file
3. Extract human GSM IDs from the metadata (with optional sample limit)
4. Extract GSE IDs with their associated GSM IDs from the metadata
5. Save results to two separate text files

Note: The script prioritizes metadata-only downloads for speed, but falls back
to the full dataset if needed. It only processes the metadata portion to minimize
memory usage.

Usage:
    python src/utils/extract_archs4_ids.py [OPTIONS]

Options:
    --max-samples INT     Maximum number of samples to process (default: all samples)
    --output-file STR     Custom output filename prefix (without extension)
    --species STR         Species to extract data for: human or mouse (default: human)

Examples:
    # Extract all samples with default filenames
    python src/utils/extract_archs4_ids.py
    
    # Extract only first 1000 samples
    python src/utils/extract_archs4_ids.py --max-samples 1000
    
    # Use custom output filename
    python src/utils/extract_archs4_ids.py --output-file my_archs4_data
    
    # Extract mouse data with sample limit
    python src/utils/extract_archs4_ids.py --species mouse --max-samples 500
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_archs4_metadata(species="human"):
    """Try to get ARCHS4 metadata without downloading the full dataset."""
    try:
        import archs4py.download
        import os
        
        # Check if we already have a downloaded file
        logger.info(f"Looking for existing {species} ARCHS4 data...")
        
        # Look for existing files in current directory
        existing_files = [f for f in os.listdir('.') if f.endswith('.h5') and species in f.lower() and ('gene' in f.lower() or 'archs4' in f.lower())]
        
        if existing_files:
            # Use the most recent existing file
            existing_file = sorted(existing_files)[-1]
            logger.info(f"Using existing file: {existing_file}")
            return existing_file
        
        # If no existing file, try to find a smaller metadata-only option
        logger.info("No existing data found. Looking for metadata-only options...")
        
        # Use the latest version for consistency and reliability
        logger.info("Downloading latest version for metadata extraction...")
        metadata_file = archs4py.download.counts(species=species, type="GENE_COUNTS", version="latest")
        logger.info("Using latest version")
        
        # Check if we got a valid file path
        if metadata_file is None:
            # The download function sometimes returns None even when successful
            # Look for the downloaded file in the current directory
            logger.warning("Download function returned None, searching for downloaded file...")
            downloaded_files = [f for f in os.listdir('.') if f.endswith('.h5') and species in f.lower() and 'gene' in f.lower()]
            if downloaded_files:
                # Use the most recent file
                metadata_file = sorted(downloaded_files)[-1]
                logger.info(f"Found downloaded file: {metadata_file}")
            else:
                raise Exception("Download function returned None and no downloaded file found")
        
        if not os.path.exists(metadata_file):
            raise Exception(f"Downloaded file not found: {metadata_file}")
        
        file_size = os.path.getsize(metadata_file) / (1024 * 1024)  # Size in MB
        logger.info(f"Successfully found file: {metadata_file} (Size: {file_size:.1f} MB)")
        logger.info("Note: We'll extract only the metadata portion to minimize memory usage.")
        
        return metadata_file
        
    except ImportError:
        logger.error("archs4py not found. Please install it with: uv add archs4py")
        return None
    except Exception as e:
        logger.error(f"Failed to download ARCHS4 data: {e}")
        return None


def try_download_metadata_only(species="human"):
    """Try to download just metadata from ARCHS4 without the full dataset."""
    try:
        import requests
        import os
        
        logger.info("Attempting to download metadata-only files...")
        
        # ARCHS4 metadata URLs (these might be available)
        base_url = "https://maayanlab.cloud/archs4"
        
        # Try different metadata file patterns
        metadata_patterns = [
            f"{base_url}/files/{species}/meta_{species}.h5",
            f"{base_url}/files/{species}/metadata_{species}.h5",
            f"{base_url}/files/{species}/samples_{species}.h5",
            f"{base_url}/files/{species}/series_{species}.h5"
        ]
        
        for url in metadata_patterns:
            try:
                logger.info(f"Trying to download: {url}")
                response = requests.head(url, timeout=10)
                if response.status_code == 200:
                    # File exists, try to download it
                    filename = url.split('/')[-1]
                    logger.info(f"Found metadata file: {filename}")
                    
                    # Download the file
                    response = requests.get(url, stream=True, timeout=30)
                    response.raise_for_status()
                    
                    with open(filename, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    
                    logger.info(f"Successfully downloaded metadata: {filename}")
                    return filename
                    
            except Exception as e:
                logger.warning(f"Failed to download {url}: {e}")
                continue
        
        logger.warning("No metadata-only files found. Will fall back to full dataset download.")
        return None
        
    except Exception as e:
        logger.error(f"Error in metadata-only download attempt: {e}")
        return None


def extract_gsm_ids(metadata_file: str, max_samples: int = None) -> Set[str]:
    """Extract human GSM IDs from ARCHS4 metadata file."""
    logger.info("Extracting GSM IDs...")
    gsm_ids = set()
    
    try:
        import h5py
        
        # Open the HDF5 file to get sample names
        with h5py.File(metadata_file, 'r') as f:
            # Get sample names from the data structure
            if 'meta' in f:
                meta_group = f['meta']
                if 'samples' in meta_group and 'geo_accession' in meta_group['samples']:
                    # The samples are stored as arrays in metadata fields
                    geo_accessions = meta_group['samples']['geo_accession']
                    total_samples = len(geo_accessions)
                    logger.info(f"Found {total_samples} samples in metadata file")
                    
                    # Limit samples if max_samples is specified
                    if max_samples and max_samples > 0:
                        geo_accessions = geo_accessions[:max_samples]
                        logger.info(f"Limiting to first {max_samples} samples")
                    
                    # Extract GSM IDs from geo_accession field
                    for accession in geo_accessions:
                        # Convert byte string to regular string and check if it's a GSM ID
                        accession_str = accession.decode('utf-8') if isinstance(accession, bytes) else str(accession)
                        if accession_str.startswith('GSM'):
                            gsm_ids.add(accession_str)
        
        logger.info(f"Extracted {len(gsm_ids)} unique GSM IDs")
        return gsm_ids
        
    except Exception as e:
        logger.error(f"Error extracting GSM IDs: {e}")
        return set()


def extract_gse_gsm_mapping(metadata_file: str, max_samples: int = None) -> Tuple[Set[str], Dict[str, str]]:
    """Extract unique GSE IDs and GSM to GSE mapping (one-to-one) from ARCHS4 metadata file."""
    logger.info("Extracting GSE IDs and GSM-GSE mappings...")
    gse_ids = set()
    gsm_to_gse_map = {}
    
    try:
        import h5py
        
        # Open the HDF5 file to get series and sample information
        with h5py.File(metadata_file, 'r') as f:
            # Get series information from the data structure
            if 'meta' in f:
                meta_group = f['meta']
                if 'samples' in meta_group and 'series_id' in meta_group['samples'] and 'geo_accession' in meta_group['samples']:
                    # Get the series IDs and geo accessions
                    series_ids = meta_group['samples']['series_id']
                    geo_accessions = meta_group['samples']['geo_accession']
                    
                    total_samples = len(series_ids)
                    logger.info(f"Found {total_samples} samples with series information")
                    
                    # Limit samples if max_samples is specified
                    if max_samples and max_samples > 0:
                        series_ids = series_ids[:max_samples]
                        geo_accessions = geo_accessions[:max_samples]
                        logger.info(f"Limiting to first {max_samples} samples for GSE mapping")
                    
                    # Create one-to-one GSM to GSE mapping and collect unique GSE IDs
                    for i in range(len(series_ids)):
                        series_id = series_ids[i]
                        geo_accession = geo_accessions[i]
                        
                        # Convert byte strings to regular strings
                        series_id_str = series_id.decode('utf-8') if isinstance(series_id, bytes) else str(series_id)
                        geo_accession_str = geo_accession.decode('utf-8') if isinstance(geo_accession, bytes) else str(geo_accession)
                        
                        # Only process if both are valid IDs
                        if series_id_str.startswith('GSE') and geo_accession_str.startswith('GSM'):
                            gse_ids.add(series_id_str)
                            gsm_to_gse_map[geo_accession_str] = series_id_str
        
        logger.info(f"Extracted {len(gse_ids)} unique GSE IDs and {len(gsm_to_gse_map)} GSM-GSE mappings")
        return gse_ids, gsm_to_gse_map
        
    except Exception as e:
        logger.error(f"Error extracting GSE-GSM mappings: {e}")
        return set(), {}


def save_gsm_ids(gsm_ids: Set[str], output_dir: Path, output_file: str = None):
    """Save GSM IDs to a text file."""
    if output_file:
        output_file_path = output_dir / output_file
    else:
        output_file_path = output_dir / "archs4_gsm_ids.txt"
    
    try:
        with open(output_file_path, 'w') as f:
            for gsm_id in sorted(gsm_ids):
                f.write(f"{gsm_id}\n")
        
        logger.info(f"Saved {len(gsm_ids)} GSM IDs to {output_file_path}")
        return output_file_path
        
    except Exception as e:
        logger.error(f"Error saving GSM IDs: {e}")
        return None


def save_gse_ids(gse_ids: Set[str], output_dir: Path, output_file: str = None):
    """Save unique GSE IDs to a text file."""
    if output_file:
        output_file_path = output_dir / f"{output_file}_gse_ids.txt"
    else:
        output_file_path = output_dir / "archs4_gse_ids.txt"
    
    try:
        with open(output_file_path, 'w') as f:
            for gse_id in sorted(gse_ids):
                f.write(f"{gse_id}\n")
        
        logger.info(f"Saved {len(gse_ids)} unique GSE IDs to {output_file_path}")
        return output_file_path
        
    except Exception as e:
        logger.error(f"Error saving GSE IDs: {e}")
        return None


def save_gse_gsm_mapping(gsm_to_gse_map: Dict[str, str], output_dir: Path, output_file: str = None):
    """Save GSM to GSE mapping (one-to-one) to a text file."""
    if output_file:
        output_file_path = output_dir / f"{output_file}_gse_gsm_mapping.txt"
    else:
        output_file_path = output_dir / "archs4_gse_gsm_mapping.txt"
    
    try:
        with open(output_file_path, 'w') as f:
            for gsm_id in sorted(gsm_to_gse_map.keys()):
                gse_id = gsm_to_gse_map[gsm_id]
                f.write(f"{gsm_id}\t{gse_id}\n")
        
        logger.info(f"Saved {len(gsm_to_gse_map)} GSM-GSE mappings to {output_file_path}")
        return output_file_path
        
    except Exception as e:
        logger.error(f"Error saving GSM-GSE mappings: {e}")
        return None


def main():
    """Main function to extract ARCHS4 IDs."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Extract GSM and GSE IDs from ARCHS4 dataset")
    parser.add_argument("--max-samples", type=int, default=None, 
                       help="Maximum number of samples to process (default: all samples)")
    parser.add_argument("--output-file", type=str, default=None,
                       help="Custom output filename (without extension, will create .txt files)")
    parser.add_argument("--species", type=str, default="human", choices=["human", "mouse"],
                       help="Species to extract data for (default: human)")
    
    args = parser.parse_args()
    
    logger.info("Starting ARCHS4 ID extraction...")
    if args.max_samples:
        logger.info(f"Limiting to maximum {args.max_samples} samples")
    if args.output_file:
        logger.info(f"Using custom output filename: {args.output_file}")
    
    # Try to get metadata-only file first
    metadata_file = try_download_metadata_only(species=args.species)
    
    # If metadata-only download failed, fall back to full dataset
    if not metadata_file:
        logger.info("Metadata-only download failed, trying full dataset...")
        metadata_file = get_archs4_metadata(species=args.species)
        if not metadata_file:
            logger.error("Failed to download ARCHS4 data. Exiting.")
            return
    
    # Create output directory
    output_dir = Path("archs4_samples")
    output_dir.mkdir(exist_ok=True)
    
    try:
        # Extract GSM IDs
        gsm_ids = extract_gsm_ids(metadata_file, max_samples=args.max_samples)
        if gsm_ids:
            gsm_filename = f"{args.output_file}_gsm_ids.txt" if args.output_file else None
            save_gsm_ids(gsm_ids, output_dir, output_file=gsm_filename)
        
        # Extract GSE IDs and GSM-GSE mappings
        gse_ids, gsm_to_gse_map = extract_gse_gsm_mapping(metadata_file, max_samples=args.max_samples)
        
        # Save unique GSE IDs
        if gse_ids:
            gse_ids_filename = f"{args.output_file}_gse_ids.txt" if args.output_file else None
            save_gse_ids(gse_ids, output_dir, output_file=args.output_file)
        
        # Save GSM to GSE mapping (one-to-one)
        if gsm_to_gse_map:
            gse_mapping_filename = f"{args.output_file}_gse_gsm_mapping.txt" if args.output_file else None
            save_gse_gsm_mapping(gsm_to_gse_map, output_dir, output_file=args.output_file)
        
        logger.info("ARCHS4 ID extraction completed successfully!")
        
    except Exception as e:
        logger.error(f"Unexpected error during extraction: {e}")
        raise


if __name__ == "__main__":
    main()
