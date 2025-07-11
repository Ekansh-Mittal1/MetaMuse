#!/usr/bin/env python3

from src.tools.extract_geo_metadata import get_gsm_metadata, get_gse_metadata, get_paper_extracts, get_gse_series_matrix

def main():
    print("Testing GEO Metadata Extraction Functions")
    print("=" * 50)
    
    # Test GSM metadata
    print("\n1. Testing GSM Metadata:")
    try:
        gsm_result = get_gsm_metadata("GSM1019742")
        print(f"GSM Metadata: {gsm_result}")
    except Exception as e:
        print(f"GSM Error: {e}")
    
    # Test GSE metadata
    print("\n2. Testing GSE Metadata:")
    try:
        gse_result = get_gse_metadata("GSE12345")
        print(f"GSE Metadata: {gse_result}")
    except Exception as e:
        print(f"GSE Error: {e}")
    
    # Test paper extracts
    print("\n3. Testing Paper Extracts:")
    try:
        paper_result = get_paper_extracts("GSE12345")
        print(f"Paper Extracts: {paper_result}")
        print(f"Abstract: {paper_result.get('summary', 'No abstract')}")
    except Exception as e:
        print(f"Paper Error: {e}")
    
    # Test series matrix
    print("\n4. Testing Series Matrix:")
    try:
        matrix_result = get_gse_series_matrix("GSE12345")
        print(f"Series Matrix: {matrix_result}")
        print(f"Sample Count: {matrix_result.get('sample_count', 0)}")
        print(f"Platform Count: {matrix_result.get('platform_count', 0)}")
    except Exception as e:
        print(f"Matrix Error: {e}")

if __name__ == "__main__":
    main() 