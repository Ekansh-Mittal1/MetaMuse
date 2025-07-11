from src.tools.extract_geo_metadata import get_gsm_metadata, get_gse_metadata, get_gse_series_matrix, get_paper_abstract

def test_get_gsm_metadata(GSM_ID: str = "GSM1019742"):
    print("Starting test...")
    try:
        metadata = get_gsm_metadata(GSM_ID)
        print(f"Retrieved metadata: {metadata}")
        assert metadata is not None
        assert metadata["gsm_id"] == GSM_ID
        print("Test passed!")
    except Exception as e:
        print(f"Test failed with error: {e}")
        raise


def test_get_gse_metadata(GSE_ID: str = "GSE41588"):
    print("\n=== Testing GSE Metadata ===")
    try:
        gse_metadata = get_gse_metadata(GSE_ID)
        print(f"GSE Metadata: {gse_metadata}")
        assert gse_metadata is not None
        assert gse_metadata["gse_id"] == GSE_ID
        assert gse_metadata["type"] == "GSE"
        print("GSE Metadata test passed!")
    except Exception as e:
        print(f"GSE Metadata test failed with error: {e}")
        raise


def test_get_gse_series_matrix(GSE_ID: str = "GSE41588"):
    print("\n=== Testing GSE Series Matrix ===")
    try:
        matrix_data = get_gse_series_matrix(GSE_ID)
        print(f"Series Matrix Data: {matrix_data}")
        
        # Print table information
        print(f"\nTable Information:")
        print(f"  Sample Count: {matrix_data.get('sample_count', 0)}")
        print(f"  Platform Count: {matrix_data.get('platform_count', 0)}")
        print(f"  Samples: {matrix_data.get('samples', [])}")
        print(f"  Platforms: {matrix_data.get('platforms', [])}")
        
        assert matrix_data is not None
        assert matrix_data["gse_id"] == GSE_ID
        assert matrix_data["type"] == "series_matrix_metadata"
        print("Series Matrix test passed!")
    except Exception as e:
        print(f"Series Matrix test failed with error: {e}")
        raise


def test_get_paper_abstract(PMID: int = 23902433):
    print("\n=== Testing Paper Abstract ===")
    try:
        paper_abstract = get_paper_abstract(PMID)
        print(f"Paper Abstract: {paper_abstract}")
        
        # Print the abstract specifically
        abstract = paper_abstract.get("abstract", "No abstract available")
        print(f"\nAbstract: {abstract}")
        
        # Print title
        title = paper_abstract.get("title", "No title available")
        print(f"Title: {title}")
        
        # Print authors
        authors = paper_abstract.get("authors", [])
        print(f"Authors: {authors}")
        
        assert paper_abstract is not None
        assert paper_abstract["pmid"] == PMID
        print("Paper Abstract test passed!")
    except Exception as e:
        print(f"Paper Abstract test failed with error: {e}")
        raise
