#!/usr/bin/env python3
"""
Pre-download script for SapBERT model to enable local caching.
This script downloads the model once and caches it locally for future use.
"""

import os
import sys
from pathlib import Path
import torch
from transformers import AutoTokenizer, AutoModel

def download_sapbert_model():
    """Download and cache the SapBERT model locally."""
    
    # Set up cache directory relative to this script
    script_dir = Path(__file__).parent
    cache_dir = script_dir / "model_cache"
    cache_dir.mkdir(exist_ok=True)
    
    model_name = "cambridgeltl/SapBERT-from-PubMedBERT-fulltext"
    
    print(f"🚀 Starting SapBERT model download...")
    print(f"📁 Cache directory: {cache_dir}")
    print(f"🔗 Model: {model_name}")
    
    # Check if model is already cached
    model_dir = cache_dir / "models--cambridgeltl--SapBERT-from-PubMedBERT-fulltext"
    if model_dir.exists():
        print(f"✅ Model already cached at: {model_dir}")
        
        # Show cache info
        total_size = 0
        file_count = 0
        for file_path in model_dir.rglob("*"):
            if file_path.is_file():
                total_size += file_path.stat().st_size
                file_count += 1
        
        print(f"📊 Cache size: {round(total_size / (1024 * 1024), 2)} MB")
        print(f"📄 Files: {file_count}")
        return True
    
    try:
        print(f"⬇️  Downloading tokenizer...")
        tokenizer = AutoTokenizer.from_pretrained(
            model_name,
            cache_dir=str(cache_dir)
        )
        print(f"✅ Tokenizer downloaded successfully")
        
        print(f"⬇️  Downloading model...")
        model = AutoModel.from_pretrained(
            model_name,
            cache_dir=str(cache_dir)
        )
        print(f"✅ Model downloaded successfully")
        
        # Verify the cache
        if model_dir.exists():
            print(f"✅ Model successfully cached at: {model_dir}")
            
            # Calculate final cache size
            total_size = 0
            file_count = 0
            for file_path in model_dir.rglob("*"):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
                    file_count += 1
            
            print(f"📊 Final cache size: {round(total_size / (1024 * 1024), 2)} MB")
            print(f"📄 Total files: {file_count}")
            print(f"🎉 Model caching completed successfully!")
            return True
        else:
            print(f"❌ Model was not cached as expected")
            return False
            
    except Exception as e:
        print(f"❌ Failed to download model: {e}")
        print(f"💡 Make sure you have internet access and sufficient disk space")
        return False

def test_cached_model():
    """Test if the cached model can be loaded successfully."""
    
    print(f"\n🧪 Testing cached model...")
    
    try:
        from semantic_search import OntologySemanticSearch
        
        # Create a dummy dictionary for testing
        test_dict = {"test_term": "test_id"}
        test_dict_path = Path(__file__).parent / "test_dict.json"
        
        import json
        with open(test_dict_path, 'w') as f:
            json.dump(test_dict, f)
        
        # Test with local cache only
        search = OntologySemanticSearch(
            dictionary_path=str(test_dict_path),
            use_local_cache_only=True
        )
        
        print(f"✅ Cached model loaded successfully!")
        print(f"📊 Cache info: {search.get_cache_info()}")
        
        # Clean up test file
        test_dict_path.unlink()
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to load cached model: {e}")
        return False

def main():
    """Main function to download and test the model."""
    
    print(f"🔧 SapBERT Model Pre-download Script")
    print(f"=" * 50)
    
    # Download the model
    success = download_sapbert_model()
    
    if success:
        # Test the cached model
        test_success = test_cached_model()
        
        if test_success:
            print(f"\n🎉 All operations completed successfully!")
            print(f"💡 The model is now cached and ready for use with use_local_cache_only=True")
        else:
            print(f"\n⚠️  Model downloaded but testing failed. Check the error above.")
            sys.exit(1)
    else:
        print(f"\n❌ Model download failed. Please check the error above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
