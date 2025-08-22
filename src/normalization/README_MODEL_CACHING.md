# Model Caching for Semantic Search

This directory contains the semantic search functionality with local model caching to avoid Hugging Face rate limits.

## 🚀 Quick Start

### 1. Pre-download the Model

First, download and cache the SapBERT model locally:

```bash
cd src/normalization
python pre_download_models.py
```

This will:
- Download the SapBERT model (~1.5GB)
- Cache it in `src/normalization/model_cache/`
- Test that the cached model works correctly

### 2. Use Local Cache (Default)

The semantic search now defaults to using local cache only:

```python
from semantic_search import OntologySemanticSearch

# This will use local cache only (default behavior)
search = OntologySemanticSearch(
    dictionary_path="path/to/dictionary.json"
)
```

### 3. Force Download (if needed)

If you need to download the model again:

```python
# This will download from Hugging Face if not cached
search = OntologySemanticSearch(
    dictionary_path="path/to/dictionary.json",
    use_local_cache_only=False
)
```

## 🔧 How It Works

### Cache Directory Structure

```
src/normalization/
├── model_cache/
│   └── models--cambridgeltl--SapBERT-from-PubMedBERT-fulltext/
│       ├── snapshots/
│       ├── refs/
│       └── blobs/
├── semantic_search.py
├── pre_download_models.py
└── test_local_cache.py
```

### Key Features

- **Default Local Cache**: `use_local_cache_only=True` by default
- **Automatic Fallback**: Falls back to download if local cache fails
- **Cache Validation**: Built-in methods to check cache status
- **Error Handling**: Clear error messages when cache is missing

## 📊 Cache Information

You can check the cache status programmatically:

```python
search = OntologySemanticSearch(dictionary_path="...")
cache_info = search.get_cache_info()
print(cache_info)

# Output:
# {
#     "cached": True,
#     "cache_dir": "/path/to/model_cache",
#     "model_dir": "/path/to/model_cache/models--cambridgeltl--SapBERT-from-PubMedBERT-fulltext",
#     "size_mb": 1456.78,
#     "file_count": 12
# }
```

## 🧪 Testing

Test the caching functionality:

```bash
cd src/normalization
python test_local_cache.py
```

This will verify:
- Cache status
- Local cache only mode
- Semantic search functionality

## 🚨 Troubleshooting

### Model Not Cached

If you get "Failed to load model from local cache":

```bash
cd src/normalization
python pre_download_models.py
```

### Cache Corrupted

If the cache is corrupted, delete and re-download:

```bash
cd src/normalization
rm -rf model_cache/
python pre_download_models.py
```

### Insufficient Disk Space

The model requires approximately 1.5GB of disk space. Ensure you have sufficient space before downloading.

## 💡 Benefits

- **No Rate Limits**: Avoid Hugging Face HTTP 429 errors
- **Faster Startup**: No need to download model each time
- **Offline Capability**: Work without internet connection
- **Consistent Performance**: Same model version every time

## 🔄 Updating Models

To update to a newer model version:

1. Delete the cache: `rm -rf model_cache/`
2. Run pre-download: `python pre_download_models.py`
3. The new version will be downloaded and cached

## 📝 Integration

The semantic search tool is used by the normalizer agent. With local caching enabled:

- **First run**: Downloads and caches the model
- **Subsequent runs**: Uses cached model instantly
- **No more rate limit errors**: All operations use local cache
