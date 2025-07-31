#!/usr/bin/env python3

import os
from pathlib import Path
from semantic_search import OntologySemanticSearch

# Directory containing the dictionaries
DICT_DIR = Path(__file__).parent / "dictionaries"

# List all JSON dictionary files, ignoring dron_terms.json
all_dict_files = [
    f for f in DICT_DIR.glob("*_terms.json")
    if f.name != "dron_terms.json"
]

print(f"Found {len(all_dict_files)} dictionaries (excluding dron):")
for f in all_dict_files:
    print(f"  - {f.name}")

for dict_file in all_dict_files:
    print(f"\n{'='*60}")
    print(f"Building index for: {dict_file.name}")
    print(f"{'='*60}")
    try:
        searcher = OntologySemanticSearch(str(dict_file))
        searcher.build_index()
        searcher.save_index("src/normalization/semantic_indexes")
        print(f"Index built and saved for {dict_file.name}")
    except Exception as e:
        import traceback
        print(f"Error building index for {dict_file.name}: {e}")
        print(traceback.format_exc()) 