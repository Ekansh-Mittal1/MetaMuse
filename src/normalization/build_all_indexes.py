#!/usr/bin/env python3
"""Build FAISS semantic indexes for every ``*_terms.json`` in ``dictionaries/``."""

from __future__ import annotations

import sys
from pathlib import Path

_NORM_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_NORM_DIR))

from semantic_search import OntologySemanticSearch  # noqa: E402

DICT_DIR = _NORM_DIR / "dictionaries"
INDEX_DIR = _NORM_DIR / "semantic_indexes"


def main() -> int:
    dict_files = sorted(DICT_DIR.glob("*_terms.json"))
    if not dict_files:
        print(f"No dictionaries found under {DICT_DIR}")
        return 1

    print(f"Found {len(dict_files)} dictionaries:")
    for f in dict_files:
        print(f"  - {f.name}")

    INDEX_DIR.mkdir(parents=True, exist_ok=True)

    for dict_file in dict_files:
        print(f"\n{'=' * 60}")
        print(f"Building index for: {dict_file.name}")
        print("=" * 60)
        try:
            searcher = OntologySemanticSearch(str(dict_file))
            searcher.build_index()
            searcher.save_index(str(INDEX_DIR))
            print(f"Index built and saved for {dict_file.name}")
        except Exception as e:
            import traceback

            print(f"Error building index for {dict_file.name}: {e}")
            print(traceback.format_exc())
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
