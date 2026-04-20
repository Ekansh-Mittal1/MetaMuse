#!/usr/bin/env python3
"""
Prepare normalization assets for a fresh clone.

- Ontology term JSON files under ``src/normalization/dictionaries/`` are **tracked in git**.
- SapBERT ``model_cache/`` and FAISS ``semantic_indexes/`` are **not** in git (large); this
  script downloads the model (if needed) and builds indexes.

Usage::

    uv sync --extra normalization   # pronto, only if using --build-dictionaries
    uv run setup-normalization
    uv run setup-normalization --build-dictionaries   # regenerate JSON from OWL (slow)

Environment: same as the rest of the app (``HF_HOME`` optional; GPU optional for index build).
On macOS, ``KMP_DUPLICATE_LIB_OK=TRUE`` is set automatically (see ``semantic_search.py``)
unless you already exported it.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path

if sys.platform == "darwin":
    os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")

REPO_ROOT = Path(__file__).resolve().parents[2]
DICT_DIR = REPO_ROOT / "src" / "normalization" / "dictionaries"
INDEX_DIR = REPO_ROOT / "src" / "normalization" / "semantic_indexes"
PRE_DOWNLOAD = REPO_ROOT / "src" / "normalization" / "pre_download_models.py"
BUILD_DICT = REPO_ROOT / "src" / "normalization" / "build_dictionarys.py"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _index_files_ready(dict_path: Path) -> bool:
    stem = dict_path.stem
    meta = INDEX_DIR / f"{stem}_metadata.pkl"
    if not meta.is_file():
        return False
    cpu = INDEX_DIR / f"{stem}_faiss_cpu.index"
    gpu = INDEX_DIR / f"{stem}_faiss_gpu.index"
    return cpu.is_file() or gpu.is_file()


def _ensure_model_cache() -> int:
    if not PRE_DOWNLOAD.is_file():
        print(f"⚠️  Missing {PRE_DOWNLOAD}", file=sys.stderr)
        return 1
    mod = _load_module("pre_download_models", PRE_DOWNLOAD)
    fn = getattr(mod, "download_sapbert_model", None)
    if fn is None:
        print("⚠️  pre_download_models has no download_sapbert_model()", file=sys.stderr)
        return 1
    return 0 if fn() else 1


def _build_indexes_for_dictionaries(*, skip_existing: bool) -> int:
    sys.path.insert(0, str(REPO_ROOT / "src" / "normalization"))
    from semantic_search import OntologySemanticSearch  # noqa: E402

    DICT_DIR.mkdir(parents=True, exist_ok=True)
    INDEX_DIR.mkdir(parents=True, exist_ok=True)

    dict_files = sorted(DICT_DIR.glob("*_terms.json"))
    if not dict_files:
        print(
            f"❌ No *_terms.json under {DICT_DIR}.\n"
            "   Clone the latest repo (dictionaries are in git) or run with --build-dictionaries.",
            file=sys.stderr,
        )
        return 1

    for dict_path in dict_files:
        if skip_existing and _index_files_ready(dict_path):
            print(f"⏭️  Index already present for {dict_path.name}, skipping")
            continue
        print(f"\n📇 Building semantic index for {dict_path.name} …")
        try:
            searcher = OntologySemanticSearch(str(dict_path))
            searcher.build_index()
            searcher.save_index(str(INDEX_DIR))
        except Exception as e:
            print(f"❌ Index build failed for {dict_path.name}: {e}", file=sys.stderr)
            return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Download SapBERT cache and build normalization FAISS indexes."
    )
    parser.add_argument(
        "--build-dictionaries",
        action="store_true",
        help="Regenerate *_terms.json from OWL via pronto (needs: uv sync --extra normalization).",
    )
    parser.add_argument(
        "--skip-model",
        action="store_true",
        help="Do not run SapBERT pre-download (fail later if model cache is empty).",
    )
    parser.add_argument(
        "--skip-indexes",
        action="store_true",
        help="Only download model / build dictionaries; skip FAISS index build.",
    )
    parser.add_argument(
        "--force-indexes",
        action="store_true",
        help="Rebuild FAISS indexes even if metadata + index files already exist.",
    )
    args = parser.parse_args(argv)

    if not REPO_ROOT.is_dir():
        print("❌ Could not resolve repository root.", file=sys.stderr)
        return 1

    if args.build_dictionaries:
        try:
            import pronto  # noqa: F401
        except ImportError:
            print(
                "❌ ``pronto`` is not installed. Run:\n"
                "     uv sync --extra normalization\n"
                "   then re-try with --build-dictionaries.",
                file=sys.stderr,
            )
            return 1
        print("\n=== Build ontology term dictionaries (OWL → JSON) ===\n")
        try:
            cwd = Path.cwd()
            try:
                os.chdir(REPO_ROOT)
                mod = _load_module("build_dictionarys", BUILD_DICT)
                mod.main()
            finally:
                os.chdir(cwd)
        except Exception as e:
            print(f"❌ build_dictionarys failed: {e}", file=sys.stderr)
            return 1

    dict_files = sorted(DICT_DIR.glob("*_terms.json"))
    if not dict_files:
        print(
            f"❌ No dictionary JSON files in {DICT_DIR}.\n"
            "   Pull the latest git, or install pronto and run with --build-dictionaries.",
            file=sys.stderr,
        )
        return 1

    if not args.skip_model:
        rc = _ensure_model_cache()
        if rc != 0:
            return rc

    if args.skip_indexes:
        print("\n✅ Skipping index build (--skip-indexes).")
        return 0

    return _build_indexes_for_dictionaries(skip_existing=not args.force_indexes)


if __name__ == "__main__":
    raise SystemExit(main())
