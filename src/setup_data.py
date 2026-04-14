#!/usr/bin/env python3
"""
Prepare local SQLite databases for MetaMuse (GEOmetadb + optional PubMed).

Run via ``uv run setup-data`` or ``python setup.py`` from the repository root.

Paths (default ``--data-dir data``) align with the repo defaults:

- ``data/GEOmetadb.sqlite`` — same default as ``SQLiteDataIntakeWorkflow`` /
  ``download_geometadb``.
- ``data/pubmed/pubmed.sqlite`` — set ``PUBMED_SQLITE_PATH`` to this path so
  ``PubMedSQLiteManager`` and abstract extraction use it (see printed hint).

GEO download is **on** by default (~1 GiB download, ~20 GiB on disk after gunzip).

Full PubMed baseline is **off** by default (many GiB download + long ingest + large
SQLite). Use ``--pubmed full`` only with the explicit confirmation flag.
"""

from __future__ import annotations

import argparse
import os
import sys
from argparse import Namespace
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _ensure_repo_on_path() -> None:
    root = str(_repo_root())
    if root not in sys.path:
        sys.path.insert(0, root)


def setup_geo(data_dir: Path, *, force: bool) -> None:
    _ensure_repo_on_path()
    from src.tools.sqlite_manager import download_geometadb

    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "GEOmetadb.sqlite"
    print(f"GEOmetadb → {db_path.resolve()}")
    ok = download_geometadb(str(db_path), force=force)
    if not ok:
        raise SystemExit("GEOmetadb download failed.")
    print("GEOmetadb ready.")


def setup_pubmed_full(data_dir: Path, *, force: bool) -> None:
    _ensure_repo_on_path()
    from src.utils.pubmed_ingest import download_command, ingest_command

    baseline_dir = data_dir / "pubmed" / "baseline"
    db_path = data_dir / "pubmed" / "pubmed.sqlite"
    baseline_dir.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if force and db_path.exists():
        db_path.unlink()
        print(f"Removed existing {db_path}")

    print(f"Downloading PubMed baseline XML to {baseline_dir.resolve()} …")
    download_command(Namespace(which="baseline", out=str(baseline_dir)))

    print(f"Ingesting baseline into {db_path.resolve()} …")
    ingest_command(
        Namespace(db=str(db_path), dirs=[str(baseline_dir)], filter_ids=None)
    )
    print("PubMed SQLite ready.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Download GEOmetadb.sqlite and optionally build pubmed.sqlite."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Directory for GEOmetadb.sqlite and pubmed/ (default: data).",
    )
    parser.add_argument(
        "--skip-geo",
        action="store_true",
        help="Do not download GEOmetadb.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download GEO if present; for --pubmed full, remove existing pubmed.sqlite first.",
    )
    parser.add_argument(
        "--pubmed",
        choices=("none", "full"),
        default="none",
        help="PubMed: 'none' (default) or 'full' baseline download + ingest (very large).",
    )
    parser.add_argument(
        "--i-accept-pubmed-baseline-cost",
        action="store_true",
        help="Required with --pubmed full (many GiB download, long runtime, large DB).",
    )
    args = parser.parse_args(argv)

    data_dir = args.data_dir.expanduser().resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    if not args.skip_geo:
        setup_geo(data_dir, force=args.force)
    else:
        print("Skipping GEO (--skip-geo).")

    if args.pubmed == "full":
        if not args.i_accept_pubmed_baseline_cost:
            print(
                "Refusing --pubmed full without --i-accept-pubmed-baseline-cost.\n"
                "PubMed baseline is a large NLM FTP download and a long one-time ingest.\n"
                "Re-run with that flag if you intend to proceed.",
                file=sys.stderr,
            )
            return 2
        setup_pubmed_full(data_dir, force=args.force)
    else:
        print(
            "Skipping PubMed (--pubmed none). For a local abstract DB, run:\n"
            "  uv run setup-data --pubmed full --i-accept-pubmed-baseline-cost\n"
            "or use src/utils/pubmed_ingest.py (download / ingest / --filter-ids).\n"
        )

    pubmed_path = data_dir / "pubmed" / "pubmed.sqlite"
    geo_path = data_dir / "GEOmetadb.sqlite"
    print("\nSuggested environment (add to .env or your shell):")
    print(f"  export PUBMED_SQLITE_PATH={pubmed_path}")
    print("\nPaths:")
    print(f"  GEO:    {geo_path}")
    print(f"  PubMed: {pubmed_path} (after optional full ingest)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
