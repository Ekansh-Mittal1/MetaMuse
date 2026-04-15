#!/usr/bin/env python3
"""
Prepare local SQLite databases for MetaMuse (GEOmetadb + PubMed SQLite).

Run via ``uv run setup-data`` or ``python setup.py`` from the repository root.

Paths (default ``--data-dir data``):

- ``data/GEOmetadb.sqlite`` — GEOmetadb (see ``download_geometadb``).
- ``data/pubmed/pubmed.sqlite`` — small **filtered** DB by default (NCBI efetch for
  PMIDs listed in ``data/samples/archs4_pubmed_ids.txt`` when present), no PubMed
  baseline FTP download. Set ``PUBMED_SQLITE_PATH`` for ``PubMedSQLiteManager``.

Use ``--pubmed full`` only for the full baseline (huge download + ingest); requires
``--i-accept-pubmed-baseline-cost``.
"""

from __future__ import annotations

import argparse
import os
import sys
from argparse import Namespace
from pathlib import Path

from dotenv import load_dotenv

from src.sample_paths import DEFAULT_PUBMED_IDS_FILE


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


def _load_pmids_from_text_file(path: Path) -> list[str]:
    out: list[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.isdigit():
            out.append(line)
    return list(dict.fromkeys(out))


def setup_pubmed_filtered(
    data_dir: Path,
    *,
    force: bool,
    filter_ids_path: Path | None,
    max_from_geo: int,
    geo_path: Path,
) -> None:
    """Build ``pubmed.sqlite`` via NCBI efetch (no baseline FTP)."""
    _ensure_repo_on_path()
    from src.utils.pubmed_efetch import build_pubmed_sqlite_from_pmids, collect_pmids_from_geometadb
    from src.utils.pubmed_ingest import load_pmid_filter

    db_path = data_dir / "pubmed" / "pubmed.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if force and db_path.exists():
        db_path.unlink()
        print(f"Removed existing {db_path}")

    pmids: list[str] = []
    if filter_ids_path is not None:
        p = filter_ids_path.expanduser().resolve()
        if not p.exists():
            raise SystemExit(f"--pubmed-filter-ids file not found: {p}")
        filt = load_pmid_filter(str(p))
        pmids = sorted(filt) if filt else []
        print(f"Loaded {len(pmids)} PMIDs from {p}")
    else:
        pubmed_list = (_repo_root() / DEFAULT_PUBMED_IDS_FILE).resolve()
        if pubmed_list.exists():
            pmids = _load_pmids_from_text_file(pubmed_list)
            print(f"Loaded {len(pmids)} PMIDs from {Path(DEFAULT_PUBMED_IDS_FILE).as_posix()}")
        elif geo_path.exists():
            pmids = collect_pmids_from_geometadb(geo_path, max_from_geo)
            print(
                f"No {DEFAULT_PUBMED_IDS_FILE}; collected {len(pmids)} "
                f"PMIDs from GEOmetadb (cap {max_from_geo})"
            )
        else:
            seed = _repo_root() / "pubmed_filter_seed_pmids.txt"
            if seed.exists():
                pmids = _load_pmids_from_text_file(seed)
                print(
                    f"No {DEFAULT_PUBMED_IDS_FILE} or GEOmetadb; "
                    f"using {len(pmids)} PMIDs from {seed.name}"
                )
            else:
                print(
                    f"⚠️  Missing {DEFAULT_PUBMED_IDS_FILE}, GEOmetadb at {geo_path}, "
                    f"and {seed.name}; skipping PubMed."
                )

    if not pmids:
        print("⚠️  No PMIDs to fetch; skipping PubMed SQLite.")
        return

    load_dotenv(override=True)
    email = (os.getenv("NCBI_EMAIL") or "").strip()
    if not email:
        print(
            "⚠️  Skipping PubMed SQLite: NCBI_EMAIL is not set (required for NCBI efetch).\n"
            "   Add it to .env and re-run setup-data, or use --pubmed none.",
            file=sys.stderr,
        )
        return

    api_key = (os.getenv("NCBI_API_KEY") or "").strip() or None
    print(f"Building {db_path.resolve()} via PubMed efetch ({len(pmids)} articles) …")
    n = build_pubmed_sqlite_from_pmids(pmids, db_path, email, api_key)
    print(f"PubMed SQLite ready (~{n} articles upserted).")


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
        description="Download GEOmetadb.sqlite and build pubmed.sqlite (filtered efetch by default)."
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
        help="Re-download GEO if present; replace pubmed.sqlite when building PubMed.",
    )
    parser.add_argument(
        "--pubmed",
        choices=("none", "filtered", "full"),
        default="filtered",
        help="PubMed: 'filtered' (default, efetch from data/samples/archs4_pubmed_ids.txt / overrides), 'none', or 'full' baseline.",
    )
    parser.add_argument(
        "--pubmed-filter-ids",
        type=Path,
        default=None,
        help="PMID list (one per line). Overrides data/samples/archs4_pubmed_ids.txt when set.",
    )
    parser.add_argument(
        "--pubmed-max-from-geo",
        type=int,
        default=5000,
        help="Fallback only: max PMIDs from GEOmetadb when data/samples/archs4_pubmed_ids.txt is missing.",
    )
    parser.add_argument(
        "--i-accept-pubmed-baseline-cost",
        action="store_true",
        help="Required with --pubmed full (many GiB download, long runtime, large DB).",
    )
    args = parser.parse_args(argv)

    load_dotenv(override=True)
    data_dir = args.data_dir.expanduser().resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    geo_path = data_dir / "GEOmetadb.sqlite"

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
    elif args.pubmed == "filtered":
        setup_pubmed_filtered(
            data_dir,
            force=args.force,
            filter_ids_path=args.pubmed_filter_ids,
            max_from_geo=args.pubmed_max_from_geo,
            geo_path=geo_path,
        )
    else:
        print("Skipping PubMed (--pubmed none).")

    pubmed_path = data_dir / "pubmed" / "pubmed.sqlite"
    print("\nSuggested environment (add to .env or your shell):")
    print(f"  export PUBMED_SQLITE_PATH={pubmed_path}")
    print("\nPaths:")
    print(f"  GEO:    {geo_path}")
    print(f"  PubMed: {pubmed_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
