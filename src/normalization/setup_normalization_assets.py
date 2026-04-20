#!/usr/bin/env python3
"""
Prepare normalization assets for a fresh clone.

- Ontology term JSON files under ``src/normalization/dictionaries/`` are **tracked in git**.
- SapBERT ``model_cache/`` is downloaded unless skipped.
- FAISS ``semantic_indexes/`` can be **downloaded** from a GitHub Release tarball (fast) or
  **built locally** (slow), or tracked with **Git LFS** if your fork opts in (see ``README_INDEXES.md``).

Usage::

    uv sync --extra normalization   # pronto, only if using --build-dictionaries
    uv run setup-normalization
    # ^ By default: tries release download if URL/repo is configured; otherwise builds locally.
    uv run setup-normalization --build-indexes-only   # never try release download
    uv run setup-normalization --download-indexes     # release/URL only; fail if download fails

Environment: ``HF_HOME`` optional; GPU optional for index build.
On macOS, ``KMP_DUPLICATE_LIB_OK=TRUE`` is set automatically unless already exported.

Release download (default when configured): set ``METAMUSE_NORMALIZATION_INDEXES_URL`` or
``METAMUSE_GITHUB_REPOSITORY`` (``owner/repo``) + optional ``METAMUSE_NORMALIZATION_INDEXES_TAG``
(default ``normalization-indexes-v{package_version}``), or pass ``--release-tag latest``.
If download fails, the tool **falls back** to building indexes locally (unless ``--download-indexes``).
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Optional

import requests
from tqdm import tqdm

if sys.platform == "darwin":
    os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")

REPO_ROOT = Path(__file__).resolve().parents[2]
DICT_DIR = REPO_ROOT / "src" / "normalization" / "dictionaries"
INDEX_DIR = REPO_ROOT / "src" / "normalization" / "semantic_indexes"
PRE_DOWNLOAD = REPO_ROOT / "src" / "normalization" / "pre_download_models.py"
BUILD_DICT = REPO_ROOT / "src" / "normalization" / "build_dictionarys.py"

INDEX_ARCHIVE_NAME = "semantic_indexes.tar.gz"


def _default_indexes_release_tag() -> str:
    try:
        from importlib.metadata import version

        v = version("MetaMuse")
    except Exception:
        v = "0.1.0"
    return f"normalization-indexes-v{v}"


def _release_asset_url(owner: str, repo: str, tag: str, asset: str) -> str:
    return f"https://github.com/{owner}/{repo}/releases/download/{tag}/{asset}"


def _parse_github_repo(spec: str) -> tuple[str, str]:
    spec = spec.strip().strip("/")
    if spec.count("/") != 1:
        raise ValueError(f"Expected owner/repo, got: {spec!r}")
    owner, repo = spec.split("/", 1)
    if not owner or not repo:
        raise ValueError(f"Invalid owner/repo: {spec!r}")
    return owner, repo


def _latest_release_asset_browser_url(owner: str, repo: str, asset_name: str) -> str:
    url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
    r = requests.get(
        url,
        timeout=120,
        headers={"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"},
    )
    r.raise_for_status()
    data = r.json()
    for a in data.get("assets", []) or []:
        if a.get("name") == asset_name:
            bu = a.get("browser_download_url")
            if isinstance(bu, str) and bu.startswith("https://"):
                return bu
    raise RuntimeError(
        f"No release asset named {asset_name!r} on latest release for {owner}/{repo}. "
        f"Attach {INDEX_ARCHIVE_NAME} to the release (see src/normalization/README_INDEXES.md)."
    )


def _resolve_indexes_download_url_optional(
    *,
    indexes_url: Optional[str],
    github_repo: Optional[str],
    release_tag: Optional[str],
) -> Optional[str]:
    """Return a download URL if configuration allows; otherwise ``None`` (local build path)."""
    if indexes_url:
        return indexes_url.strip()
    env_url = (os.getenv("METAMUSE_NORMALIZATION_INDEXES_URL") or "").strip()
    if env_url:
        return env_url

    repo_spec = (github_repo or "").strip() or (
        os.getenv("METAMUSE_GITHUB_REPOSITORY") or os.getenv("GITHUB_REPOSITORY") or ""
    ).strip()
    if not repo_spec:
        return None

    owner, repo = _parse_github_repo(repo_spec)
    tag = (release_tag or "").strip() or (
        os.getenv("METAMUSE_NORMALIZATION_INDEXES_TAG") or _default_indexes_release_tag()
    )
    if tag.lower() == "latest":
        return _latest_release_asset_browser_url(owner, repo, INDEX_ARCHIVE_NAME)
    return _release_asset_url(owner, repo, tag, INDEX_ARCHIVE_NAME)


def _download_file(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length") or 0)
        with open(dest, "wb") as f:
            if total:
                pbar = tqdm(total=total, unit="B", unit_scale=True, desc="Downloading indexes")
            else:
                pbar = tqdm(unit="B", unit_scale=True, desc="Downloading indexes")
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                f.write(chunk)
                pbar.update(len(chunk))
            pbar.close()


def _extract_index_archive(archive: Path, normalization_parent: Path) -> None:
    """Extract ``semantic_indexes`` tree into ``normalization_parent/`` (contains ``dictionaries/``)."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        with tarfile.open(archive, "r:*") as tf:
            try:
                tf.extractall(tmp_path, filter="data")
            except TypeError:
                tf.extractall(tmp_path)

        candidates = list(tmp_path.iterdir())
        if len(candidates) == 1 and candidates[0].is_dir():
            root = candidates[0]
        else:
            root = tmp_path

        if (root / "semantic_indexes").is_dir():
            src_tree = root / "semantic_indexes"
        elif root.name == "semantic_indexes" and root.is_dir():
            src_tree = root
        else:
            # Flat archive: *.index / *.pkl at root
            dest = normalization_parent / "semantic_indexes"
            dest.mkdir(parents=True, exist_ok=True)
            for p in root.rglob("*"):
                if p.is_file() and p.suffix in (".index", ".pkl"):
                    shutil.copy2(p, dest / p.name)
            return

        dest = normalization_parent / "semantic_indexes"
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(src_tree, dest)


def _install_indexes_from_url(url: str) -> int:
    """Download and extract ``semantic_indexes`` from ``url``. Returns 0 on success, 1 on failure."""
    print(f"⬇️  Downloading normalization indexes from:\n   {url}")
    INDEX_DIR.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(suffix=".tar.gz")
    os.close(fd)
    tmp_file = Path(tmp_path)
    try:
        _download_file(url, tmp_file)
        _extract_index_archive(tmp_file, INDEX_DIR.parent)
    finally:
        try:
            tmp_file.unlink(missing_ok=True)
        except OSError:
            pass

    dict_files = sorted(DICT_DIR.glob("*_terms.json"))
    if dict_files and not any(_index_files_ready(p) for p in dict_files):
        print(
            "⚠️  Download finished but no index metadata matched dictionary files. "
            "Check archive layout (expect top-level ``semantic_indexes/`` in the tarball).",
            file=sys.stderr,
        )
        return 1

    print(f"✅ Indexes installed under {INDEX_DIR}")
    return 0


def download_semantic_indexes(
    *,
    indexes_url: Optional[str],
    github_repo: Optional[str],
    release_tag: Optional[str],
) -> int:
    """Strict download: requires a resolvable URL; no local build fallback."""
    url = _resolve_indexes_download_url_optional(
        indexes_url=indexes_url,
        github_repo=github_repo,
        release_tag=release_tag,
    )
    if not url:
        print(
            "❌ No download URL configured. Pass --indexes-url URL, or set "
            "METAMUSE_NORMALIZATION_INDEXES_URL, or set METAMUSE_GITHUB_REPOSITORY=owner/repo "
            "with --release-tag / METAMUSE_NORMALIZATION_INDEXES_TAG (see README_INDEXES.md).",
            file=sys.stderr,
        )
        return 2
    try:
        return _install_indexes_from_url(url)
    except (requests.RequestException, OSError, RuntimeError, ValueError, tarfile.TarError) as e:
        print(f"❌ Index download failed: {e}", file=sys.stderr)
        return 1


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
        description="Download SapBERT cache; download or build normalization FAISS indexes."
    )
    parser.add_argument(
        "--build-dictionaries",
        action="store_true",
        help="Regenerate *_terms.json from OWL via pronto (needs: uv sync --extra normalization).",
    )
    parser.add_argument(
        "--skip-model",
        action="store_true",
        help="Do not run SapBERT pre-download (semantic search still needs the model on disk).",
    )
    parser.add_argument(
        "--skip-indexes",
        action="store_true",
        help="Skip index download/build.",
    )
    parser.add_argument(
        "--download-indexes",
        action="store_true",
        help=(
            f"Download {INDEX_ARCHIVE_NAME} only (no local build). "
            "Exits with an error if the URL cannot be resolved or download fails."
        ),
    )
    parser.add_argument(
        "--build-indexes-only",
        action="store_true",
        help="Skip release download and build FAISS indexes locally.",
    )
    parser.add_argument(
        "--indexes-url",
        type=str,
        default=None,
        help="Direct URL to the indexes archive (overrides env METAMUSE_NORMALIZATION_INDEXES_URL).",
    )
    parser.add_argument(
        "--github-repo",
        type=str,
        default=None,
        metavar="OWNER/REPO",
        help="GitHub repository for release download (overrides METAMUSE_GITHUB_REPOSITORY).",
    )
    parser.add_argument(
        "--release-tag",
        type=str,
        default=None,
        metavar="TAG",
        help="Release tag for fixed URL download, or 'latest' to use the repository's latest release.",
    )
    parser.add_argument(
        "--force-indexes",
        action="store_true",
        help="Rebuild FAISS indexes even if metadata + index files already exist (local build only).",
    )
    parser.add_argument(
        "--download-then-fill",
        action="store_true",
        help="After a successful release download, run a local build for any dictionary still missing an index.",
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
        print("\n✅ Skipping indexes (--skip-indexes).")
        return 0

    if args.build_indexes_only and args.download_indexes:
        print("❌ Use only one of --build-indexes-only and --download-indexes.", file=sys.stderr)
        return 2

    if not args.force_indexes and all(_index_files_ready(p) for p in dict_files):
        print("✅ All semantic indexes already present.")
        return 0

    if args.download_indexes:
        rc = download_semantic_indexes(
            indexes_url=args.indexes_url,
            github_repo=args.github_repo,
            release_tag=args.release_tag,
        )
        if rc != 0:
            return rc
        if args.download_then_fill:
            rc = _build_indexes_for_dictionaries(skip_existing=True)
            if rc != 0:
                return rc
        return 0

    if args.build_indexes_only:
        return _build_indexes_for_dictionaries(skip_existing=not args.force_indexes)

    url = _resolve_indexes_download_url_optional(
        indexes_url=args.indexes_url,
        github_repo=args.github_repo,
        release_tag=args.release_tag,
    )
    if url:
        try:
            rc = _install_indexes_from_url(url)
            if rc == 0:
                if args.download_then_fill:
                    brc = _build_indexes_for_dictionaries(skip_existing=True)
                    if brc != 0:
                        return brc
                return 0
            print(
                "⚠️  Release download did not produce usable indexes; building locally…",
                file=sys.stderr,
            )
        except (requests.RequestException, OSError, RuntimeError, ValueError, tarfile.TarError) as e:
            print(f"⚠️  Release download failed ({e}); building indexes locally…", file=sys.stderr)
    else:
        print(
            "ℹ️  No release URL configured (set METAMUSE_GITHUB_REPOSITORY or "
            "METAMUSE_NORMALIZATION_INDEXES_URL). Building indexes locally (slow on CPU)…",
        )

    return _build_indexes_for_dictionaries(skip_existing=not args.force_indexes)


if __name__ == "__main__":
    raise SystemExit(main())
