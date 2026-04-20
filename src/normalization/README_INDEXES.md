# Normalization FAISS indexes (~700 MiB+)

Dictionary JSON lives in git under `dictionaries/`. **Semantic indexes** (`semantic_indexes/*.index`, `*.pkl`) are large and optional in git.

## Option A — GitHub Release (recommended)

Maintainers publish a tarball once; everyone else runs:

```bash
export METAMUSE_NORMALIZATION_INDEXES_URL="https://github.com/OWNER/REPO/releases/download/TAG/semantic_indexes.tar.gz"
uv run setup-normalization --download-indexes
```

Or pass the URL once:

```bash
uv run setup-normalization --download-indexes --indexes-url "https://github.com/..."
```

**Create the tarball** (from repo root, after indexes exist locally):

```bash
tar -czvf semantic_indexes.tar.gz -C src/normalization semantic_indexes
```

Attach `semantic_indexes.tar.gz` to a GitHub **Release** whose tag matches `METAMUSE_NORMALIZATION_INDEXES_TAG` (default in code) or whatever URL you publish.

**Convenience** (no full URL): set `METAMUSE_GITHUB_REPOSITORY=owner/repo` and `METAMUSE_NORMALIZATION_INDEXES_TAG=your-tag`, then:

```bash
uv run setup-normalization --download-indexes
```

Use `--release-tag latest` to resolve the asset from the repository’s **latest** GitHub release (asset name must still be `semantic_indexes.tar.gz`).

## Option B — Git LFS

1. Install [Git LFS](https://git-lfs.com/) and run `git lfs install`.
2. Remove the line `src/normalization/semantic_indexes/` from the **root** `.gitignore` (otherwise git will not track those files).
3. The repo root `.gitattributes` already maps `src/normalization/semantic_indexes/*.{index,pkl}` to LFS.
4. Add the built files, commit, and push; collaborators run `git lfs pull`.

Indexes must be built once (same SapBERT + faiss-cpu stack you expect consumers to use) before committing to LFS.

## Compatibility

Release/LFS packs should be produced with the **same** embedding model (`cambridgeltl/SapBERT-from-PubMedBERT-fulltext`) and **faiss-cpu** index layout as in this repo. CPU indexes are the most portable across machines.
