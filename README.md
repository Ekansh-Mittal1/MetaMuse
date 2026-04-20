# MetaMuse

Agentic Metadata Curation System for GEO sample metadata extraction, curation, and normalization.

## 🚀 Quick Start

Complete these steps from the **repository root** after cloning.

### 1. Install UV
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Configure environment variables

Create a `.env` file in the project root (same variables are read by `setup-data` and workflows):

```bash
NCBI_EMAIL=your_email@example.com
OPENROUTER_API_KEY=your_openrouter_api_key
NCBI_API_KEY=your_ncbi_api_key  # Optional but recommended for eutils rate limits
```

**Optional — normalization release download** (recommended so you do not build large FAISS indexes locally):

```bash
METAMUSE_GITHUB_REPOSITORY=yourOrg/MetaMuse
METAMUSE_NORMALIZATION_INDEXES_TAG=normalization-indexes-v0.1.0
# Or a direct URL to semantic_indexes.tar.gz:
# METAMUSE_NORMALIZATION_INDEXES_URL=https://github.com/...
```

See `src/normalization/README_INDEXES.md` for creating the release asset and optional Git LFS.

### 3. Install dependencies

```bash
uv sync
```

Optional: `uv sync --extra normalization` if you will run `setup-normalization --build-dictionaries` (OWL → JSON via **pronto**).

### 4. Local SQLite databases (GEO + PubMed)

Large databases are **not** in Git. Run:

```bash
uv run setup-data
```

Equivalent:

```bash
uv run python setup.py
```

This:

1. Downloads **`data/GEOmetadb.sqlite`** (~1 GiB compressed, ~20 GiB on disk after gunzip) using the same mirror as the data-intake workflow (`https://gbnci.cancer.gov/geo/` by default; override with **`GEOMETADB_DOWNLOAD_URL`** / **`GEOMETADB_MD5_URL`** if needed). The download staging directory **`geo_cache/`** is removed after a successful extract.
2. By default, builds a **filtered** **`data/pubmed/pubmed.sqlite`** with **NCBI efetch** (no PubMed baseline FTP). PMIDs come from **`data/samples/archs4_pubmed_ids.txt`** when present; otherwise up to **5000** from GEOmetadb `gse.pubmed_id`, then **`pubmed_filter_seed_pmids.txt`**. **`NCBI_EMAIL`** is required for efetch.

The command prints a suggested **`PUBMED_SQLITE_PATH`**; add it to `.env` if tools expect a non-default path (see **Local data** below).

**Optional — full PubMed baseline** (very large download + ingest):

```bash
uv run setup-data --pubmed full --i-accept-pubmed-baseline-cost
```

For baseline XML download/ingest only, see `src/utils/pubmed_ingest.py`.

| Flag | Meaning |
|------|---------|
| `--data-dir PATH` | Root for `GEOmetadb.sqlite` and `pubmed/` (default: `data`) |
| `--skip-geo` | Do not download GEOmetadb |
| `--force` | Re-download GEO if present; replace `pubmed.sqlite` when rebuilding PubMed |
| `--pubmed none` \| `filtered` \| `full` | PubMed: skip; **filtered efetch (default)**; full baseline |
| `--pubmed-filter-ids PATH` | PMID list (one per line); overrides `data/samples/archs4_pubmed_ids.txt` |
| `--pubmed-max-from-geo N` | Fallback only: max PMIDs from GEO if `data/samples/archs4_pubmed_ids.txt` is missing (default: 5000) |

### 5. Normalization assets (SapBERT + FAISS indexes)

Ontology **term JSON** is in Git under `src/normalization/dictionaries/`. **SapBERT** weights and **FAISS** indexes under `src/normalization/model_cache/` and `src/normalization/semantic_indexes/` are large and are **not** committed by default.

```bash
uv run setup-normalization
```

This downloads the SapBERT model into `src/normalization/model_cache/` (unless `--skip-model`), then:

- **By default:** if `METAMUSE_NORMALIZATION_INDEXES_URL` or `METAMUSE_GITHUB_REPOSITORY` / `GITHUB_REPOSITORY` is set, **downloads** `semantic_indexes.tar.gz` from a GitHub Release (with optional `--release-tag` / `METAMUSE_NORMALIZATION_INDEXES_TAG`, or `latest`). If the download fails or no URL is configured, it **builds** indexes locally (often **hours** on CPU).
- **`--download-indexes`** — download only; exit on failure (no local build).
- **`--build-indexes-only`** — skip release download; build indexes locally.
- **`--skip-indexes`** — model download only.
- **`--download-then-fill`** — after a successful download, build indexes only for dictionaries still missing an index.

Regenerate dictionaries from OWL (rare): `uv sync --extra normalization` then `uv run setup-normalization --build-dictionaries`.

### 6. Verify installation

```bash
uv run metamuse --list-workflows
```

Equivalent:

```bash
uv run python main.py --list-workflows
```

The `metamuse` entry point is defined in `[project.scripts]` in `pyproject.toml` and runs `src/metamuse_cli.py`. Use `uv run` from the repo (or any cwd with the same environment) after `uv sync`.

---

## 📋 Workflows

You can run workflows with **`uv run metamuse …`** (same arguments as `uv run python main.py …`).

### batch_samples_efficient
Multi-sample batch processing with quality control (production workflow).

**Basic Example:**
```bash
uv run python main.py batch_samples_efficient "sample_count=100 batch_size=5"
```

**Common Examples:**
```bash
# Process 50 samples with custom name
uv run python main.py batch_samples_efficient "sample_count=50 batch_name=my_test"

# Filter by sample type
uv run python main.py batch_samples_efficient "sample_count=100 sample_type_filter=primary_sample"

# Process specific fields only
uv run python main.py batch_samples_efficient "sample_count=50 target_fields=disease,tissue,organ"

# Fast mode (no quality control)
uv run python main.py batch_samples_efficient "sample_count=100 conditional_mode=classic"

# CSV output with parallel processing
uv run python main.py batch_samples_efficient "sample_count=200 max_workers=10 output_format=csv"
```

### deterministic_sql
Simple single-field processing workflow.

**Examples:**
```bash
# Process single sample (default: disease field)
uv run python main.py deterministic_sql "GSM1000981"

# Process for different field
uv run python main.py deterministic_sql "GSM1000981 target_field:tissue"

# Process multiple samples
uv run python main.py deterministic_sql "GSM1000981,GSM1000984 target_field:disease"
```

---

## 📊 Parameters

### batch_samples_efficient

| Parameter | Default | Description |
|-----------|---------|-------------|
| `sample_count` | 100 | Number of samples to process |
| `batch_size` | 5 | Samples per batch |
| `samples_file` | data/samples/archs4_gsm_ids.txt | Path to sample IDs file |
| `target_fields` | All | Comma-separated fields: disease,tissue,organ,cell_line,cell_type,developmental_stage,ethnicity,sex,age,assay_type,treatment |
| `sample_type_filter` | None | Filter by type: primary_sample, cell_line, or unknown |
| `conditional_mode` | eval | Mode: eval (quality control) or classic (faster) |
| `max_iterations` | 2 | Arbitrator cycles (eval mode only) |
| `max_workers` | None | Parallel workers (recommend 5-10) |
| `batch_name` | None | Custom batch name |
| `output_format` | parquet | Output format: parquet or csv |

### deterministic_sql

| Parameter | Default | Description |
|-----------|---------|-------------|
| Input | Required | GSM ID(s), comma-separated |
| `target_field` | disease | Field to extract: disease,tissue,organ,cell_line,cell_type,developmental_stage,ethnicity,sex,age,assay_type,treatment |

**Usage:** `"GSM1000981 target_field:tissue"`

---

## 📁 Output

### batch_samples_efficient
```
batch/batch_{name}_{timestamp}/
├── batch_results.csv                    # Main results file
├── comprehensive_batch_results.csv      # Detailed results
└── processing_log.txt                   # Execution log
```

### deterministic_sql
```
sandbox/det_sql_{session_id}/
├── workflow_summary.json
├── curator_output.json
└── normalizer_output.json
```

---

## 🔧 Data requirements

- **GEOmetadb** — Required for SQL-based data intake. Use **Quick Start → step 4** (`uv run setup-data`) or the manual GEO steps under **Local data (GEO & PubMed)**.
- **PubMed SQLite** — Built by default with `setup-data` as **`data/pubmed/pubmed.sqlite`**. Set **`PUBMED_SQLITE_PATH`** in `.env` to that path (or elsewhere) so agents and tools resolve the same file; `setup-data` prints a suggested export.
- **Normalization** — For ontology semantic search, run **Quick Start → step 5** (`uv run setup-normalization`). Indexes are fastest via a **GitHub Release** tarball; see `src/normalization/README_INDEXES.md`.

### Sample lists

Committed under **`data/samples/`** (see `.gitignore`; other files in that directory stay local):

- `archs4_gsm_ids.txt` — Default GSM list for batch workflows  
- `archs4_pubmed_ids.txt` — Default PMID list for `setup-data` filtered PubMed  
- `archs4_gse_ids.txt` — Default input for `src/utils/extract_pubmed_ids.py`  

Other lists (e.g. `manual_100_samples.txt`) live in the same folder but are not tracked in git.

---

## ⚡ Quick Reference

### Test Commands
```bash
# Quick test (20 samples, 8 minutes)
uv run python main.py batch_samples_efficient \
  "sample_count=20 batch_size=5 samples_file=data/samples/manual_100_samples.txt \
   batch_name=test conditional_mode=classic output_format=csv"

# Single sample test (30 seconds)
uv run python main.py deterministic_sql "GSM1000981"
```

### Production Commands
```bash
# Production batch (100 samples with quality control)
uv run python main.py batch_samples_efficient \
  "sample_count=100 batch_size=10 conditional_mode=eval max_iterations=2 \
   max_workers=10 batch_name=production output_format=csv"
```

---

## 🐛 Troubleshooting

**Missing `data/GEOmetadb.sqlite` or `data/pubmed/pubmed.sqlite`:**
- Run `uv run setup-data` (Quick Start, step 4). Ensure **`NCBI_EMAIL`** is set for the default filtered PubMed build.

**Missing normalization indexes (`src/normalization/semantic_indexes/`):**
- Run `uv run setup-normalization` (Quick Start, step 5). Configure **`METAMUSE_GITHUB_REPOSITORY`** (or **`METAMUSE_NORMALIZATION_INDEXES_URL`**) so the default path can download release assets; otherwise the tool builds locally (slow).

**Missing environment variables:**
- Create a `.env` file with the keys in Quick Start, step 2.

**macOS + OpenMP (“Initializing libomp.dylib…”) when building indexes:**
- The project sets **`KMP_DUPLICATE_LIB_OK=TRUE`** automatically on Darwin before loading PyTorch/FAISS; you can also export it in your shell if an older entrypoint is used.

**Git LFS errors on clone/pull (e.g. “Object does not exist on the server”, smudge failed):**

Some forks may use Git LFS for large blobs. Install [Git LFS](https://git-lfs.com/) and run `git lfs pull`. If the remote LFS object is missing (404), use:

```bash
GIT_LFS_SKIP_SMUDGE=1 git clone <repo-url>
```

Optional **normalization** LFS for FAISS files is documented in `src/normalization/README_INDEXES.md` (separate from `data/`). Sample lists under **`data/samples/*.txt`** are normal Git files (not LFS).

**`data/pubmed/pubmed.sqlite` / `data/GEOmetadb.sqlite`:** These are **not** in Git (use `uv run setup-data`). If an older branch tracked them via LFS, prefer updating to current `main` or clone with `GIT_LFS_SKIP_SMUDGE=1` and run `setup-data`.

**UV issues:**
```bash
rm -rf .venv
uv sync
```

**Import errors:**
```bash
find . -type d -name __pycache__ -exec rm -rf {} +
uv sync --reinstall
```

---

## 📖 Available Fields

**Direct extraction:** organism, series_id, pubmed_id, platform_id, instrument

**Curated extraction:** disease, tissue, organ, cell_line, cell_type, developmental_stage, ethnicity, sex, age, assay_type, treatment

**Normalized (ontology-mapped):** disease, tissue, organ

---

## 🗄️ Local data (GEO & PubMed)

### Recommended: `setup-data`

Same behavior as **Quick Start → step 4**. From the repo root:

```bash
uv run setup-data
```

This creates **`data/GEOmetadb.sqlite`** and, by default, **filtered** **`data/pubmed/pubmed.sqlite`** (NCBI efetch; PMIDs from **`data/samples/archs4_pubmed_ids.txt`** when present; **`NCBI_EMAIL`** required). Large files under `data/` stay gitignored; only selected lists under **`data/samples/`** are tracked—see `.gitignore`. GEO uses **`https://gbnci.cancer.gov/geo/`** by default; override with **`GEOMETADB_DOWNLOAD_URL`** / **`GEOMETADB_MD5_URL`** if needed.

### Manual GEOmetadb (alternative)

If you prefer not to use `setup-data`, download the official gzip and extract into `data/`:

```bash
mkdir -p data
wget -O data/GEOmetadb.sqlite.gz https://gbnci.cancer.gov/geo/GEOmetadb.sqlite.gz
gzip -d data/GEOmetadb.sqlite.gz
```

Verify:

```bash
ls -lh data/GEOmetadb.sqlite
# Expect on the order of ~20 GiB uncompressed
```

### PubMed SQLite

- **Default (with `setup-data`)**: filtered DB via **efetch** using **`data/samples/archs4_pubmed_ids.txt`**; needs `NCBI_EMAIL`.  
- **Full baseline** (large): `uv run setup-data --pubmed full --i-accept-pubmed-baseline-cost`  
- **FTP baseline + `--filter-ids`**: use `src/utils/pubmed_ingest.py` (`download`, `ingest`, optional `--filter-ids`).

Set **`PUBMED_SQLITE_PATH`** in `.env` if tools should not rely on implicit defaults, for example:

```bash
PUBMED_SQLITE_PATH=/absolute/path/to/your/checkout/data/pubmed/pubmed.sqlite
```
