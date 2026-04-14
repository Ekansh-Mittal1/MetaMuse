# MetaMuse

Agentic Metadata Curation System for GEO sample metadata extraction, curation, and normalization.

## 🚀 Quick Start

### 1. Install UV
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Set up environment variables
Create a `.env` file in the project root:
```bash
NCBI_EMAIL=your_email@example.com
OPENROUTER_API_KEY=your_openrouter_api_key
NCBI_API_KEY=your_ncbi_api_key  # Optional but recommended
```

### 3. Install dependencies
```bash
uv sync
```

### 4. Download local SQLite databases (GEO + optional PubMed)

Large database files are not committed to Git. After install, run the **data setup** entry point (implementation lives in `src/setup_data.py`; the root `setup.py` file is only a thin wrapper and is **not** setuptools packaging):

```bash
uv run setup-data
```

Equivalent:

```bash
uv run python setup.py
```

This downloads **`data/GEOmetadb.sqlite`** (~1 GiB compressed, ~20 GiB on disk after extraction) using the same downloader as the data-intake workflow.

**Optional — full PubMed baseline** (large download, long ingest; only if you want local PMID lookups without the NCBI API):

```bash
uv run setup-data --pubmed full --i-accept-pubmed-baseline-cost
```

By default PubMed is skipped; the script prints a suggested `PUBMED_SQLITE_PATH` so abstract tooling can find **`data/pubmed/pubmed.sqlite`** after you build it. You can also build a filtered DB with `src/utils/pubmed_ingest.py` (see that file’s docstring).

| Flag | Meaning |
|------|---------|
| `--data-dir PATH` | Root for `GEOmetadb.sqlite` and `pubmed/` (default: `data`) |
| `--skip-geo` | Do not download GEOmetadb |
| `--force` | Re-download GEO if present; with `--pubmed full`, remove existing `pubmed.sqlite` first |
| `--pubmed none` \| `full` | Skip PubMed (default) or full baseline download + ingest |

### 5. Verify installation
```bash
uv run metamuse --list-workflows
```

Equivalent:

```bash
uv run python main.py --list-workflows
```

The `metamuse` command is defined in `[project.scripts]` in `pyproject.toml` and calls `src/metamuse_cli.py`, so you do not need to be in a particular directory beyond having run `uv sync` in this repo (or using `uv run` from it).

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
| `samples_file` | archs4_samples/archs4_gsm_ids.txt | Path to sample IDs file |
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

- **GEOmetadb** — Required for SQL-based data intake. Obtain with `uv run setup-data` (recommended) or see **Local data (GEO & PubMed)** below for manual steps.
- **PubMed SQLite** — Optional; speeds up abstract lookups when present. Default env / code may expect `~/data/pubmed/pubmed.sqlite`; after `setup.py`, point **`PUBMED_SQLITE_PATH`** at `data/pubmed/pubmed.sqlite` (the setup script prints this).

### Sample lists

Included in `archs4_samples/`:

- `archs4_gsm_ids.txt` — Full sample list  
- `manual_100_samples.txt` — Curated test set  

---

## ⚡ Quick Reference

### Test Commands
```bash
# Quick test (20 samples, 8 minutes)
uv run python main.py batch_samples_efficient \
  "sample_count=20 batch_size=5 samples_file=archs4_samples/manual_100_samples.txt \
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

**Missing `data/GEOmetadb.sqlite`:**
- Run `uv run setup-data` (Quick Start, step 4).

**Missing environment variables:**
- Create `.env` file with required API keys

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

From the repo root (after `uv sync`):

```bash
uv run setup-data
```

This populates **`data/GEOmetadb.sqlite`**. The `data/` directory is gitignored; each developer generates their own copy.

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

- **Full baseline** (large): `uv run setup-data --pubmed full --i-accept-pubmed-baseline-cost`  
- **Custom / filtered ingest**: use `src/utils/pubmed_ingest.py` (`download`, `ingest`, optional `--filter-ids`).

Set **`PUBMED_SQLITE_PATH`** in `.env` if your database is not at the default `~/data/pubmed/pubmed.sqlite`, for example:

```bash
PUBMED_SQLITE_PATH=/absolute/path/to/repo/data/pubmed/pubmed.sqlite
```
