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

### 4. Verify installation
```bash
uv run python main.py --list-workflows
```

---

## 📋 Workflows

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
| `target_fields` | All | Comma-separated fields: disease,tissue,organ,cell_line,cell_type,developmental_stage,ethnicity,gender,age,assay_type,treatment |
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
| `target_field` | disease | Field to extract: disease,tissue,organ,cell_line,cell_type,developmental_stage,ethnicity,gender,age,assay_type,treatment |

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

## 🔧 Data Requirements

### Required Databases

1. **GEOmetadb.sqlite** (18+ GB) - Place in `data/GEOmetadb.sqlite`
2. **PubMed database** (50+ MB) - Place in `data/pubmed/`

### Sample Lists

Included in `archs4_samples/`:
- `archs4_gsm_ids.txt` - Full sample list
- `manual_100_samples.txt` - Curated test set

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

**Curated extraction:** disease, tissue, organ, cell_line, cell_type, developmental_stage, ethnicity, gender, age, assay_type, treatment

**Normalized (ontology-mapped):** disease, tissue, organ

---

## 🗄️ Data Setup

### GEOmetadb.sqlite Setup

The `GEOmetadb.sqlite` file (19GB) contains GEO metadata and is required for the workflows. Due to GitHub's file size limits, it's not included in this repository.

**Download:**
```bash
# Create data directory
mkdir -p data/

# Download and extract
wget https://gbnci.cancer.gov/geo/GEOmetadb.sqlite.gz
gzip -d GEOmetadb.sqlite.gz
mv GEOmetadb.sqlite data/
```

**Verify Setup:**
```bash
ls -lh data/GEOmetadb.sqlite
# Should show ~19GB file
```

### Other Data Files

The following files are included in this repository:
- `data/pubmed/pubmed.sqlite` (51MB) - PubMed abstracts
- `archs4_samples/archs4_gsm_ids.txt` (9.4MB) - Sample ID mappings
