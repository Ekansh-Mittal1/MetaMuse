# BixBench Benchmarking ‑ Quick Guide

This document describes **only what you need to run and inspect a BixBench evaluation with DendroForge**.  All implementation notes, background information and troubleshooting tips were removed for brevity.

---

## 1. Prerequisites

1. A working Python ≥ 3.11 environment managed with **uv**.
2. Environment variables in a `.env` file:
   ```bash
   OPENROUTER_API_KEY=<your-key>
   OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
   ```
3. `huggingface-cli login` (only if the dataset requires authentication).

---

## 2. Configuration

Benchmark options live in YAML files inside `benchmarking/` – e.g.:

* `config_rnaseq_sonnet.yaml` – runs RNA-seq capsules with Anthropic Claude Sonnet-4
* `config_rnaseq_gemini.yaml` – same capsules with Google Gemini 2.5 Pro

Key fields:

| field | description |
|-------|-------------|
| `single_test_id`   | Run one capsule (e.g. `bix-1`). Overrides other filters. |
| `category_filter`  | List of categories to include. |
| `limit`            | Max number of capsules. |
| `concurrency`      | Parallel DendroForge processes. |
| `model_name`       | `anthropic/claude-sonnet-4` or `google/gemini-2.5-pro`. |
| `data_dir`, `log_dir` | Where to store data and logs. |

---

## 3. Running the Benchmark

```bash
# Default configuration (config_rnaseq_sonnet.yaml)
uv run benchmarking/bixbench.py benchmarking/config_rnaseq_sonnet.yaml

# Run with a different config
uv run benchmarking/bixbench.py benchmarking/config_rnaseq_gemini.yaml
```

Common flags (override YAML values on the fly):

```bash
--limit 5                # first five capsules only
--concurrency 4          # four parallel runs
--data-dir my_data       # custom data directory
--log-dir  my_logs       # custom log directory
```

---

## 4. Output

Each run creates a timestamped folder in `log_dir`:

```
log_dir/
└── run_YYYYMMDD_HHMMSS/
    ├── benchmark_run.log       # execution log
    ├── benchmark_summary.json  # high-level results
    └── bix-*/                  # one sub-folder per capsule
        ├── prompt.txt
        ├── full_log.json
        └── dendroforge_output.log
```

Open the JSON or log files to inspect predictions and scores.

---

## 5. Quick Checks

```bash
# Show how many capsules finished successfully
jq '.completed' log_dir/run_*/benchmark_summary.json

# View accuracy for a single capsule
cat log_dir/run_*/bix-1/score_summary.txt
```

That’s all you need to execute and analyze a BixBench evaluation with DendroForge. 😊 