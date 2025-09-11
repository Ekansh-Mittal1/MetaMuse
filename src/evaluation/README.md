# Evaluation Tools

This package evaluates batch outputs against raw evidence using Gemini 2.5 Pro via OpenRouter and produces structured Pydantic JSON outputs plus an accuracy bar chart.

## Setup

- Python dependencies:
  - `openai` (for OpenRouter-compatible client)
  - `python-dotenv`
  - `matplotlib`
  - `pydantic`

Install (example):

```bash
pip install openai python-dotenv matplotlib pydantic
```

- Environment:
  - Export `OPENROUTER_API_KEY` (required)
  - Optional: `OPENROUTER_BASE_URL` (defaults to https://openrouter.ai/api/v1)

```bash
export OPENROUTER_API_KEY=your_openrouter_api_key
export OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
```

## Usage

```bash
python -m src.evaluation.evaluate_batch /absolute/path/to/batch_dir \
  --model-name google/gemini-2.5-pro \
  --output-dir /absolute/path/to/output_dir
```

- The tool expects a `batch_results.csv` in the `batch_dir`.
- It will read raw context from `{batch_dir}/data_intake/{GSE...}` including series/sample metadata and any `PMID_*_metadata.json` for abstracts.
- Per-sample structured outputs are saved as `{output_dir}/{GSM}_evaluation.json`.
- A `summary.json` and `accuracy_barchart.png` will be written to `output_dir`.

## Output Schema

- Per-sample outputs match `SampleEvaluation` in `models.py`.
- Each field uses `FieldEvaluation` with judgments for curation and normalization, including brief reasons.

## Notes

- If insufficient evidence is available, the evaluator may mark fields as incorrect and will include an explanation.
- The chart shows per-field accuracy for both curation and normalization across all evaluated samples.
