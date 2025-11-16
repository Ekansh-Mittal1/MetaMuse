import argparse
import asyncio
import csv
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from agents import Agent, ModelProvider, Model, OpenAIChatCompletionsModel, Runner, RunConfig, ModelSettings
from dotenv import load_dotenv
from openai import AsyncOpenAI
from openai.types.shared import Reasoning

from .normalization_models import NormalizationFieldEvaluation, SampleNormalizationEvaluation
from .normalization_prompts import NORMALIZATION_SYSTEM_PROMPT, build_normalization_prompt
# Note: renderer is for curation, we'll implement our own simple functions


CURATED_FIELD_SUFFIX = "_final_candidate"
NORMALIZED_TERM_SUFFIX = "_normalized_term"
NORMALIZED_ID_SUFFIX = "_normalized_id"

# Fields that have normalization
NORMALIZED_FIELDS = ["disease", "tissue", "organ"]


def extract_normalized_fields_from_header(header: List[str]) -> List[str]:
    """Extract only fields that have normalization columns."""
    fields: set[str] = set()
    for col in header:
        if col.endswith(NORMALIZED_TERM_SUFFIX) or col.endswith(NORMALIZED_ID_SUFFIX):
            field = col.replace(NORMALIZED_TERM_SUFFIX, "").replace(NORMALIZED_ID_SUFFIX, "")
            if field in NORMALIZED_FIELDS:
                fields.add(field)
    return sorted(fields)


def build_normalization_dicts(row: Dict[str, str], normalized_fields: List[str]) -> tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
    """Build curated and normalized value dictionaries for normalized fields only."""
    curated: Dict[str, str] = {}
    normalized: Dict[str, Dict[str, str]] = {}
    for field in normalized_fields:
        curated[field] = row.get(f"{field}{CURATED_FIELD_SUFFIX}") or ""
        normalized[field] = {
            "term": row.get(f"{field}{NORMALIZED_TERM_SUFFIX}") or "",
            "id": row.get(f"{field}{NORMALIZED_ID_SUFFIX}") or "",
        }
    return curated, normalized


class NormalizationEvalModelProvider(ModelProvider):
    """OpenRouter-backed model provider for normalization evaluation."""

    def __init__(self, default_model: str):
        self.default_model = default_model

    def get_model(self, model_name: str | None) -> Model:
        model = model_name or self.default_model
        base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise ValueError("OPENROUTER_API_KEY environment variable is required.")
        client = AsyncOpenAI(
            base_url=base_url,
            api_key=api_key,
            default_headers={
                "HTTP-Referer": "localhost",
                "X-Title": "MetaMuse Normalization Evaluation",
                "X-App-Name": "MetaMuse",
            },
        )
        return OpenAIChatCompletionsModel(model=model, openai_client=client)


async def process_single_sample_normalization(
    row: Dict[str, str],
    normalized_fields: List[str],
    output_dir: str,
    model_name: str,
    provider_order: List[str],
    max_retries: int,
    retry_backoff_seconds: float,
    semaphore: asyncio.Semaphore,
) -> Optional[SampleNormalizationEvaluation]:
    """Process normalization evaluation for a single sample."""
    async with semaphore:
        sample_id = row.get("sample_id") or ""
        series_id = row.get("series_id") or ""
        sample_type = row.get("sample_type") or ""

        logger = logging.getLogger("norm_evaluator")
        
        try:
            print("Processing normalization for sample %s", sample_id)
            
            curated_dict, normalized_dict = build_normalization_dicts(row, normalized_fields)

            # Skip if no normalized fields have data
            has_normalization_data = any(
                normalized_dict.get(field, {}).get("term") or normalized_dict.get(field, {}).get("id")
                for field in normalized_fields
            )
            if not has_normalization_data:
                logger.debug("No normalization data for %s, skipping", sample_id)
                return None

            user_prompt = build_normalization_prompt(
                sample_id=sample_id,
                series_id=series_id,
                sample_type=sample_type,
                normalized_fields=normalized_fields,
                curated_values=curated_dict,
                normalized_values=normalized_dict,
            )

            # Create agent with structured output
            agent = Agent(
                name="NormalizationEvaluationAgent",
                instructions=NORMALIZATION_SYSTEM_PROMPT,
                tools=[],
                handoffs=[],
                output_type=SampleNormalizationEvaluation,
            )

            extra_body: Dict[str, Any] = {}
            if provider_order:
                extra_body = {"provider": {"order": provider_order}}

            run_config = RunConfig(
                model_provider=NormalizationEvalModelProvider(model_name),
                model_settings=ModelSettings(
                    max_tokens=None,
                    reasoning=Reasoning(effort="high"),
                    extra_body=extra_body,
                ),
            )

            # Retry logic at sample level
            attempt = 0
            response = None
            last_exception: Optional[Exception] = None
            while attempt < max(1, max_retries):
                attempt += 1
                try:
                    result = await Runner.run(agent, user_prompt, run_config=run_config, max_turns=1)
                    response = result.final_output
                    if not isinstance(response, SampleNormalizationEvaluation):
                        raise RuntimeError(f"Expected SampleNormalizationEvaluation, got {type(response)}")
                    break
                except Exception as e:
                    last_exception = e
                    print("Failed normalization eval for %s (attempt %d): %s", sample_id, attempt, e)
                    if attempt < max_retries:
                        await asyncio.sleep(retry_backoff_seconds * attempt)

            if response is None:
                # Persist error and return None
                import traceback as _tb
                err_path = Path(output_dir) / f"{sample_id}_normalization_error.json"
                err_payload = {
                    "sample_id": sample_id,
                    "series_id": series_id,
                    "error": str(last_exception) if last_exception else "Unknown error",
                    "traceback": _tb.format_exc(),
                }
                err_path.write_text(json.dumps(err_payload, indent=2), encoding="utf-8")
                return None

            # Fill identifiers in case model omitted them
            response.sample_id = response.sample_id or sample_id
            response.series_id = response.series_id or series_id
            response.sample_type = response.sample_type or sample_type

            # Backfill missing fields
            existing_fields = {fe.field_name for fe in response.normalized_fields}
            for field in normalized_fields:
                if field not in existing_fields:
                    response.normalized_fields.append(
                        NormalizationFieldEvaluation(
                            field_name=field,
                            curated_value=curated_dict.get(field) or None,
                            normalized_term=normalized_dict.get(field, {}).get("term") or None,
                            normalized_id=normalized_dict.get(field, {}).get("id") or None,
                            is_normalization_correct=None,
                            normalization_reason="No judgment returned",
                        )
                    )

            # Compute per-sample summary
            correct_marks = [1 for fe in response.normalized_fields if fe.is_normalization_correct]
            total_marks = [1 for fe in response.normalized_fields if fe.is_normalization_correct is not None]
            response.overall_normalization_accuracy = (sum(correct_marks) / len(total_marks)) if total_marks else None

            # Write per-sample JSON
            out_path = Path(output_dir) / f"{sample_id}_normalization_evaluation.json"
            out_path.write_text(response.model_dump_json(indent=2), encoding="utf-8")

            return response

        except Exception as outer_err:
            import traceback as _tb
            err_path = Path(output_dir) / f"{sample_id}_normalization_error.json"
            err_payload = {
                "sample_id": sample_id,
                "series_id": series_id,
                "error": str(outer_err),
                "traceback": _tb.format_exc(),
            }
            err_path.write_text(json.dumps(err_payload, indent=2), encoding="utf-8")
            print("Unhandled exception for normalization eval %s; recorded in %s", sample_id, err_path)
            return None


def generate_normalization_errors_report(sample_evaluations: List[SampleNormalizationEvaluation], output_dir: str) -> None:
    """Generate unified normalization errors report."""
    false_results = []
    
    for sample_eval in sample_evaluations:
        sample_id = sample_eval.sample_id
        series_id = sample_eval.series_id or ""
        
        for field_eval in sample_eval.normalized_fields:
            if field_eval.is_normalization_correct is False:
                false_results.append({
                    "sample_id": sample_id,
                    "series_id": series_id,
                    "field_name": field_eval.field_name,
                    "curated_value": field_eval.curated_value or "",
                    "normalized_term": field_eval.normalized_term or "",
                    "normalized_id": field_eval.normalized_id or "",
                    "reason": field_eval.normalization_reason or "",
                })
    
    if not false_results:
        print("✅ No false normalization results found!")
        return
    
    # Create DataFrame for easier analysis
    import pandas as pd
    df = pd.DataFrame(false_results)
    
    # Generate summary statistics
    summary = {
        "total_false_results": len(false_results),
        "samples_with_errors": df["sample_id"].nunique(),
        "fields_with_errors": df["field_name"].value_counts().to_dict(),
    }
    
    # Create detailed report
    report = {
        "summary": summary,
        "detailed_errors": false_results,
        "recommendations": [
            "Review normalization mappings for semantic accuracy",
            "Verify ontology IDs match the normalized terms",
            "Check for missing normalizations where expected",
        ]
    }
    
    # Save report
    errors_path = Path(output_dir) / "normalization_errors_report.json"
    with errors_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    
    print(f"📊 Generated normalization errors report: {errors_path}")
    print(f"❌ Total false normalization results: {len(false_results)}")
    print(f"📝 Samples with normalization errors: {df['sample_id'].nunique()}")
    print(f"🔍 Fields with most normalization errors: {dict(df['field_name'].value_counts().head())}")


def compute_normalization_accuracy(samples: List[SampleNormalizationEvaluation]) -> Dict[str, float]:
    """Compute per-field normalization accuracy."""
    from collections import defaultdict
    
    counts: Dict[str, List[int]] = defaultdict(lambda: [0, 0])  # [correct, total]
    
    for sample in samples:
        for field_eval in sample.normalized_fields:
            field = field_eval.field_name
            if field_eval.is_normalization_correct is not None:
                counts[field][1] += 1
                if field_eval.is_normalization_correct:
                    counts[field][0] += 1
    
    return {
        f: (num_correct / total if total > 0 else 0.0) 
        for f, (num_correct, total) in counts.items()
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate batch normalization results using Gemini 2.5 Pro")
    parser.add_argument("batch_dir", type=str, help="Path to batch directory containing batch_results.csv")
    parser.add_argument(
        "--model-name",
        type=str,
        default="google/gemini-2.5-pro",
        help="OpenRouter model name to use (e.g., google/gemini-2.5-pro)",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Max retries per sample for structured parsing",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=2.0,
        help="Base backoff (seconds) between retries",
    )
    parser.add_argument(
        "--provider-order",
        type=str,
        default="google-vertex/us",
        help="Comma-separated provider order for OpenRouter (e.g., google-vertex/us)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=5,
        help="Maximum number of concurrent workers for parallel processing",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to write evaluation outputs (JSON per sample, summary, and chart)",
    )
    args = parser.parse_args()

    # Load environment variables
    load_dotenv(override=True)

    # Reduce noisy HTTP logs
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("norm_evaluator")

    batch_dir = args.batch_dir.rstrip("/")
    output_dir = args.output_dir or str(Path(batch_dir) / "normalization_evaluation")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    csv_path = Path(batch_dir) / "batch_results.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"batch_results.csv not found at {csv_path}")

    # Read all rows first
    rows = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        normalized_fields = extract_normalized_fields_from_header(header)
        print("Discovered normalized fields: %s", ", ".join(normalized_fields))
        rows = list(reader)

    if not normalized_fields:
        print("No normalized fields found in batch_results.csv")
        return

    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(args.max_workers)
    provider_order = [p.strip() for p in (args.provider_order or "").split(",") if p.strip()]
    print("Processing %d samples with max %d workers for normalization evaluation", len(rows), args.max_workers)

    # Process all samples in parallel
    tasks = [
        process_single_sample_normalization(
            row, normalized_fields, output_dir, 
            args.model_name, provider_order, args.max_retries, 
            args.retry_backoff_seconds, semaphore
        )
        for row in rows
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Collect successful evaluations
    sample_evaluations: List[SampleNormalizationEvaluation] = []
    for result in results:
        if isinstance(result, SampleNormalizationEvaluation):
            sample_evaluations.append(result)
        elif isinstance(result, Exception):
            print("Normalization task failed with exception: %s", result)

    # Compute accuracy and render chart
    norm_acc = compute_normalization_accuracy(sample_evaluations)
    chart_path = str(Path(output_dir) / "normalization_accuracy_barchart.png")
    
    # Create a simple bar chart for normalization accuracy
    import matplotlib.pyplot as plt
    
    fields = sorted(norm_acc.keys())
    accuracies = [norm_acc[f] for f in fields]
    
    fig, ax = plt.subplots(figsize=(max(8, len(fields) * 0.8), 5))
    ax.bar(fields, accuracies)
    ax.set_ylabel("Normalization Accuracy")
    ax.set_ylim(0, 1)
    ax.set_title("Per-field Normalization Accuracy")
    ax.set_xticklabels(fields, rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig(chart_path)
    plt.close(fig)
    
    print("Saved normalization accuracy chart to %s", chart_path)

    # Write summary JSON
    summary = {
        "batch_dir": batch_dir,
        "num_samples": len(sample_evaluations),
        "per_field_normalization_accuracy": norm_acc,
    }
    Path(output_dir, "normalization_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print("Saved normalization_summary.json with %d samples", len(sample_evaluations))

    # Generate errors report automatically
    generate_normalization_errors_report(sample_evaluations, output_dir)


if __name__ == "__main__":
    asyncio.run(main())
