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

from .loader import load_raw_context
from .curation_models import FieldEvaluation, SampleEvaluation
from .curation_prompts import SYSTEM_PROMPT, build_user_prompt
from .renderer import compute_accuracy, render_accuracy_barchart


def generate_errors_report(sample_evaluations: List[SampleEvaluation], output_dir: str) -> None:
    """Generate unified errors report from evaluation results."""
    false_results = []
    
    for sample_eval in sample_evaluations:
        sample_id = sample_eval.sample_id
        series_id = sample_eval.series_id or ""
        
        for field_eval in sample_eval.fields:
            if field_eval.is_curated_correct is False:
                false_results.append({
                    "sample_id": sample_id,
                    "series_id": series_id,
                    "field_name": field_eval.field_name,
                    "curated_value": field_eval.curated_value or "",
                    "reason": field_eval.curated_reason or "",
                })
    
    if not false_results:
        print("✅ No false curation results found!")
        return
    
    # Create DataFrame for easier analysis
    import pandas as pd
    df = pd.DataFrame(false_results)
    
    # Generate summary statistics
    summary = {
        "total_false_results": len(false_results),
        "samples_with_errors": df["sample_id"].nunique(),
        "fields_with_errors": df["field_name"].value_counts().to_dict(),
        "common_error_patterns": {},
    }
    
    # Analyze common error patterns
    reason_patterns = {}
    for reason in df["reason"]:
        if "empty string" in reason.lower():
            reason_patterns["empty_string_instead_of_none_reported"] = reason_patterns.get("empty_string_instead_of_none_reported", 0) + 1
        elif "incorrect" in reason.lower() and "generic" in reason.lower():
            reason_patterns["too_generic_curation"] = reason_patterns.get("too_generic_curation", 0) + 1
        elif "missing" in reason.lower() or "should be" in reason.lower():
            reason_patterns["missing_expected_value"] = reason_patterns.get("missing_expected_value", 0) + 1
        else:
            reason_patterns["other"] = reason_patterns.get("other", 0) + 1
    
    summary["common_error_patterns"] = reason_patterns
    
    # Create detailed report
    report = {
        "summary": summary,
        "detailed_errors": false_results,
        "recommendations": [
            "Review empty string cases - most should be 'None reported'",
            "Check if curation guidelines are being followed consistently",
            "Consider updating extraction templates for frequently problematic fields",
        ]
    }
    
    # Save report
    errors_path = Path(output_dir) / "errors_report.json"
    with errors_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    
    print(f"📊 Generated errors report: {errors_path}")
    print(f"❌ Total false results: {len(false_results)}")
    print(f"📝 Samples with errors: {df['sample_id'].nunique()}")
    print(f"🔍 Fields with most errors: {dict(df['field_name'].value_counts().head())}")


CURATED_FIELD_SUFFIX = "_final_candidate"
NORMALIZED_TERM_SUFFIX = "_normalized_term"
NORMALIZED_ID_SUFFIX = "_normalized_id"


def extract_fields_from_header(header: List[str]) -> List[str]:
    fields: set[str] = set()
    for col in header:
        if col.endswith(CURATED_FIELD_SUFFIX):
            fields.add(col[: -len(CURATED_FIELD_SUFFIX)])
        elif col.endswith(NORMALIZED_TERM_SUFFIX):
            fields.add(col[: -len(NORMALIZED_TERM_SUFFIX)])
        elif col.endswith(NORMALIZED_ID_SUFFIX):
            fields.add(col[: -len(NORMALIZED_ID_SUFFIX)])
    # Exclude known non-target identifiers
    fields.discard("sandbox")
    return sorted(fields)


def build_curated_and_normalized_dicts(row: Dict[str, str], target_fields: List[str]) -> tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
    curated: Dict[str, str] = {}
    normalized: Dict[str, Dict[str, str]] = {}
    for field in target_fields:
        curated[field] = row.get(f"{field}{CURATED_FIELD_SUFFIX}") or ""
        normalized[field] = {
            "term": row.get(f"{field}{NORMALIZED_TERM_SUFFIX}") or "",
            "id": row.get(f"{field}{NORMALIZED_ID_SUFFIX}") or "",
        }
    return curated, normalized


class EvalModelProvider(ModelProvider):
    """Minimal OpenRouter-backed model provider for Agents SDK."""

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
                "X-Title": "MetaMuse Evaluation",
                "X-App-Name": "MetaMuse",
            },
        )
        return OpenAIChatCompletionsModel(model=model, openai_client=client)


async def process_single_sample(
    row: Dict[str, str],
    target_fields: List[str],
    batch_dir: str,
    output_dir: str,
    model_name: str,
    provider_order: List[str],
    max_retries: int,
    retry_backoff_seconds: float,
    semaphore: asyncio.Semaphore,
) -> Optional[SampleEvaluation]:
    """Process a single sample with concurrency control."""
    async with semaphore:
        sample_id = row.get("sample_id") or ""
        series_id = row.get("series_id") or ""
        sample_type = row.get("sample_type") or ""
        pubmed_id = row.get("pubmed_id") or ""

        logger = logging.getLogger("evaluator")
        
        try:
            print("Processing sample %s", sample_id)
            abstract_text, series_meta, sample_meta = load_raw_context(batch_dir, series_id, sample_id)

            curated_dict, normalized_dict = build_curated_and_normalized_dicts(row, target_fields)

            user_prompt = build_user_prompt(
                sample_id=sample_id,
                series_id=series_id,
                target_fields=target_fields,
                abstract_text=abstract_text,
                series_metadata_json=json.dumps(series_meta or {}, indent=2),
                sample_metadata_json=json.dumps(sample_meta or {}, indent=2),
                curated_values_json=json.dumps(curated_dict, indent=2),
                normalized_values_json=json.dumps(normalized_dict, indent=2),
                sample_type=sample_type,
            )

            # Create agent with structured output
            agent = Agent(
                name="EvaluationAgent",
                instructions=SYSTEM_PROMPT,
                tools=[],
                handoffs=[],
                output_type=SampleEvaluation,
            )

            extra_body: Dict[str, Any] = {}
            if provider_order:
                extra_body = {"provider": {"order": provider_order}}

            run_config = RunConfig(
                model_provider=EvalModelProvider(model_name),
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
                    if not isinstance(response, SampleEvaluation):
                        raise RuntimeError(f"Expected SampleEvaluation, got {type(response)}")
                    break
                except Exception as e:
                    last_exception = e
                    print("Failed sample %s (attempt %d): %s", sample_id, attempt, e)
                    if attempt < max_retries:
                        await asyncio.sleep(retry_backoff_seconds * attempt)

            if response is None:
                # Persist error and return None
                import traceback as _tb
                err_path = Path(output_dir) / f"{sample_id}_evaluation_error.json"
                err_payload = {
                    "sample_id": sample_id,
                    "series_id": series_id,
                    "error": str(last_exception) if last_exception else "Unknown error",
                    "traceback": _tb.format_exc(),
                }
                err_path.write_text(json.dumps(err_payload, indent=2), encoding="utf-8")
                return None

            # Fill identifiers and raw context in case model omitted them
            response.sample_id = response.sample_id or sample_id
            response.series_id = response.series_id or series_id
            response.sample_type = response.sample_type or sample_type
            response.pubmed_id = response.pubmed_id or pubmed_id
            response.abstract_text = response.abstract_text or abstract_text
            
            # Convert metadata dicts to strict key/value lists for strict schemas
            if response.series_metadata is None and isinstance(series_meta, dict):
                response.series_metadata = [
                    {"key": str(k), "value": str(v)} for k, v in series_meta.items()
                ]
            if response.sample_metadata is None and isinstance(sample_meta, dict):
                response.sample_metadata = [
                    {"key": str(k), "value": str(v)} for k, v in sample_meta.items()
                ]

            # If the response missed some fields, backfill placeholders to keep accounting consistent
            existing_fields = {fe.field_name for fe in response.fields}
            for field in target_fields:
                if field not in existing_fields:
                    response.fields.append(
                        FieldEvaluation(
                            field_name=field,
                            curated_value=curated_dict.get(field) or None,
                            normalized_term=normalized_dict.get(field, {}).get("term") or None,
                            normalized_id=normalized_dict.get(field, {}).get("id") or None,
                            is_curated_correct=None,
                            curated_reason="No judgment returned",
                            is_normalized_correct=None,
                            normalized_reason="No judgment returned",
                        )
                    )

            # Compute simple per-sample summaries
            curated_marks = [1 for fe in response.fields if fe.is_curated_correct]
            curated_total = [1 for fe in response.fields if fe.is_curated_correct is not None]
            response.overall_curated_accuracy = (sum(curated_marks) / len(curated_total)) if curated_total else None

            norm_marks = [1 for fe in response.fields if fe.is_normalized_correct]
            norm_total = [1 for fe in response.fields if fe.is_normalized_correct is not None]
            response.overall_normalized_accuracy = (sum(norm_marks) / len(norm_total)) if norm_total else None

            # Write per-sample JSON
            out_path = Path(output_dir) / f"{sample_id}_evaluation.json"
            out_path.write_text(response.model_dump_json(indent=2), encoding="utf-8")

            return response

        except Exception as outer_err:
            import traceback as _tb
            err_path = Path(output_dir) / f"{sample_id}_evaluation_error.json"
            err_payload = {
                "sample_id": sample_id,
                "series_id": series_id,
                "error": str(outer_err),
                "traceback": _tb.format_exc(),
            }
            err_path.write_text(json.dumps(err_payload, indent=2), encoding="utf-8")
            print("Unhandled exception for %s; recorded in %s", sample_id, err_path)
            return None


async def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate batch results using Gemini 2.5 Pro")
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

    # Load environment variables like other workflows
    load_dotenv(override=True)

    # Reduce noisy HTTP logs
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("evaluator")

    batch_dir = args.batch_dir.rstrip("/")
    output_dir = args.output_dir or str(Path(batch_dir) / "evaluation")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    csv_path = Path(batch_dir) / "batch_results.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"batch_results.csv not found at {csv_path}")

    # Read all rows first
    rows = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        target_fields = extract_fields_from_header(header)
        print("Discovered target fields: %s", ", ".join(target_fields))
        rows = list(reader)

    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(args.max_workers)
    provider_order = [p.strip() for p in (args.provider_order or "").split(",") if p.strip()]
    print("Processing %d samples with max %d workers", len(rows), args.max_workers)

    # Process all samples in parallel
    tasks = [
        process_single_sample(
            row, target_fields, batch_dir, output_dir, 
            args.model_name, provider_order, args.max_retries, 
            args.retry_backoff_seconds, semaphore
        )
        for row in rows
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Collect successful evaluations
    sample_evaluations: List[SampleEvaluation] = []
    for result in results:
        if isinstance(result, SampleEvaluation):
            sample_evaluations.append(result)
        elif isinstance(result, Exception):
            print("Task failed with exception: %s", result)

    # Aggregate and render chart
    curated_acc, norm_acc = compute_accuracy(sample_evaluations)
    chart_path = str(Path(output_dir) / "accuracy_barchart.png")
    render_accuracy_barchart(curated_acc, norm_acc, chart_path)
    print("Saved accuracy chart to %s", chart_path)

    # Write summary JSON
    summary = {
        "batch_dir": batch_dir,
        "num_samples": len(sample_evaluations),
        "per_field_curated_accuracy": curated_acc,
        "per_field_normalized_accuracy": norm_acc,
    }
    Path(output_dir, "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print("Saved summary.json with %d samples", len(sample_evaluations))

    # Generate errors report automatically
    generate_errors_report(sample_evaluations, output_dir)


if __name__ == "__main__":
    asyncio.run(main())