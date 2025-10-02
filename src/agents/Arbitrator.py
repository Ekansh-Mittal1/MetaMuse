"""
Arbitrator agent for per-sample holistic curation evaluation and suggestions.

Evaluates all fields for a given sample together and returns SampleEvaluation
objects (reusing evaluation models) with suggested_curation for incorrect fields.
"""

from __future__ import annotations

import json
from typing import Optional, Dict, Any, List

from agents import Agent, RunConfig, ModelSettings, Runner
from src.utils.prompts import load_prompt
from src.evaluation.curation_models import SampleEvaluation


def build_user_payload(
    sample_id: str,
    series_id: str,
    sample_type: str,
    abstract_text: Optional[str],
    series_metadata_json: str,
    sample_metadata_json: str,
    curated_values_json: str,
) -> str:
    prompt = f"""
Evaluate curated values for the following sample fields. Provide correctness judgment and suggested corrections when needed.

Identifiers:
- Sample ID: {sample_id}
- Series ID: {series_id}
- Sample type: {sample_type}

Raw evidence:
- Abstract:
{abstract_text or '[No abstract available]'}

- Series metadata (JSON):
{series_metadata_json}

- Sample metadata (JSON):
{sample_metadata_json}

Curated values (JSON):
{curated_values_json}
"""
    return prompt


def create_arbitrator_agent(model_name: str) -> Agent:
    system_prompt = load_prompt("arbitrator_agent.md")
    return Agent(
        name="ArbitratorAgent",
        instructions=system_prompt,
        tools=[],
        handoffs=[],
        output_type=SampleEvaluation,
    )


async def run_arbitration_for_sample(
    *,
    model_name: str,
    model_provider,
    sample_id: str,
    series_id: str,
    sample_type: str,
    abstract_text: Optional[str],
    series_metadata: Dict[str, Any] | None,
    sample_metadata: Dict[str, Any] | None,
    curated_values: Dict[str, str],
    provider_order: Optional[List[str]] = None,
    max_retries: int = 2,
    retry_backoff_seconds: float = 2.0,
) -> SampleEvaluation:
    agent = create_arbitrator_agent(model_name)

    user_payload = build_user_payload(
        sample_id=sample_id,
        series_id=series_id,
        sample_type=sample_type,
        abstract_text=abstract_text,
        series_metadata_json=json.dumps(series_metadata or {}, indent=2),
        sample_metadata_json=json.dumps(sample_metadata or {}, indent=2),
        curated_values_json=json.dumps(curated_values, indent=2),
    )

    extra_body: Dict[str, Any] = {}
    if provider_order:
        extra_body = {"provider": {"order": provider_order}}

    # Maintain local for debugging; not used further
    last_err: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            # Force arbitrator to use Gemini 2.5 Pro by setting provider order
            order = ["google/gemini-2.5-pro"]
            if provider_order:
                order = provider_order
            extra_body = {"provider": {"order": order}}

            run_config = RunConfig(
                model_provider=model_provider,
                model_settings=ModelSettings(
                    extra_body=extra_body,
                ),
            )

            result = Runner.run_streamed(agent, user_payload, run_config=run_config, max_turns=50)

            # Stream until we get final output
            final_result = None
            try:
                async for event in result.stream_events():
                    if event.type == "agent_response_event":
                        final_result = event.result
                        break
            except Exception:
                pass

            if final_result is None:
                try:
                    final_result = result.final_output
                except Exception:
                    raise RuntimeError("No result received from Arbitrator agent")

            if isinstance(final_result, SampleEvaluation):
                final_result.sample_id = final_result.sample_id or sample_id
                final_result.series_id = final_result.series_id or series_id
                final_result.sample_type = final_result.sample_type or sample_type
                return final_result
            raise RuntimeError(f"Arbitrator returned unexpected type: {type(final_result)}")
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                import asyncio
                await asyncio.sleep(retry_backoff_seconds * (attempt + 1))
            else:
                raise


