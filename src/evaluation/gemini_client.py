import json
import os
import time
from typing import Any, Dict, Optional, Type, TypeVar

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, ValidationError


T = TypeVar("T", bound=BaseModel)


class GeminiClient:
    """Client wrapper using OpenRouter (OpenAI-compatible) for Gemini models.

    Mirrors environment setup used in batch_samples_efficient and main.py:
    - load_dotenv()
    - Use OPENROUTER_API_KEY and OPENROUTER_BASE_URL (default https://openrouter.ai/api/v1)
    - Default model names like "google/gemini-2.5-pro".
    """

    def __init__(self, model_name: str = "google/gemini-2.5-pro") -> None:
        load_dotenv(override=True)

        self.model_name = model_name
        base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY environment variable is required.")

        self._client = OpenAI(
            base_url=base_url,
            api_key=api_key,
            default_headers={
                "HTTP-Referer": "localhost",
                "X-Title": "MetaMuse Evaluation",
                "X-App-Name": "MetaMuse",
            },
        )

    def generate_structured_json(
        self,
        system_prompt: str,
        user_prompt: str,
        output_model: Type[T],
        temperature: float = 0.0,
        max_retries: int = 3,
        retry_backoff_seconds: float = 2.0,
        provider_order: Optional[list[str]] = None,
    ) -> T:
        """Call the model via OpenRouter and validate the JSON into output_model."""

        last_error: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                extra_body: Dict[str, Any] = {}
                if provider_order:
                    extra_body = {"provider": {"order": provider_order}}

                resp = self._client.chat.completions.create(
                    model=self.model_name,
                    temperature=temperature,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    response_format={"type": "json_object"},
                    extra_body=extra_body or None,
                )

                content = resp.choices[0].message.content if resp.choices else "{}"
                data = json.loads(content)
                return output_model.model_validate(data)
            except (json.JSONDecodeError, ValidationError) as schema_error:
                last_error = schema_error
            except Exception as api_error:  # pragma: no cover
                last_error = api_error

            if attempt < max_retries:
                time.sleep(retry_backoff_seconds * attempt)

        assert last_error is not None
        raise RuntimeError(f"OpenRouter structured call failed after retries: {last_error}")

    def generate_json_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.0,
        provider_order: Optional[list[str]] = None,
    ) -> str:
        """Call the model and return the raw JSON text (no validation)."""
        extra_body: Dict[str, Any] = {}
        if provider_order:
            extra_body = {"provider": {"order": provider_order}}

        resp = self._client.chat.completions.create(
            model=self.model_name,
            temperature=temperature,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            extra_body=extra_body or None,
        )

        return resp.choices[0].message.content if resp.choices else "{}"


