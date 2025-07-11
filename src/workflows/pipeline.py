from agents import handoff, RunContextWrapper
from pydantic import BaseModel

from openai import AsyncOpenAI
from typing import Union
import os
from dotenv import load_dotenv
load_dotenv()

# Read environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
NCBI_API_KEY = os.getenv("NCBI_API_KEY") 
NCBI_EMAIL = os.getenv("NCBI_EMAIL")
NCBI_API_URL = os.getenv("NCBI_API_URL", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/")

# Validate required environment variables
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is required")

if not NCBI_EMAIL:
    raise ValueError("NCBI_EMAIL environment variable is required")

BASE_URL = "https://openrouter.ai/api/v1"
API_KEY = os.getenv("OPENROUTER_API_KEY")
MODEL_NAME = "openai/gpt-4o"

client = AsyncOpenAI(
    base_url=BASE_URL, 
    api_key=API_KEY,
    default_headers={
        "HTTP-Referer": "localhost",
        "X-Title": "MetaMuse Test"
    }
)
set_tracing_disabled(disabled=True)


class CustomModelProvider(ModelProvider):
    def get_model(self, model_name: Union[str, None]) -> Model:
        return OpenAIChatCompletionsModel(model=model_name or MODEL_NAME, openai_client=client)


CUSTOM_MODEL_PROVIDER = CustomModelProvider()

