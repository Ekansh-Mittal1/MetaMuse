from dotenv import load_dotenv
load_dotenv()

import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from agents import handoff, RunContextWrapper
from pydantic import BaseModel

from openai import AsyncOpenAI
from typing import Union

# Configure OpenRouter for the agents library
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
if OPENROUTER_API_KEY:
    os.environ["OPENAI_API_KEY"] = OPENROUTER_API_KEY
    os.environ["OPENAI_BASE_URL"] = "https://openrouter.ai/api/v1"
    os.environ["OPENAI_DEFAULT_HEADERS"] = '{"HTTP-Referer": "localhost", "X-Title": "MetaMuse Test"}'
    # Additional environment variables that might be needed
    os.environ["OPENAI_API_TYPE"] = "open_ai"
    os.environ["OPENAI_API_VERSION"] = "2024-01-01"

# Read environment variables
NCBI_API_KEY = os.getenv("NCBI_API_KEY") 
NCBI_EMAIL = os.getenv("NCBI_EMAIL")
NCBI_API_URL = os.getenv("NCBI_API_URL", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/")

# Validate required environment variables
if not OPENROUTER_API_KEY:
    raise ValueError("OPENROUTER_API_KEY environment variable is required")

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

# Import the ingestion agent
from src.agents.ingestion import create_ingestion_agent
import asyncio
import uuid


def create_agent():
    """
    Create and return an ingestion agent instance.
    
    Returns:
        Agent: Configured ingestion agent ready for use
    """
    session_id = str(uuid.uuid4())
    return create_ingestion_agent(session_id)


async def chat_with_agent(agent, message: str):
    """
    Send a message to the agent and get a response.
    
    Args:
        agent: The agent instance to chat with
        message (str): The message to send to the agent
        
    Returns:
        str: The agent's response
    """
    try:
        response = await agent.run(message)
        return response
    except Exception as e:
        return f"Error communicating with agent: {str(e)}"


async def interactive_chat():
    """
    Start an interactive chat session with the ingestion agent.
    """
    print("🤖 Initializing MetaMuse Ingestion Agent...")
    print("=" * 50)
    
    # Create the agent
    agent = create_agent()
    
    print("✅ Agent created successfully!")
    print("📋 Available tools:")
    print("   - extract_gsm_metadata: Extract sample-level metadata")
    print("   - extract_gse_metadata: Extract series-level metadata")
    print("   - extract_series_matrix_metadata: Extract matrix metadata and sample names")
    print("   - extract_paper_abstract: Extract paper abstracts and metadata")
    print("   - validate_geo_inputs: Validate input parameters")
    print()
    print("💡 Example queries:")
    print("   - 'Extract metadata for GSM1019742'")
    print("   - 'Get series metadata for GSE41588'")
    print("   - 'Extract paper abstract for PMID 23902433'")
    print("   - 'Get series matrix metadata for GSE41588'")
    print("   - 'Validate inputs: GSM1019742, GSE41588, PMID 23902433'")
    print()
    print("Type 'quit' or 'exit' to end the session")
    print("=" * 50)
    
    while True:
        try:
            # Get user input
            user_input = input("\n👤 You: ").strip()
            
            # Check for exit commands
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye! Ending session...")
                break
            
            if not user_input:
                continue
            
            print("🤖 Agent: Thinking...")
            
            # Get agent response
            response = await chat_with_agent(agent, user_input)
            
            print(f"🤖 Agent: {response}")
            
        except KeyboardInterrupt:
            print("\n👋 Goodbye! Ending session...")
            break
        except Exception as e:
            print(f"❌ Error: {str(e)}")


def run_chat():
    """
    Run the interactive chat session.
    """
    asyncio.run(interactive_chat())


if __name__ == "__main__":
    run_chat()

