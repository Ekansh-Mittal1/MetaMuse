"""
Pytest configuration for curator tools tests.

This module sets up the test environment with OpenRouter API configuration.
"""

import os
import pytest
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """
    Set up test environment with OpenRouter configuration.
    
    This fixture runs once per test session and ensures that
    the OpenRouter API key is available for all tests.
    """
    # Set default OpenRouter configuration for testing
    if not os.getenv("OPENROUTER_API_KEY"):
        # Set a dummy API key for testing (tests will be skipped if no real key)
        os.environ["OPENROUTER_API_KEY"] = "test_key_for_testing"
        os.environ["OPENROUTER_BASE_URL"] = "https://openrouter.ai/api/v1"
    
    # Ensure the environment is properly configured
    print("🔧 Test environment configured with OpenRouter settings")
    print(f"📡 OpenRouter Base URL: {os.getenv('OPENROUTER_BASE_URL')}")
    print(f"🔑 API Key configured: {'Yes' if os.getenv('OPENROUTER_API_KEY') else 'No'}")


@pytest.fixture(scope="function")
def mock_openai_client(monkeypatch):
    """
    Mock OpenAI client for tests that don't need real API calls.
    
    This fixture can be used to mock the OpenAI client in tests
    that don't require actual API calls to OpenRouter.
    """
    class MockOpenAIClient:
        def __init__(self, *args, **kwargs):
            self.base_url = kwargs.get('base_url', 'https://openrouter.ai/api/v1')
            self.api_key = kwargs.get('api_key', 'test_key')
        
        def chat(self, *args, **kwargs):
            # Return a mock response
            class MockResponse:
                def __init__(self):
                    self.choices = [type('Choice', (), {'message': type('Message', (), {'content': 'Mock response'})()})()]
            
            return MockResponse()
    
    # Mock the OpenAI client
    monkeypatch.setattr("src.tools.curator_tools.OpenAI", MockOpenAIClient)
    return MockOpenAIClient() 