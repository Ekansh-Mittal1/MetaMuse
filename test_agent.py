#!/usr/bin/env python3

"""
Test script for the MetaMuse Ingestion Agent.
"""

import sys
import os
from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

sys.path.append('.')

def test_imports():
    """Test that all imports work correctly."""
    print("Testing imports...")
    
    try:
        from src.agents.tool_utils import get_geo_tools, get_available_tools
        print("✅ tool_utils imported successfully")
        
        from src.agents.ingestion import create_ingestion_agent
        print("✅ ingestion imported successfully")
        
        from src.workflows.pipeline import create_agent
        print("✅ pipeline imported successfully")
        
        from src.utils.prompts import load_prompt
        print("✅ prompts imported successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False


def test_tools():
    """Test that tools can be created."""
    print("\nTesting tool creation...")
    
    try:
        from src.agents.tool_utils import get_geo_tools
        
        # Create tools
        tools = get_geo_tools("test_session")
        
        print(f"✅ Created {len(tools)} tools:")
        for i, tool in enumerate(tools, 1):
            # Handle different tool types
            if hasattr(tool, '__name__'):
                tool_name = tool.__name__
            elif hasattr(tool, 'name'):
                tool_name = tool.name
            elif hasattr(tool, 'function'):
                tool_name = tool.function.__name__
            else:
                tool_name = f"Tool_{i}"
            print(f"   {i}. {tool_name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Tool creation error: {e}")
        return False


def test_agent_creation():
    """Test that agent can be created."""
    print("\nTesting agent creation...")
    
    try:
        from src.workflows.pipeline import create_agent
        
        # Create agent
        agent = create_agent()
        
        print(f"✅ Agent created successfully: {agent.name}")
        print(f"   Tools: {len(agent.tools)}")
        print(f"   Instructions length: {len(agent.instructions)} characters")
        
        return True
        
    except ValueError as e:
        if "OPENROUTER_API_KEY" in str(e):
            print("❌ Agent creation error: OPENROUTER_API_KEY environment variable is required")
            print("   Please set OPENROUTER_API_KEY in your .env file")
        elif "NCBI_EMAIL" in str(e):
            print("❌ Agent creation error: NCBI_EMAIL environment variable is required")
            print("   Please set NCBI_EMAIL in your .env file")
        else:
            print(f"❌ Agent creation error: {e}")
        return False
    except Exception as e:
        print(f"❌ Agent creation error: {e}")
        return False


def test_prompt_loading():
    """Test that prompts can be loaded."""
    print("\nTesting prompt loading...")
    
    try:
        from src.utils.prompts import load_prompt
        
        # Load prompt
        prompt = load_prompt("planning_agent.md")
        
        print(f"✅ Prompt loaded successfully")
        print(f"   Length: {len(prompt)} characters")
        print(f"   Contains 'MetaMuse': {'MetaMuse' in prompt}")
        
        return True
        
    except Exception as e:
        print(f"❌ Prompt loading error: {e}")
        return False


def main():
    """Run all tests."""
    print("🧪 Testing MetaMuse Ingestion Agent Setup")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_tools,
        test_agent_creation,
        test_prompt_loading
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print("=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The agent is ready to use.")
        print("\nTo start chatting with the agent, run:")
        print("   python src/workflows/pipeline.py")
        print("\nRequired environment variables:")
        print("   - OPENROUTER_API_KEY: Your OpenRouter API key")
        print("   - NCBI_EMAIL: Your email for NCBI E-Utils")
        print("   - NCBI_API_KEY: Your NCBI API key (optional but recommended)")
    else:
        print("❌ Some tests failed. Please check the errors above.")
        print("\nMake sure you have the required environment variables set:")
        print("   - OPENROUTER_API_KEY: Your OpenRouter API key")
        print("   - NCBI_EMAIL: Your email for NCBI E-Utils")
        print("   - NCBI_API_KEY: Your NCBI API key (optional but recommended)")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main()) 