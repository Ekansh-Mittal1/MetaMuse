#!/usr/bin/env python3

"""
Test script for the launcher.
"""

import sys
import os
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from src.workflows.pipeline import create_agent
    print("✅ Successfully imported pipeline")
    
    agent = create_agent()
    print(f"✅ Successfully created agent: {agent.name}")
    
    print("🎉 Launcher is working correctly!")
    print("You can now run: python run_agent.py")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc() 