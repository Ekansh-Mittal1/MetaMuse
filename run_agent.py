#!/usr/bin/env python3

"""
Launcher script for the MetaMuse Ingestion Agent.
"""

import sys
import os
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

# Import and run the pipeline
from src.workflows.pipeline import run_chat

if __name__ == "__main__":
    run_chat() 