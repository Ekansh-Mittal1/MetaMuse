"""Evaluation package for batch result assessment using Gemini 2.5 Pro.

This package provides:
- Pydantic models defining structured outputs per field and per sample
- A Gemini client wrapper for structured JSON outputs
- Prompt templates used for evaluation
- Utilities to load raw data (abstract, series, sample) from batch directories
- A CLI entrypoint to evaluate a batch directory
"""



