#!/usr/bin/env python3
"""Thin wrapper; implementation lives in ``src.setup_data`` (use ``uv run setup-data``)."""

from src.setup_data import main

if __name__ == "__main__":
    raise SystemExit(main())
