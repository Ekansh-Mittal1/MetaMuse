"""
Common models used across multiple model files.
"""

from pydantic import BaseModel, Field, ConfigDict


class KeyValue(BaseModel):
    """Key-value pair for arbitrary string mappings."""

    model_config = ConfigDict(extra="forbid")

    key: str = Field(..., description="The key")
    value: str = Field(..., description="The value")
