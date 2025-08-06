"""
Pydantic models for metadata curation workflow.

These models package cleaned metadata from different sources (series, sample, abstract)
for efficient handoff to the CuratorAgent without requiring file I/O operations.
"""

from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

from .metadata_models import (
    CleanedSeriesMetadata,
    CleanedSampleMetadata,
    CleanedAbstractMetadata,
)


class CurationDataPackage(BaseModel):
    """Complete package of cleaned metadata for one sample from all sources."""

    model_config = ConfigDict(extra="forbid")

    sample_id: str = Field(..., description="Primary sample ID being curated")
    series_metadata: Optional[CleanedSeriesMetadata] = Field(
        None, description="Cleaned series metadata"
    )
    sample_metadata: Optional[CleanedSampleMetadata] = Field(
        None, description="Cleaned sample metadata"
    )
    abstract_metadata: Optional[CleanedAbstractMetadata] = Field(
        None, description="Cleaned abstract metadata"
    )


class ExtractedCandidate(BaseModel):
    """A single extracted candidate from metadata."""

    model_config = ConfigDict(extra="forbid")

    value: str = Field(..., description="Extracted candidate value")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score (0-1)")
    source: str = Field(..., description="Source type (series/sample/abstract)")
    context: str = Field(..., description="Context where candidate was found")
    rationale: str = Field(
        ..., description="Explicit reasoning for why this candidate was extracted"
    )
    prenormalized: str = Field(..., description="Ontology-normalized term with ID")


class CurationResult(BaseModel):
    """Result of curation for a single sample and target field."""

    model_config = ConfigDict(extra="forbid")

    # Tool identification
    tool_name: str = Field(
        default="Unknown Tool", description="Name of the tool that produced this output"
    )

    sample_id: str = Field(..., description="Sample ID that was curated")
    target_field: str = Field(..., description="Target metadata field (e.g., Disease)")

    # Extracted candidates by source
    series_candidates: List[ExtractedCandidate] = Field(
        default_factory=list, description="Candidates from series metadata"
    )
    sample_candidates: List[ExtractedCandidate] = Field(
        default_factory=list, description="Candidates from sample metadata"
    )
    abstract_candidates: List[ExtractedCandidate] = Field(
        default_factory=list, description="Candidates from abstract metadata"
    )

    # Final result - top 3 candidates across all sources
    final_candidates: List[ExtractedCandidate] = Field(
        default_factory=list,
        description="Top 3 candidates ranked by confidence across all sources",
    )

    # Legacy fields for backward compatibility with existing structured output
    final_candidate: Optional[str] = Field(
        None,
        description="Legacy: Final reconciled candidate value (use final_candidates instead)",
    )
    final_confidence: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Legacy: Confidence in final result (use final_candidates instead)",
    )

    reconciliation_needed: bool = Field(
        False, description="Whether manual reconciliation is needed"
    )
    reconciliation_reason: Optional[str] = Field(
        None, description="Reason for manual reconciliation"
    )

    # Processing metadata
    sources_processed: List[str] = Field(
        default_factory=list, description="Sources that were processed"
    )
    processing_notes: List[str] = Field(
        default_factory=list, description="Processing notes and warnings"
    )
