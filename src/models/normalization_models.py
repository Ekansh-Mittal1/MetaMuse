"""
Pydantic models for metadata normalization workflow.

These models extend the curation workflow by adding ontology normalization
capabilities to candidate values extracted by the CuratorAgent.
"""

from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

from .curation_models import ExtractedCandidate
from .common import KeyValue


class OntologyMatch(BaseModel):
    """A single ontology match result from semantic search."""

    model_config = ConfigDict(extra="forbid")

    term: str = Field(..., description="The matched ontology term")
    term_id: str = Field(..., description="The ontology term ID (e.g., MONDO:0018906)")
    score: float = Field(
        ..., ge=0.0, le=1.0, description="Semantic similarity score (0-1)"
    )
    ontology: str = Field(..., description="Source ontology (e.g., 'mondo', 'efo')")
    definition: Optional[str] = Field(None, description="Term definition if available")


class NormalizedCandidate(BaseModel):
    """An extracted candidate enhanced with ontology normalization."""

    model_config = ConfigDict(extra="forbid")

    # Original candidate information
    value: str = Field(..., description="Original extracted candidate value")
    confidence: float = Field(
        ..., ge=0.0, le=1.0, description="Original confidence score (0-1)"
    )
    source: str = Field(..., description="Source type (series/sample/abstract)")
    context: str = Field(..., description="Context where candidate was found")
    rationale: str = Field(..., description="Explicit reasoning for extraction")
    prenormalized: str = Field(
        ..., description="Original ontology-normalized term with ID"
    )

    # Normalization results - top 5 matches only
    top_ontology_matches: List[OntologyMatch] = Field(
        default_factory=list, description="Top 5 ranked ontology matches"
    )

    # Legacy field for backward compatibility
    best_match: Optional[OntologyMatch] = Field(
        None,
        description="Legacy: The highest-scoring ontology match (use top_ontology_matches instead)",
    )

    normalization_confidence: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Overall confidence in normalization"
    )
    normalization_notes: List[str] = Field(
        default_factory=list, description="Notes about the normalization process"
    )


class NormalizationResult(BaseModel):
    """Result of normalization for a single sample and target field."""

    model_config = ConfigDict(extra="forbid")

    # Basic identification (copied from CurationResult)
    tool_name: str = Field(
        default="NormalizerAgent",
        description="Name of the tool that produced this output",
    )
    sample_id: str = Field(..., description="Sample ID that was normalized")
    target_field: str = Field(..., description="Target metadata field (e.g., Disease)")

    # Original candidates for reference
    series_candidates: List[ExtractedCandidate] = Field(
        default_factory=list, description="Original candidates from series metadata"
    )
    sample_candidates: List[ExtractedCandidate] = Field(
        default_factory=list, description="Original candidates from sample metadata"
    )
    abstract_candidates: List[ExtractedCandidate] = Field(
        default_factory=list, description="Original candidates from abstract metadata"
    )
    final_candidates: List[ExtractedCandidate] = Field(
        default_factory=list,
        description="Original top 3 candidates selected for normalization",
    )

    # Normalization results
    final_normalized_candidates: List[NormalizedCandidate] = Field(
        default_factory=list,
        description="Top 3 normalized candidates with their ontology matches",
    )

    # Legacy curation fields for backward compatibility
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

    # Legacy normalization fields for backward compatibility
    final_normalized_term: Optional[str] = Field(
        None, description="Legacy: Final normalized ontology term"
    )
    final_normalized_id: Optional[str] = Field(
        None, description="Legacy: Final normalized ontology term ID"
    )
    final_ontology: Optional[str] = Field(
        None, description="Legacy: Source ontology of final normalized term"
    )

    # Processing metadata
    reconciliation_needed: bool = Field(
        False, description="Whether manual reconciliation was needed"
    )
    reconciliation_reason: Optional[str] = Field(
        None, description="Reason for manual reconciliation"
    )
    sources_processed: List[str] = Field(
        default_factory=list, description="Sources that were processed"
    )
    processing_notes: List[str] = Field(
        default_factory=list, description="Processing notes and warnings"
    )

    # Normalization-specific metadata
    normalization_method: Optional[str] = Field(
        None, description="Method used for normalization (e.g., 'semantic_search')"
    )
    ontologies_searched: List[str] = Field(
        default_factory=list, description="List of ontologies that were searched"
    )
    normalization_timestamp: Optional[str] = Field(
        None, description="When normalization was performed"
    )
    normalization_tool_version: Optional[str] = Field(
        None, description="Version of normalization tool used"
    )


class NormalizationRequest(BaseModel):
    """Request for normalizing a set of candidates."""

    model_config = ConfigDict(extra="forbid")

    candidates: List[str] = Field(
        ..., description="List of candidate values to normalize"
    )
    target_field: str = Field(
        ..., description="Target metadata field (e.g., 'Disease')"
    )
    ontologies: Optional[List[str]] = Field(
        None,
        description="Specific ontologies to search (if None, uses defaults for field)",
    )
    top_k: int = Field(
        default=5, ge=1, le=20, description="Number of top matches to return"
    )
    min_score: float = Field(
        default=0.5, ge=0.0, le=1.0, description="Minimum similarity score threshold"
    )


class SampleResultEntry(BaseModel):
    sample_id: str
    result: NormalizationResult

    model_config = ConfigDict(extra="forbid")


class BatchNormalizationResult(BaseModel):
    """Result of batch normalization across multiple samples."""

    model_config = ConfigDict(extra="forbid")

    sample_results: List[SampleResultEntry] = Field(
        ..., description="Normalization results keyed by sample_id"
    )
    session_directory: str = Field(..., description="Path to session directory")
    target_field: str = Field(..., description="Target field that was normalized")
    total_candidates_normalized: int = Field(
        ..., description="Total number of candidates processed"
    )
    successful_normalizations: int = Field(
        ..., description="Number of candidates successfully normalized"
    )
    processing_summary: List[KeyValue] = Field(
        default_factory=list, description="Summary statistics and metadata"
    )
