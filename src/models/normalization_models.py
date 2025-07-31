"""
Pydantic models for metadata normalization workflow.

These models extend the curation workflow by adding ontology normalization
capabilities to candidate values extracted by the CuratorAgent.
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict

from .curation_models import CurationResult, ExtractedCandidate


class OntologyMatch(BaseModel):
    """A single ontology match result from semantic search."""

    model_config = ConfigDict(extra="forbid")

    term: str = Field(..., description="The matched ontology term")
    term_id: str = Field(..., description="The ontology term ID (e.g., MONDO:0018906)")
    score: float = Field(..., ge=0.0, le=1.0, description="Semantic similarity score (0-1)")
    ontology: str = Field(..., description="Source ontology (e.g., 'mondo', 'efo')")
    definition: Optional[str] = Field(None, description="Term definition if available")


class NormalizedCandidate(BaseModel):
    """An extracted candidate enhanced with ontology normalization."""

    model_config = ConfigDict(extra="forbid")

    # Original candidate information
    value: str = Field(..., description="Original extracted candidate value")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Original confidence score (0-1)")
    source: str = Field(..., description="Source type (series/sample/abstract)")
    context: str = Field(..., description="Context where candidate was found")
    rationale: str = Field(..., description="Explicit reasoning for extraction")
    prenormalized: str = Field(..., description="Original ontology-normalized term with ID")

    # Normalization results
    ontology_matches: List[OntologyMatch] = Field(
        default_factory=list, description="Ranked list of ontology matches"
    )
    best_match: Optional[OntologyMatch] = Field(
        None, description="The highest-scoring ontology match"
    )
    normalization_confidence: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Overall confidence in normalization"
    )
    normalization_notes: List[str] = Field(
        default_factory=list, description="Notes about the normalization process"
    )


class NormalizationResult(CurationResult):
    """Result of normalization for a single sample and target field, extending CurationResult."""

    model_config = ConfigDict(extra="forbid")

    # Enhanced candidates with normalization
    normalized_series_candidates: List[NormalizedCandidate] = Field(
        default_factory=list, description="Normalized candidates from series metadata"
    )
    normalized_sample_candidates: List[NormalizedCandidate] = Field(
        default_factory=list, description="Normalized candidates from sample metadata"
    )
    normalized_abstract_candidates: List[NormalizedCandidate] = Field(
        default_factory=list, description="Normalized candidates from abstract metadata"
    )

    # Enhanced final result with normalization
    final_normalized_term: Optional[str] = Field(
        None, description="Final normalized ontology term"
    )
    final_normalized_id: Optional[str] = Field(
        None, description="Final normalized ontology term ID"
    )
    final_ontology: Optional[str] = Field(
        None, description="Source ontology of final normalized term"
    )
    normalization_method: Optional[str] = Field(
        None, description="Method used for normalization (e.g., 'semantic_search')"
    )

    # Normalization metadata
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

    candidates: List[str] = Field(..., description="List of candidate values to normalize")
    target_field: str = Field(..., description="Target metadata field (e.g., 'Disease')")
    ontologies: Optional[List[str]] = Field(
        None, description="Specific ontologies to search (if None, uses defaults for field)"
    )
    top_k: int = Field(default=5, ge=1, le=20, description="Number of top matches to return")
    min_score: float = Field(
        default=0.5, ge=0.0, le=1.0, description="Minimum similarity score threshold"
    )


class BatchNormalizationResult(BaseModel):
    """Result of batch normalization across multiple samples."""

    model_config = ConfigDict(extra="forbid")

    sample_results: Dict[str, NormalizationResult] = Field(
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
    processing_summary: Dict[str, Any] = Field(
        default_factory=dict, description="Summary statistics and metadata"
    ) 