"""
Pydantic models for metadata normalization workflow.

These models focus specifically on ontology normalization results,
providing a clean separation from curation workflow models.
"""

from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

from .curation_models import SampleType
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

    # Basic identification
    tool_name: str = Field(
        default="NormalizerAgent",
        description="Name of the tool that produced this output",
    )
    sample_id: str = Field(..., description="Sample ID that was normalized")
    target_field: str = Field(..., description="Target metadata field (e.g., Disease)")

    # Input candidates (minimal reference to original extraction)
    original_candidates: List[str] = Field(
        default_factory=list, description="Original candidate values that were normalized"
    )
    
    # Normalization results - the core output
    normalized_candidates: List[NormalizedCandidate] = Field(
        default_factory=list,
        description="Top normalized candidates with their ontology matches",
    )
    
    # Best normalization result
    best_normalized_result: Optional[NormalizedCandidate] = Field(
        None, description="The highest-confidence normalized result"
    )
    
    # Normalization-specific metadata
    normalization_method: str = Field(
        ..., description="Method used for normalization (e.g., 'semantic_search', 'exact_match')"
    )
    ontologies_searched: List[str] = Field(
        default_factory=list, description="List of ontologies that were searched"
    )
    normalization_timestamp: str = Field(
        ..., description="When normalization was performed"
    )
    normalization_tool_version: str = Field(
        ..., description="Version of normalization tool used"
    )
    
    # Processing metadata
    sources_processed: List[str] = Field(
        default_factory=list, description="Sources that were processed during normalization"
    )
    processing_notes: List[str] = Field(
        default_factory=list, description="Processing notes and warnings from normalization"
    )
    
    # Quality indicators
    normalization_success: bool = Field(
        ..., description="Whether normalization was successful"
    )
    normalization_confidence: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Overall confidence in the normalization result"
    )
    
    # Legacy fields for backward compatibility (deprecated)
    final_normalized_term: Optional[str] = Field(
        None, description="Legacy: Final normalized ontology term (use best_normalized_result instead)"
    )
    final_normalized_id: Optional[str] = Field(
        None, description="Legacy: Final normalized ontology term ID (use best_normalized_result instead)"
    )
    final_ontology: Optional[str] = Field(
        None, description="Legacy: Source ontology of final normalized term (use best_normalized_result instead)"
    )


class SampleTypeNormalizationResult(BaseModel):
    """Result for SampleType target field (no normalization needed)."""

    model_config = ConfigDict(extra="forbid")

    # Basic identification
    tool_name: str = Field(
        default="NormalizerAgent",
        description="Name of the tool that produced this output",
    )
    sample_id: str = Field(..., description="Sample ID that was processed")
    target_field: str = Field(
        default="SampleType", description="Target metadata field (SampleType)"
    )

    # Classification result (no normalization needed)
    sample_type: SampleType = Field(
        ..., description="Sample type classification (primary_sample, cell_line, or unknown)"
    )
    confidence: float = Field(
        ..., ge=0.0, le=1.0, description="Confidence in the sample type classification"
    )

    # Input candidates (minimal reference)
    original_candidates: List[str] = Field(
        default_factory=list, description="Original candidate values that were processed"
    )

    # Processing metadata
    processing_method: str = Field(
        default="enum_classification", description="Method used (enum classification)"
    )
    sources_processed: List[str] = Field(
        default_factory=list, description="Sources that were processed"
    )
    processing_notes: List[str] = Field(
        default_factory=list, description="Processing notes and warnings"
    )
    processing_timestamp: str = Field(
        ..., description="When processing was performed"
    )
    processing_tool_version: str = Field(
        ..., description="Version of processing tool used"
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


class PrimarySampleResultEntry(BaseModel):
    sample_id: str
    result: SampleTypeNormalizationResult

    model_config = ConfigDict(extra="forbid")


class BatchNormalizationResult(BaseModel):
    """Result of batch normalization across multiple samples."""

    model_config = ConfigDict(extra="forbid")

    # Core normalization results
    sample_results: List[SampleResultEntry] = Field(
        ..., description="Normalization results keyed by sample_id"
    )
    
    # Session and processing metadata
    session_directory: str = Field(..., description="Path to session directory")
    target_field: str = Field(..., description="Target field that was normalized")
    
    # Normalization statistics
    total_samples_processed: int = Field(
        ..., description="Total number of samples processed for normalization"
    )
    successful_normalizations: int = Field(
        ..., description="Number of samples successfully normalized"
    )
    failed_normalizations: int = Field(
        default=0, description="Number of samples that failed normalization"
    )
    
    # Normalization-specific metadata
    ontologies_searched: List[str] = Field(
        default_factory=list, description="List of ontologies that were searched during normalization"
    )
    normalization_method: str = Field(
        default="semantic_search", description="Method used for normalization (e.g., 'semantic_search', 'exact_match')"
    )
    normalization_timestamp: str = Field(
        ..., description="When normalization was performed"
    )
    normalization_tool_version: str = Field(
        ..., description="Version of normalization tool used"
    )
    
    # Processing summary
    processing_summary: List[KeyValue] = Field(
        default_factory=list, description="Summary of normalization processing steps"
    )
    
    # Quality metrics
    average_normalization_confidence: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Average confidence score across all successful normalizations"
    )
    top_ontology_sources: List[str] = Field(
        default_factory=list, description="Most frequently used ontology sources in normalization results"
    )


class BatchPrimarySampleResult(BaseModel):
    """Result of batch PrimarySample processing across multiple samples."""

    model_config = ConfigDict(extra="forbid")

    sample_results: List[PrimarySampleResultEntry] = Field(
        ..., description="PrimarySample results keyed by sample_id"
    )
    session_directory: str = Field(..., description="Path to session directory")
    target_field: str = Field(
        default="PrimarySample", description="Target field that was processed"
    )
    total_samples_processed: int = Field(
        ..., description="Total number of samples processed"
    )
    primary_samples_count: int = Field(
        ..., description="Number of samples classified as primary (patient biopsy)"
    )
    cell_line_samples_count: int = Field(
        ..., description="Number of samples classified as cell lines"
    )
    processing_summary: List[KeyValue] = Field(
        default_factory=list, description="Summary of processing steps"
    )


class SampleTypeResultEntry(BaseModel):
    sample_id: str
    result: SampleTypeNormalizationResult

    model_config = ConfigDict(extra="forbid")


class BatchSampleTypeResult(BaseModel):
    """Result of batch SampleType processing across multiple samples."""

    model_config = ConfigDict(extra="forbid")

    sample_results: List[SampleTypeResultEntry] = Field(
        ..., description="SampleType results keyed by sample_id"
    )
    session_directory: str = Field(..., description="Path to session directory")
    target_field: str = Field(
        default="SampleType", description="Target field that was processed"
    )
    total_samples_processed: int = Field(
        ..., description="Total number of samples processed"
    )
    primary_samples_count: int = Field(
        ..., description="Number of samples classified as primary (patient biopsy)"
    )
    cell_line_samples_count: int = Field(
        ..., description="Number of samples classified as cell lines"
    )
    unknown_samples_count: int = Field(
        ..., description="Number of samples classified as unknown"
    )
    processing_summary: List[KeyValue] = Field(
        default_factory=list, description="Processing summary information"
    )
