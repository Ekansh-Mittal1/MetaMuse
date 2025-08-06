"""
Agent output models for structured outputs.

These models are designed to work with openai-agent's structured output
capabilities, allowing agents to produce validated, typed outputs that
can be seamlessly passed between agents or consumed by workflows.
"""

from typing import List, Optional, Dict
from pydantic import BaseModel, Field, ConfigDict, field_validator

from .common import KeyValue
from .metadata_models import (
    CleanedSeriesMetadata,
    CleanedSampleMetadata,
    CleanedAbstractMetadata,
)

# Import CurationDataPackage for LinkerOutput
from .curation_models import CurationDataPackage, CurationResult


class IngestionOutput(BaseModel):
    """Structured output from IngestionAgent."""

    model_config = ConfigDict(extra="forbid")

    # Execution summary
    success: bool = Field(..., description="Whether ingestion completed successfully")
    message: str = Field(..., description="Summary of ingestion results")
    execution_time_seconds: float = Field(
        ..., ge=0, description="Time taken for execution"
    )

    # Input tracking
    geo_ids_requested: List[str] = Field(
        ..., description="List of GEO/PMID IDs requested"
    )
    extraction_type: str = Field(..., description="Type of extraction performed")

    # Output data (strict JSON schema compatible)
    extracted_metadata: Optional[List[KeyValue]] = Field(
        default=None, description="Raw extracted metadata as key-value pairs"
    )
    series_sample_mapping: Optional[List[KeyValue]] = Field(
        default=None, description="Series to sample mapping as key-value pairs"
    )

    # File management
    session_directory: str = Field(..., description="Path to session directory")
    files_created: List[str] = Field(
        default_factory=list, description="Files created during ingestion"
    )

    # Processing summary
    successful_extractions: List[str] = Field(
        default_factory=list, description="Successfully processed IDs"
    )
    failed_extractions: List[str] = Field(
        default_factory=list, description="Failed extraction IDs"
    )
    warnings: List[str] = Field(default_factory=list, description="Warnings generated")

    # Handoff data for next agent
    sample_ids_for_linking: List[str] = Field(
        default_factory=list, description="Sample IDs ready for linking process"
    )


class LinkerOutput(BaseModel):
    """Structured output from LinkerAgent."""

    model_config = ConfigDict(extra="forbid")

    # Execution summary
    success: bool = Field(..., description="Whether linking completed successfully")
    message: str = Field(..., description="Summary of linking results")
    execution_time_seconds: float = Field(
        ..., ge=0, description="Time taken for execution"
    )

    # Input tracking
    sample_ids_requested: List[str] = Field(
        ..., description="Sample IDs requested for linking"
    )
    session_directory: str = Field(..., description="Session directory used")

    # Processing configuration
    fields_removed_during_cleaning: List[str] = Field(
        default_factory=list,
        description="Fields that were removed during metadata cleaning",
    )

    # Output data (strict JSON schema compatible)
    linked_data: Optional[List[KeyValue]] = Field(
        default=None, description="Linked data objects as key-value pairs"
    )
    cleaned_metadata_files: Optional[List[KeyValue]] = Field(
        default=None, description="Paths to cleaned metadata files as key-value pairs"
    )

    # Cleaned metadata content
    cleaned_series_metadata: Optional[Dict[str, CleanedSeriesMetadata]] = Field(
        default=None, description="Cleaned series metadata by series ID"
    )
    cleaned_sample_metadata: Optional[Dict[str, CleanedSampleMetadata]] = Field(
        default=None, description="Cleaned sample metadata by sample ID"
    )
    cleaned_abstract_metadata: Optional[Dict[str, CleanedAbstractMetadata]] = Field(
        default=None, description="Cleaned abstract metadata by PMID"
    )

    # Curation data packages for handoff to CuratorAgent
    curation_packages: Optional[List[CurationDataPackage]] = Field(
        default=None,
        description="Structured curation data packages for CuratorAgent handoff",
    )

    # File management
    files_created: List[str] = Field(
        default_factory=list, description="Files created during linking"
    )

    # Processing summary
    successfully_linked: List[str] = Field(
        default_factory=list, description="Successfully linked sample IDs"
    )
    failed_linking: List[str] = Field(
        default_factory=list, description="Failed linking sample IDs"
    )
    warnings: List[str] = Field(default_factory=list, description="Warnings generated")

    # Handoff data for next agent
    sample_ids_for_curation: List[str] = Field(
        default_factory=list, description="Sample IDs ready for curation process"
    )
    recommended_curation_fields: List[str] = Field(
        default_factory=list, description="Suggested metadata fields for curation"
    )


class CuratorOutput(BaseModel):
    """Structured output from CuratorAgent."""

    model_config = ConfigDict(extra="forbid")

    # Execution summary
    success: bool = Field(..., description="Whether curation completed successfully")
    message: str = Field(..., description="Summary of curation results")
    execution_time_seconds: float = Field(
        ..., ge=0, description="Time taken for execution"
    )

    # Input tracking
    sample_ids_requested: List[str] = Field(
        ..., description="Sample IDs requested for curation"
    )
    target_field: str = Field(..., description="Target metadata field that was curated")
    session_directory: str = Field(..., description="Session directory used")

    # Output data - full CurationResult objects with complete information
    curation_results: Optional[List[CurationResult]] = Field(
        default=None,
        description="Detailed curation results with full CurationResult objects including candidates, confidence scores, and reconciliation details",
    )

    # Summary statistics
    total_samples_processed: int = Field(
        ..., ge=0, description="Total samples processed"
    )
    successful_curations: int = Field(
        ..., ge=0, description="Successfully curated samples"
    )
    samples_needing_review: int = Field(
        ..., ge=0, description="Samples requiring manual review"
    )

    # File management
    files_created: List[str] = Field(
        default_factory=list, description="Files created during curation"
    )
    curation_results_file: Optional[str] = Field(
        None, description="Path to detailed curation results file"
    )

    # Quality metrics
    average_confidence: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Average confidence across all curations"
    )

    @field_validator("average_confidence", mode="before")
    @classmethod
    def validate_average_confidence(cls, v):
        """Handle 'Not applicable' string by converting to None."""
        if isinstance(v, str) and v.lower() in ["not applicable", "n/a", "none"]:
            return None
        return v

    # Processing summary
    warnings: List[str] = Field(default_factory=list, description="Warnings generated")


# Utility functions for creating outputs


def create_successful_ingestion_output(
    geo_ids: List[str],
    extraction_type: str,
    session_dir: str,
    execution_time: float,
    **kwargs,
) -> IngestionOutput:
    """Create a successful ingestion output."""
    return IngestionOutput(
        success=True,
        message=f"Successfully processed {len(geo_ids)} GEO/PMID IDs",
        execution_time_seconds=execution_time,
        geo_ids_requested=geo_ids,
        extraction_type=extraction_type,
        session_directory=session_dir,
        **kwargs,
    )


def create_successful_linker_output(
    sample_ids: List[str], session_dir: str, execution_time: float, **kwargs
) -> LinkerOutput:
    """Create a successful linker output."""
    return LinkerOutput(
        success=True,
        message=f"Successfully linked {len(sample_ids)} samples",
        execution_time_seconds=execution_time,
        sample_ids_requested=sample_ids,
        session_directory=session_dir,
        **kwargs,
    )


def create_successful_curator_output(
    sample_ids: List[str],
    target_field: str,
    session_dir: str,
    execution_time: float,
    **kwargs,
) -> CuratorOutput:
    """Create a successful curator output."""
    return CuratorOutput(
        success=True,
        message=f"Successfully curated {target_field} for {len(sample_ids)} samples",
        execution_time_seconds=execution_time,
        sample_ids_requested=sample_ids,
        target_field=target_field,
        session_directory=session_dir,
        total_samples_processed=len(sample_ids),
        **kwargs,
    )
