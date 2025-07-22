"""
Agent output models for structured outputs.

These models are designed to work with openai-agent's structured output
capabilities, allowing agents to produce validated, typed outputs that
can be seamlessly passed between agents or consumed by workflows.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, ConfigDict

from .metadata_models import (
    GSMMetadata,
    GSEMetadata, 
    PMIDMetadata,
    SeriesSampleMapping,
    LinkedData
)
from .result_models import CandidateExtraction


class IngestionOutput(BaseModel):
    """Structured output from IngestionAgent."""
    
    model_config = ConfigDict(extra="forbid")
    
    # Execution summary
    success: bool = Field(..., description="Whether ingestion completed successfully")
    message: str = Field(..., description="Summary of ingestion results")
    execution_time_seconds: float = Field(..., ge=0, description="Time taken for execution")
    
    # Input tracking
    geo_ids_requested: List[str] = Field(..., description="List of GEO/PMID IDs requested")
    extraction_type: str = Field(..., description="Type of extraction performed")
    
    # Output data (simplified for DendroForge pattern compatibility)
    extracted_metadata: Optional[dict] = Field(
        default=None,
        description="Raw extracted metadata by ID"
    )
    series_sample_mapping: Optional[dict] = Field(
        default=None,
        description="Series to sample mapping"
    )
    
    # File management
    session_directory: str = Field(..., description="Path to session directory")
    files_created: List[str] = Field(default_factory=list, description="Files created during ingestion")
    
    # Processing summary
    successful_extractions: List[str] = Field(default_factory=list, description="Successfully processed IDs")
    failed_extractions: List[str] = Field(default_factory=list, description="Failed extraction IDs")
    warnings: List[str] = Field(default_factory=list, description="Warnings generated")
    
    # Handoff data for next agent
    sample_ids_for_linking: List[str] = Field(
        default_factory=list,
        description="Sample IDs ready for linking process"
    )
    
    @property
    def extraction_success_rate(self) -> float:
        """Calculate the success rate of extractions."""
        total = len(self.geo_ids_requested)
        if total == 0:
            return 0.0
        return len(self.successful_extractions) / total


class LinkerOutput(BaseModel):
    """Structured output from LinkerAgent."""
    
    model_config = ConfigDict(extra="forbid")
    
    # Execution summary
    success: bool = Field(..., description="Whether linking completed successfully")
    message: str = Field(..., description="Summary of linking results")
    execution_time_seconds: float = Field(..., ge=0, description="Time taken for execution")
    
    # Input tracking
    sample_ids_requested: List[str] = Field(..., description="Sample IDs requested for linking")
    session_directory: str = Field(..., description="Session directory used")
    
    # Processing configuration
    fields_removed_during_cleaning: List[str] = Field(
        default_factory=list,
        description="Fields that were removed during metadata cleaning"
    )
    
    # Output data (simplified for DendroForge pattern compatibility)
    linked_data: Optional[dict] = Field(
        default=None,
        description="Linked data objects by sample ID"
    )
    cleaned_metadata_files: Optional[dict] = Field(
        default=None,
        description="Paths to cleaned metadata files by sample ID"
    )
    
    # File management  
    files_created: List[str] = Field(default_factory=list, description="Files created during linking")
    
    # Processing summary
    successfully_linked: List[str] = Field(default_factory=list, description="Successfully linked sample IDs")
    failed_linking: List[str] = Field(default_factory=list, description="Failed linking sample IDs")
    warnings: List[str] = Field(default_factory=list, description="Warnings generated")
    
    # Handoff data for next agent
    sample_ids_for_curation: List[str] = Field(
        default_factory=list,
        description="Sample IDs ready for curation process"
    )
    recommended_curation_fields: List[str] = Field(
        default_factory=list,
        description="Suggested metadata fields for curation"
    )
    
    @property
    def linking_success_rate(self) -> float:
        """Calculate the success rate of linking operations."""
        total = len(self.sample_ids_requested)
        if total == 0:
            return 0.0
        return len(self.successfully_linked) / total





class CuratorOutput(BaseModel):
    """Structured output from CuratorAgent."""
    
    model_config = ConfigDict(extra="forbid")
    
    # Execution summary
    success: bool = Field(..., description="Whether curation completed successfully")
    message: str = Field(..., description="Summary of curation results")
    execution_time_seconds: float = Field(..., ge=0, description="Time taken for execution")
    
    # Input tracking
    sample_ids_requested: List[str] = Field(..., description="Sample IDs requested for curation")
    target_field: str = Field(..., description="Target metadata field that was curated")
    session_directory: str = Field(..., description="Session directory used")
    
    # Output data (simplified for DendroForge pattern compatibility)
    curation_results: Optional[dict] = Field(
        default=None,
        description="Detailed curation results for each sample"
    )
    
    # Summary statistics
    total_samples_processed: int = Field(..., ge=0, description="Total samples processed")
    successful_curations: int = Field(..., ge=0, description="Successfully curated samples")
    samples_needing_review: int = Field(..., ge=0, description="Samples requiring manual review")
    
    # File management
    files_created: List[str] = Field(default_factory=list, description="Files created during curation")
    curation_results_file: Optional[str] = Field(
        None, description="Path to detailed curation results file"
    )
    
    # Quality metrics
    average_confidence: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Average confidence across all curations"
    )
    
    # Processing summary
    warnings: List[str] = Field(default_factory=list, description="Warnings generated")
    
    @property
    def curation_success_rate(self) -> float:
        """Calculate the success rate of curation operations."""
        if self.total_samples_processed == 0:
            return 0.0
        return self.successful_curations / self.total_samples_processed
    
    @property
    def manual_review_rate(self) -> float:
        """Calculate the rate of samples requiring manual review."""
        if self.total_samples_processed == 0:
            return 0.0
        return self.samples_needing_review / self.total_samples_processed
    
    def get_final_values(self) -> Dict[str, str]:
        """Get a dictionary of sample_id -> final_value for all successfully curated samples."""
        return {
            result.sample_id: result.final_value
            for result in self.curation_results
            if result.curation_successful and result.final_value is not None
        }
    
    def get_samples_for_review(self) -> List[str]:
        """Get list of sample IDs that need manual review."""
        return [
            result.sample_id
            for result in self.curation_results
            if result.needs_manual_review
        ]


# Utility functions for creating outputs

def create_successful_ingestion_output(
    geo_ids: List[str],
    extraction_type: str,
    session_dir: str,
    execution_time: float,
    **kwargs
) -> IngestionOutput:
    """Create a successful ingestion output."""
    return IngestionOutput(
        success=True,
        message=f"Successfully processed {len(geo_ids)} GEO/PMID IDs",
        execution_time_seconds=execution_time,
        geo_ids_requested=geo_ids,
        extraction_type=extraction_type,
        session_directory=session_dir,
        **kwargs
    )


def create_successful_linker_output(
    sample_ids: List[str],
    session_dir: str,
    execution_time: float,
    **kwargs
) -> LinkerOutput:
    """Create a successful linker output."""
    return LinkerOutput(
        success=True,
        message=f"Successfully linked {len(sample_ids)} samples",
        execution_time_seconds=execution_time,
        sample_ids_requested=sample_ids,
        session_directory=session_dir,
        **kwargs
    )


def create_successful_curator_output(
    sample_ids: List[str],
    target_field: str,
    session_dir: str,
    execution_time: float,
    **kwargs
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
        **kwargs
    ) 