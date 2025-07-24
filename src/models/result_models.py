"""
Pydantic result models for agent and tool operations.

These models replace the previous @dataclass Result classes with validated,
typed Pydantic models that provide better error handling and serialization.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field, ConfigDict

from .common import KeyValue

from .metadata_models import (
    GSMMetadata, 
    GSEMetadata, 
    PMIDMetadata, 
    SeriesSampleMapping,
    LinkedData
)


class AgentResult(BaseModel):
    """Base result model for all agent operations."""
    
    model_config = ConfigDict(extra="forbid")
    
    success: bool = Field(..., description="Whether the operation succeeded")
    message: str = Field(..., description="Human-readable status message")
    agent_name: str = Field(..., description="Name of the agent that produced this result")
    timestamp: datetime = Field(default_factory=datetime.now, description="When this result was created")
    session_id: Optional[str] = Field(None, description="Session identifier")
    
    # Optional data payload - specific to each agent type (flexible for internal use)
    data: Optional[Dict[str, Any]] = Field(None, description="Agent-specific result data")
    
    # File tracking
    files_created: Optional[List[str]] = Field(None, description="Paths to files created during operation")
    files_modified: Optional[List[str]] = Field(None, description="Paths to files modified during operation")
    
    # Error tracking
    errors: Optional[List[str]] = Field(None, description="List of errors encountered")
    warnings: Optional[List[str]] = Field(None, description="List of warnings generated")


class IngestionResult(AgentResult):
    """Result from IngestionAgent operations."""
    
    agent_name: str = Field(default="IngestionAgent", description="Agent name")
    
    # Ingestion-specific data
    extracted_metadata: Optional[Dict[str, Union[GSMMetadata, GSEMetadata, PMIDMetadata]]] = Field(
        None, description="Extracted metadata objects keyed by ID"
    )
    series_mapping: Optional[SeriesSampleMapping] = Field(
        None, description="Series to sample mapping structure"
    )
    geo_ids_processed: Optional[List[str]] = Field(
        None, description="List of GEO/PMID IDs that were processed"
    )
    extraction_type: Optional[str] = Field(
        None, description="Type of extraction performed"
    )


class LinkerResult(AgentResult):
    """Result from LinkerAgent operations."""
    
    agent_name: str = Field(default="LinkerAgent", description="Agent name")
    
    # Linker-specific data
    linked_data: Optional[Dict[str, LinkedData]] = Field(
        None, description="Linked data objects keyed by sample ID"
    )
    cleaned_files: Optional[Dict[str, List[str]]] = Field(
        None, description="Cleaned files organized by sample ID"
    )
    samples_processed: Optional[List[str]] = Field(
        None, description="List of sample IDs that were processed"
    )
    fields_removed: Optional[List[str]] = Field(
        None, description="Fields that were removed during cleaning"
    )


class CandidateExtraction(BaseModel):
    """Individual candidate extraction result."""
    
    model_config = ConfigDict(extra="forbid")
    
    value: str = Field(..., description="Extracted candidate value")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score (0-1)")
    source_file: str = Field(..., description="Source file where value was found")
    context: Optional[str] = Field(None, description="Surrounding context")
    reasoning: Optional[str] = Field(None, description="Why this value was extracted")


class CuratorResult(AgentResult):
    """Result from CuratorAgent operations."""
    
    agent_name: str = Field(default="CuratorAgent", description="Agent name")
    
    # Curator-specific data
    target_field: Optional[str] = Field(
        None, description="The metadata field that was curated"
    )
    candidates: Optional[Dict[str, List[CandidateExtraction]]] = Field(
        None, description="Extracted candidates organized by sample ID"
    )
    reconciled_values: Optional[Dict[str, str]] = Field(
        None, description="Final reconciled values for each sample"
    )
    confidence_scores: Optional[Dict[str, float]] = Field(
        None, description="Confidence scores for reconciled values"
    )
    samples_curated: Optional[List[str]] = Field(
        None, description="List of sample IDs that were curated"
    )


class WorkflowResult(AgentResult):
    """Result from workflow operations that may involve multiple agents."""
    
    agent_name: str = Field(default="WorkflowOrchestrator", description="Agent name")
    
    # Workflow-specific data
    workflow_type: Optional[str] = Field(
        None, description="Type of workflow that was executed"
    )
    agent_results: Optional[List[AgentResult]] = Field(
        None, description="Results from individual agents in the workflow"
    )
    final_output_path: Optional[str] = Field(
        None, description="Path to final consolidated output"
    )
    



# Utility functions for working with results

def create_success_result(
    result_type: type,
    message: str,
    **kwargs
) -> AgentResult:
    """Create a successful result of the specified type."""
    return result_type(
        success=True,
        message=message,
        **kwargs
    )


def create_error_result(
    result_type: type,
    message: str,
    errors: List[str] = None,
    **kwargs
) -> AgentResult:
    """Create a failed result of the specified type."""
    return result_type(
        success=False,
        message=message,
        errors=errors or [message],
        **kwargs
    )


class SerializationResult(BaseModel):
    """Result from serialization operations - DendroForge pattern compatible."""
    
    model_config = ConfigDict(extra="forbid")
    
    success: bool = Field(..., description="Whether serialization succeeded")
    message: str = Field(..., description="Status message")
    files_created: Optional[List[str]] = Field(None, description="Paths to files created")
    timestamp: Optional[str] = Field(None, description="Timestamp of operation")
    error: Optional[str] = Field(None, description="Error message if failed") 