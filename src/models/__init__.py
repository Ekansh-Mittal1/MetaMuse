"""
Pydantic models for the metadata curation workflow.

This module provides structured data models for:
- Metadata cleaning and validation
- Curation workflow data packages
- Agent outputs and results
- Normalization results
"""

# Import all model classes for easy access
from .metadata_models import (
    CleanedSeriesMetadata,
    CleanedSampleMetadata,
    CleanedAbstractMetadata,
    GSMMetadata,
    GSEMetadata,
    PMIDMetadata,
    SeriesSampleMapping,
    LinkedData,
)

# Curation models
from .curation_models import (
    CurationDataPackage,
    ExtractedCandidate,
    SampleTypeExtractedCandidate,
    CurationResult,
    SampleTypeCurationResult,
    SampleType,
)

# Normalization models
from .normalization_models import (
    OntologyMatch,
    OntologyMatchCandidate,
    CandidateWithMatches,
    ToolNormalizationOutput,
    NormalizedCandidate,
    NormalizationResult,
    SampleTypeNormalizationResult,
    NormalizationRequest,
    SampleResultEntry,
    SampleTypeResultEntry,
    BatchNormalizationResult,
    BatchSampleTypeResult,
)

from .agent_outputs import (
    IngestionOutput,
    LinkerOutput,
    CuratorOutput,
    SampleTypeCuratorOutput,
    create_successful_ingestion_output,
    create_successful_linker_output,
    create_successful_curator_output,
)

from .result_models import (
    WorkflowResult,
    AgentResult,
    IngestionResult,
    LinkerResult,
    CuratorResult,
    SerializationResult,
    create_success_result,
    create_error_result,
)

from .common import KeyValue

from .serialization import (
    serialize_any_metadata,
    load_metadata_from_json,
)

# Export all models for easy importing
__all__ = [
    # Metadata models
    "CleanedSeriesMetadata",
    "CleanedSampleMetadata", 
    "CleanedAbstractMetadata",
    "GSMMetadata",
    "GSEMetadata",
    "PMIDMetadata",
    "SeriesSampleMapping",
    "LinkedData",
    
    # Curation models
    "CurationDataPackage",
    "ExtractedCandidate",
    "SampleTypeExtractedCandidate",
    "CurationResult",
    "SampleTypeCurationResult",
    "SampleType",
    
    # Normalization models
    "OntologyMatch",
    "OntologyMatchCandidate",
    "CandidateWithMatches",
    "ToolNormalizationOutput",
    "NormalizedCandidate", 
    "NormalizationResult",
    "SampleTypeNormalizationResult",
    "NormalizationRequest",
    "SampleResultEntry",
    "SampleTypeResultEntry",
    "BatchNormalizationResult",
    "BatchSampleTypeResult",
    
    # Agent output models
    "IngestionOutput",
    "LinkerOutput", 
    "CuratorOutput",
    "SampleTypeCuratorOutput",
    
    # Result models
    "WorkflowResult",
    "AgentResult",
    "IngestionResult",
    "LinkerResult",
    "CuratorResult",
    "SerializationResult",
    "create_success_result",
    "create_error_result",
    
    # Common models
    "KeyValue",
    
    # Serialization utilities
    "serialize_any_metadata",
    "load_metadata_from_json",
]
