"""
Pydantic models for MetaMuse agent system.

This package contains all data models used for structured data exchange
between agents, replacing the previous JSON-based approach.
"""

from .metadata_models import (
    GSMMetadata,
    GSMAttributes,
    GSEMetadata, 
    GSEAttributes,
    PMIDMetadata,
    SeriesSampleMapping,
    LinkedData,
)

from .result_models import (
    IngestionResult,
    LinkerResult,
    CuratorResult,
    WorkflowResult,
    AgentResult,
    CandidateExtraction,
    SerializationResult,
    create_success_result,
    create_error_result,
)

from .agent_outputs import (
    IngestionOutput,
    LinkerOutput,
    CuratorOutput,
)

from .serialization import (
    ModelSerializer,
    WorkflowSerializer,
    SerializationError,
    serialize_any_metadata,
    load_metadata_from_json,
)

from .curation_models import (
    CleanedSeriesMetadata,
    CleanedSampleMetadata,
    CleanedAbstractMetadata,
    CurationDataPackage,
    ExtractedCandidate,
    CurationResult,
)

__all__ = [
    # Metadata models
    "GSMMetadata",
    "GSMAttributes",
    "GSEMetadata", 
    "GSEAttributes",
    "PMIDMetadata",
    "SeriesSampleMapping",
    "LinkedData",
    # Result models
    "IngestionResult",
    "LinkerResult", 
    "CuratorResult",
    "WorkflowResult",
    "AgentResult",
    "CandidateExtraction",
    "SerializationResult",
    "create_success_result",
    "create_error_result",
    # Agent output models
    "IngestionOutput",
    "LinkerOutput",
    "CuratorOutput",
    # Serialization
    "ModelSerializer",
    "WorkflowSerializer",
    "SerializationError",
    "serialize_any_metadata",
    "load_metadata_from_json",
    # Curation models
    "CleanedSeriesMetadata",
    "CleanedSampleMetadata", 
    "CleanedAbstractMetadata",
    "CurationDataPackage",
    "ExtractedCandidate",
    "CurationResult",
] 