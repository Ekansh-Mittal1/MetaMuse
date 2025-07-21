"""
Serialization utilities for converting Pydantic models to JSON files.

This module provides tools for persisting Pydantic objects as JSON files
at the end of agent workflows, maintaining compatibility with the existing
file-based approach while leveraging the benefits of typed objects.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Union, Optional
from pydantic import BaseModel

from .metadata_models import (
    GSMMetadata,
    GSEMetadata, 
    PMIDMetadata,
    SeriesSampleMapping,
    LinkedData
)
from .result_models import (
    AgentResult,
    IngestionResult,
    LinkerResult,
    CuratorResult,
    WorkflowResult
)
from .agent_outputs import (
    IngestionOutput,
    LinkerOutput,
    CuratorOutput
)


class SerializationError(Exception):
    """Exception raised during serialization operations."""
    pass


class ModelSerializer:
    """Handles serialization of Pydantic models to JSON files."""
    
    def __init__(self, session_dir: Union[str, Path]):
        """
        Initialize serializer with session directory.
        
        Parameters
        ----------
        session_dir : str or Path
            Path to the session directory where files will be written
        """
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
    
    def serialize_metadata(
        self, 
        metadata: Union[GSMMetadata, GSEMetadata, PMIDMetadata],
        filename: Optional[str] = None
    ) -> str:
        """
        Serialize metadata object to JSON file.
        
        Parameters
        ----------
        metadata : GSMMetadata, GSEMetadata, or PMIDMetadata
            Metadata object to serialize
        filename : str, optional
            Custom filename. If not provided, generates based on metadata type and ID
            
        Returns
        -------
        str
            Path to the created JSON file
        """
        if filename is None:
            if isinstance(metadata, GSMMetadata):
                filename = f"{metadata.gsm_id}_metadata.json"
            elif isinstance(metadata, GSEMetadata):
                filename = f"{metadata.gse_id}_metadata.json"
            elif isinstance(metadata, PMIDMetadata):
                filename = f"PMID_{metadata.pmid}_metadata.json"
            else:
                raise SerializationError(f"Unknown metadata type: {type(metadata)}")
        
        file_path = self.session_dir / filename
        
        # Handle series subdirectories for GSM files
        if isinstance(metadata, GSMMetadata):
            # Try to find the appropriate series directory
            series_dirs = [d for d in self.session_dir.iterdir() if d.is_dir() and d.name.startswith('GSE')]
            if series_dirs:
                # Use the first series directory found
                # In a real implementation, you'd want to match based on the series mapping
                file_path = series_dirs[0] / filename
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(
                    metadata.model_dump(),
                    f,
                    indent=2,
                    ensure_ascii=False,
                    default=self._json_encoder
                )
            return str(file_path)
        except Exception as e:
            raise SerializationError(f"Failed to serialize metadata to {file_path}: {str(e)}")
    
    def serialize_mapping(
        self,
        mapping: SeriesSampleMapping,
        filename: str = "series_sample_mapping.json"
    ) -> str:
        """
        Serialize series-sample mapping to JSON file.
        
        Parameters
        ----------
        mapping : SeriesSampleMapping
            Mapping object to serialize
        filename : str
            Filename for the mapping file
            
        Returns
        -------
        str
            Path to the created JSON file
        """
        file_path = self.session_dir / filename
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(
                    mapping.model_dump(),
                    f,
                    indent=2,
                    ensure_ascii=False,
                    default=self._json_encoder
                )
            return str(file_path)
        except Exception as e:
            raise SerializationError(f"Failed to serialize mapping to {file_path}: {str(e)}")
    
    def serialize_linked_data(
        self,
        linked_data: LinkedData,
        filename: Optional[str] = None
    ) -> str:
        """
        Serialize linked data to JSON file.
        
        Parameters
        ----------
        linked_data : LinkedData
            Linked data object to serialize
        filename : str, optional
            Custom filename. If not provided, generates based on sample ID
            
        Returns
        -------
        str
            Path to the created JSON file
        """
        if filename is None:
            filename = f"{linked_data.sample_id}_linked_data.json"
        
        # Place in appropriate series directory
        series_dir = self.session_dir / linked_data.series_id
        series_dir.mkdir(exist_ok=True)
        file_path = series_dir / filename
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(
                    linked_data.model_dump(),
                    f,
                    indent=2,
                    ensure_ascii=False,
                    default=self._json_encoder
                )
            return str(file_path)
        except Exception as e:
            raise SerializationError(f"Failed to serialize linked data to {file_path}: {str(e)}")
    
    def serialize_agent_result(
        self,
        result: AgentResult,
        filename: Optional[str] = None
    ) -> str:
        """
        Serialize agent result to JSON file.
        
        Parameters
        ----------
        result : AgentResult
            Agent result to serialize
        filename : str, optional
            Custom filename. If not provided, generates based on agent name and timestamp
            
        Returns
        -------
        str
            Path to the created JSON file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{result.agent_name.lower()}_result_{timestamp}.json"
        
        file_path = self.session_dir / filename
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(
                    result.model_dump(),
                    f,
                    indent=2,
                    ensure_ascii=False,
                    default=self._json_encoder
                )
            return str(file_path)
        except Exception as e:
            raise SerializationError(f"Failed to serialize result to {file_path}: {str(e)}")
    
    def serialize_agent_output(
        self,
        output: Union[IngestionOutput, LinkerOutput, CuratorOutput],
        filename: Optional[str] = None
    ) -> str:
        """
        Serialize agent output to JSON file.
        
        Parameters
        ----------
        output : IngestionOutput, LinkerOutput, or CuratorOutput
            Agent output to serialize
        filename : str, optional
            Custom filename. If not provided, generates based on output type
            
        Returns
        -------
        str
            Path to the created JSON file
        """
        if filename is None:
            if isinstance(output, IngestionOutput):
                filename = "ingestion_output.json"
            elif isinstance(output, LinkerOutput):
                filename = "linker_output.json"
            elif isinstance(output, CuratorOutput):
                filename = "curator_output.json"
            else:
                raise SerializationError(f"Unknown output type: {type(output)}")
        
        file_path = self.session_dir / filename
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(
                    output.model_dump(),
                    f,
                    indent=2,
                    ensure_ascii=False,
                    default=self._json_encoder
                )
            return str(file_path)
        except Exception as e:
            raise SerializationError(f"Failed to serialize output to {file_path}: {str(e)}")
    
    def serialize_cleaned_metadata(
        self,
        metadata: Union[GSMMetadata, GSEMetadata, PMIDMetadata],
        original_id: str,
        series_id: Optional[str] = None
    ) -> str:
        """
        Serialize cleaned metadata to the cleaned subdirectory.
        
        Parameters
        ----------
        metadata : GSMMetadata, GSEMetadata, or PMIDMetadata
            Cleaned metadata object
        original_id : str
            Original ID for filename generation
        series_id : str, optional
            Series ID for proper directory placement
            
        Returns
        -------
        str
            Path to the created JSON file
        """
        # Determine base directory
        if series_id:
            base_dir = self.session_dir / series_id
        else:
            base_dir = self.session_dir
        
        cleaned_dir = base_dir / "cleaned"
        cleaned_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        if isinstance(metadata, GSMMetadata):
            filename = f"{original_id}_metadata_cleaned.json"
        elif isinstance(metadata, GSEMetadata):
            filename = f"{original_id}_metadata_cleaned.json"
        elif isinstance(metadata, PMIDMetadata):
            filename = f"PMID_{original_id}_metadata_cleaned.json"
        else:
            raise SerializationError(f"Unknown metadata type: {type(metadata)}")
        
        file_path = cleaned_dir / filename
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(
                    metadata.model_dump(),
                    f,
                    indent=2,
                    ensure_ascii=False,
                    default=self._json_encoder
                )
            return str(file_path)
        except Exception as e:
            raise SerializationError(f"Failed to serialize cleaned metadata to {file_path}: {str(e)}")
    
    @staticmethod
    def _json_encoder(obj):
        """Custom JSON encoder for objects that json.dump can't handle."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Path):
            return str(obj)
        elif hasattr(obj, 'model_dump'):  # Pydantic model
            return obj.model_dump()
        elif hasattr(obj, 'dict'):  # Older Pydantic syntax
            return obj.dict()
        else:
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class WorkflowSerializer:
    """Handles complete workflow serialization."""
    
    def __init__(self, session_dir: Union[str, Path]):
        """
        Initialize workflow serializer.
        
        Parameters
        ----------
        session_dir : str or Path
            Path to the session directory
        """
        self.serializer = ModelSerializer(session_dir)
        self.session_dir = Path(session_dir)
    
    def serialize_ingestion_workflow(
        self,
        output: IngestionOutput,
        metadata_objects: Dict[str, Union[GSMMetadata, GSEMetadata, PMIDMetadata]],
        mapping: Optional[SeriesSampleMapping] = None
    ) -> List[str]:
        """
        Serialize complete ingestion workflow results.
        
        Parameters
        ----------
        output : IngestionOutput
            Ingestion output object
        metadata_objects : Dict
            Dictionary of metadata objects keyed by ID
        mapping : SeriesSampleMapping, optional
            Series-sample mapping object
            
        Returns
        -------
        List[str]
            List of file paths created
        """
        files_created = []
        
        # Serialize main output
        files_created.append(self.serializer.serialize_agent_output(output))
        
        # Serialize individual metadata objects
        for obj_id, metadata in metadata_objects.items():
            files_created.append(self.serializer.serialize_metadata(metadata))
        
        # Serialize mapping if provided
        if mapping:
            files_created.append(self.serializer.serialize_mapping(mapping))
        
        return files_created
    
    def serialize_linker_workflow(
        self,
        output: LinkerOutput,
        linked_data_objects: Dict[str, LinkedData],
        cleaned_metadata: Dict[str, List[Union[GSMMetadata, GSEMetadata, PMIDMetadata]]]
    ) -> List[str]:
        """
        Serialize complete linker workflow results.
        
        Parameters
        ----------
        output : LinkerOutput
            Linker output object
        linked_data_objects : Dict
            Dictionary of linked data objects keyed by sample ID
        cleaned_metadata : Dict
            Dictionary of cleaned metadata objects organized by sample ID
            
        Returns
        -------
        List[str]
            List of file paths created
        """
        files_created = []
        
        # Serialize main output
        files_created.append(self.serializer.serialize_agent_output(output))
        
        # Serialize linked data objects
        for sample_id, linked_data in linked_data_objects.items():
            files_created.append(self.serializer.serialize_linked_data(linked_data))
        
        # Serialize cleaned metadata
        for sample_id, metadata_list in cleaned_metadata.items():
            for metadata in metadata_list:
                if isinstance(metadata, GSMMetadata):
                    # Extract series ID from linked data if available
                    series_id = linked_data_objects.get(sample_id, {}).series_id if sample_id in linked_data_objects else None
                    files_created.append(
                        self.serializer.serialize_cleaned_metadata(metadata, sample_id, series_id)
                    )
                else:
                    files_created.append(
                        self.serializer.serialize_cleaned_metadata(metadata, sample_id)
                    )
        
        return files_created
    
    def serialize_curator_workflow(
        self,
        output: CuratorOutput
    ) -> List[str]:
        """
        Serialize complete curator workflow results.
        
        Parameters
        ----------
        output : CuratorOutput
            Curator output object
            
        Returns
        -------
        List[str]
            List of file paths created
        """
        files_created = []
        
        # Serialize main output
        files_created.append(self.serializer.serialize_agent_output(output))
        
        # Additional curator-specific files could be added here
        # (e.g., detailed candidate analysis, confidence reports, etc.)
        
        return files_created


# Utility functions

def serialize_any_metadata(
    metadata: Union[GSMMetadata, GSEMetadata, PMIDMetadata],
    session_dir: Union[str, Path],
    **kwargs
) -> str:
    """
    Convenience function to serialize any metadata object.
    
    Parameters
    ----------
    metadata : GSMMetadata, GSEMetadata, or PMIDMetadata
        Metadata object to serialize
    session_dir : str or Path
        Session directory path
    **kwargs
        Additional arguments passed to serialize_metadata
        
    Returns
    -------
    str
        Path to created file
    """
    serializer = ModelSerializer(session_dir)
    return serializer.serialize_metadata(metadata, **kwargs)


def load_metadata_from_json(
    file_path: Union[str, Path],
    metadata_type: str
) -> Union[GSMMetadata, GSEMetadata, PMIDMetadata]:
    """
    Load metadata object from JSON file.
    
    Parameters
    ----------
    file_path : str or Path
        Path to JSON file
    metadata_type : str
        Type of metadata ('gsm', 'gse', 'pmid')
        
    Returns
    -------
    GSMMetadata, GSEMetadata, or PMIDMetadata
        Loaded metadata object
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if metadata_type.lower() == 'gsm':
        return GSMMetadata(**data)
    elif metadata_type.lower() == 'gse':
        return GSEMetadata(**data)
    elif metadata_type.lower() == 'pmid':
        return PMIDMetadata(**data)
    else:
        raise ValueError(f"Unknown metadata type: {metadata_type}") 