"""
Serialization tools for agents to persist structured outputs as JSON files.

This module provides tools that agents can call at the end of their workflows
to convert their Pydantic output objects into JSON files, maintaining the
existing file structure while leveraging the benefits of typed objects.
"""

from pathlib import Path
from typing import Dict, List, Any, Union
from datetime import datetime

from src.models import (
    IngestionOutput,
    LinkerOutput,
    CuratorOutput,
    WorkflowSerializer,
    ModelSerializer,
    GSMMetadata,
    GSEMetadata,
    PMIDMetadata,
    SeriesSampleMapping,
    LinkedData,
)


class SerializationTools:
    """Tools for persisting agent outputs as JSON files."""

    def __init__(self, session_dir: str):
        """
        Initialize serialization tools.

        Parameters
        ----------
        session_dir : str
            Path to the session directory
        """
        self.session_dir = Path(session_dir)
        self.workflow_serializer = WorkflowSerializer(session_dir)
        self.model_serializer = ModelSerializer(session_dir)

    def serialize_ingestion_output(
        self,
        output: IngestionOutput,
        metadata_objects: Dict[
            str, Union[GSMMetadata, GSEMetadata, PMIDMetadata]
        ] = None,
        mapping: SeriesSampleMapping = None,
    ) -> Dict[str, Any]:
        """
        Serialize IngestionAgent output to JSON files.

        Parameters
        ----------
        output : IngestionOutput
            The structured output from IngestionAgent
        metadata_objects : Dict, optional
            Dictionary of metadata objects keyed by ID
        mapping : SeriesSampleMapping, optional
            Series-sample mapping object

        Returns
        -------
        Dict[str, Any]
            Result with success status and files created
        """
        try:
            files_created = []

            # Serialize the main output
            output_file = self.model_serializer.serialize_agent_output(output)
            files_created.append(output_file)

            # Convert raw metadata to typed objects and serialize
            if metadata_objects:
                for obj_id, metadata in metadata_objects.items():
                    metadata_file = self.model_serializer.serialize_metadata(metadata)
                    files_created.append(metadata_file)

            # Serialize mapping if provided
            if mapping:
                mapping_file = self.model_serializer.serialize_mapping(mapping)
                files_created.append(mapping_file)

            return {
                "success": True,
                "message": f"Serialized IngestionAgent output to {len(files_created)} files",
                "files_created": files_created,
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to serialize IngestionAgent output: {str(e)}",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }

    def serialize_linker_output(
        self,
        output: LinkerOutput,
        linked_data_objects: Dict[str, LinkedData] = None,
        cleaned_metadata: Dict[
            str, List[Union[GSMMetadata, GSEMetadata, PMIDMetadata]]
        ] = None,
    ) -> Dict[str, Any]:
        """
        Serialize LinkerAgent output to JSON files.

        Parameters
        ----------
        output : LinkerOutput
            The structured output from LinkerAgent
        linked_data_objects : Dict, optional
            Dictionary of linked data objects keyed by sample ID
        cleaned_metadata : Dict, optional
            Dictionary of cleaned metadata objects organized by sample ID

        Returns
        -------
        Dict[str, Any]
            Result with success status and files created
        """
        try:
            files_created = []

            # Serialize the main output
            output_file = self.model_serializer.serialize_agent_output(output)
            files_created.append(output_file)

            # Serialize linked data objects
            if linked_data_objects:
                for sample_id, linked_data in linked_data_objects.items():
                    linked_file = self.model_serializer.serialize_linked_data(
                        linked_data
                    )
                    files_created.append(linked_file)

            # Serialize cleaned metadata
            if cleaned_metadata:
                for sample_id, metadata_list in cleaned_metadata.items():
                    for metadata in metadata_list:
                        # Extract series ID if available
                        series_id = None
                        if linked_data_objects and sample_id in linked_data_objects:
                            series_id = linked_data_objects[sample_id].series_id

                        cleaned_file = self.model_serializer.serialize_cleaned_metadata(
                            metadata, sample_id, series_id
                        )
                        files_created.append(cleaned_file)

            return {
                "success": True,
                "message": f"Serialized LinkerAgent output to {len(files_created)} files",
                "files_created": files_created,
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to serialize LinkerAgent output: {str(e)}",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }

    def serialize_curator_output(self, output: CuratorOutput) -> Dict[str, Any]:
        """
        Serialize CuratorAgent output to JSON files.

        Parameters
        ----------
        output : CuratorOutput
            The structured output from CuratorAgent

        Returns
        -------
        Dict[str, Any]
            Result with success status and files created
        """
        try:
            files_created = []

            # Serialize the main output
            output_file = self.model_serializer.serialize_agent_output(output)
            files_created.append(output_file)

            # Additional curator-specific serialization could be added here
            # (e.g., detailed reports, confidence analyses, etc.)

            return {
                "success": True,
                "message": f"Serialized CuratorAgent output to {len(files_created)} files",
                "files_created": files_created,
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to serialize CuratorAgent output: {str(e)}",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }


# Implementation functions for tool_utils.py


def serialize_ingestion_output_impl(
    session_dir: str,
    output_data: Dict[str, Any],
    metadata_objects: Dict[str, Dict[str, Any]] = None,
    mapping_data: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    Serialize IngestionAgent output implementation.

    Parameters
    ----------
    session_dir : str
        Session directory path
    output_data : Dict[str, Any]
        Raw output data to convert to IngestionOutput
    metadata_objects : Dict, optional
        Raw metadata objects to convert to typed objects
    mapping_data : Dict, optional
        Raw mapping data to convert to SeriesSampleMapping

    Returns
    -------
    Dict[str, Any]
        Serialization result
    """
    try:
        tools = SerializationTools(session_dir)

        # Convert raw data to Pydantic objects
        output = IngestionOutput(**output_data)

        typed_metadata = {}
        if metadata_objects:
            for obj_id, metadata_dict in metadata_objects.items():
                if obj_id.startswith("GSM"):
                    typed_metadata[obj_id] = GSMMetadata(**metadata_dict)
                elif obj_id.startswith("GSE"):
                    typed_metadata[obj_id] = GSEMetadata(**metadata_dict)
                elif obj_id.startswith("PMID") or "pmid" in metadata_dict:
                    typed_metadata[obj_id] = PMIDMetadata(**metadata_dict)

        typed_mapping = None
        if mapping_data:
            typed_mapping = SeriesSampleMapping(**mapping_data)

        return tools.serialize_ingestion_output(output, typed_metadata, typed_mapping)

    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to serialize ingestion output: {str(e)}",
            "error": str(e),
        }


def serialize_linker_output_impl(
    session_dir: str,
    output_data: Dict[str, Any],
    linked_data_objects: Dict[str, Dict[str, Any]] = None,
    cleaned_metadata: Dict[str, List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Serialize LinkerAgent output implementation.

    Parameters
    ----------
    session_dir : str
        Session directory path
    output_data : Dict[str, Any]
        Raw output data to convert to LinkerOutput
    linked_data_objects : Dict, optional
        Raw linked data objects to convert to typed objects
    cleaned_metadata : Dict, optional
        Raw cleaned metadata to convert to typed objects

    Returns
    -------
    Dict[str, Any]
        Serialization result
    """
    try:
        tools = SerializationTools(session_dir)

        # Convert raw data to Pydantic objects
        output = LinkerOutput(**output_data)

        typed_linked_data = {}
        if linked_data_objects:
            for sample_id, linked_dict in linked_data_objects.items():
                typed_linked_data[sample_id] = LinkedData(**linked_dict)

        typed_cleaned_metadata = {}
        if cleaned_metadata:
            for sample_id, metadata_list in cleaned_metadata.items():
                typed_list = []
                for metadata_dict in metadata_list:
                    if "gsm_id" in metadata_dict:
                        typed_list.append(GSMMetadata(**metadata_dict))
                    elif "gse_id" in metadata_dict:
                        typed_list.append(GSEMetadata(**metadata_dict))
                    elif "pmid" in metadata_dict:
                        typed_list.append(PMIDMetadata(**metadata_dict))
                typed_cleaned_metadata[sample_id] = typed_list

        return tools.serialize_linker_output(
            output, typed_linked_data, typed_cleaned_metadata
        )

    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to serialize linker output: {str(e)}",
            "error": str(e),
        }


def serialize_curator_output_impl(
    session_dir: str, output_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Serialize CuratorAgent output implementation.

    Parameters
    ----------
    session_dir : str
        Session directory path
    output_data : Dict[str, Any]
        Raw output data to convert to CuratorOutput

    Returns
    -------
    Dict[str, Any]
        Serialization result
    """
    try:
        tools = SerializationTools(session_dir)

        # Convert raw data to Pydantic object
        output = CuratorOutput(**output_data)

        return tools.serialize_curator_output(output)

    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to serialize curator output: {str(e)}",
            "error": str(e),
        }
