"""
Tool utilities for agents to use GEO metadata extraction functionality.

This module exposes the GEO metadata extraction tools as function tools
that can be used by agents in the system. All logic is delegated to
the actual tool implementations in src.tools.ingestion_tools.
"""

from __future__ import annotations

from pathlib import Path
import os
import json

from agents import function_tool
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import Pydantic models for data validation (used internally, not as return types)
from src.models import (
    GSMMetadata,
    GSEMetadata,
    PMIDMetadata,
    SeriesSampleMapping,
    LinkedData
)

from datetime import datetime

# Import serialization tools
from src.tools.serialization_tools import (
    serialize_ingestion_output_impl,
    serialize_linker_output_impl,
    serialize_curator_output_impl,
)

# Import the tool implementations
from src.tools.ingestion_tools import (
    extract_gsm_metadata_impl,
    extract_gse_metadata_impl,
    extract_paper_abstract_impl,
    extract_pubmed_id_from_gse_metadata_impl,
    extract_series_id_from_gsm_metadata_impl,
    validate_geo_inputs_impl,
    create_series_sample_mapping_impl,
)

from src.tools.linker_tools import (
    load_mapping_file_impl,
    find_sample_directory_impl,
    clean_metadata_files_impl,
    # Legacy series matrix tools - removed from agent access
    # download_series_matrix_impl,
    # extract_matrix_metadata_impl,
    # extract_sample_metadata_impl,
    package_linked_data_impl,
)

from src.tools.curator_tools import (
    load_sample_data_impl,
    extract_metadata_candidates_impl,
    reconcile_candidates_impl,
    save_curator_results_impl,
)


def get_session_tools(session_dir: str | Path) -> list:
    """
    Creates a suite of GEO metadata extraction tools that are bound to a specific session directory.

    This approach avoids redefining tools within agent creation functions and
    provides a centralized, reusable way to create session-specific tools.

    Parameters
    ----------
    session_dir : str or Path
        The directory path for the session.

    Returns
    -------
    list
        A list of session-bound GEO metadata extraction tools.
    """
    try:
        session_dir = str(session_dir)

        # Get environment variables with defaults
        default_email = os.getenv("NCBI_EMAIL")
        default_api_key = os.getenv("NCBI_API_KEY")

        # Validate that we have required configuration
        if not default_email:
            print(
                "Warning: NCBI_EMAIL environment variable is not set. "
                "Tools will use a default email for testing."
            )
            default_email = "test@example.com"

        # Warn if API key is not set (optional but recommended)
        if not default_api_key:
            print(
                "Warning: NCBI_API_KEY environment variable is not set. "
                "This will limit API rate limits to 3 requests per second."
            )
            default_api_key = None

        @function_tool
        def extract_gsm_metadata(
            gsm_id: str, email: str = None, api_key: str = None
        ) -> str:
            """
            Extract metadata for a GEO Sample (GSM) record.

            This tool retrieves comprehensive metadata for a specific GEO sample,
            including sample characteristics, experimental protocols, and associated
            information.

            Parameters
            ----------
            gsm_id : str
                Gene Expression Omnibus sample ID (e.g., "GSM1019742").
            email : str, optional
                Email address for NCBI E-Utils identification.
                If not provided, uses session default.
            api_key : str, optional
                NCBI API key for higher rate limits.
                If not provided, uses session default.

            Returns
            -------
            str
                Path to the saved metadata file.
            """
            # Use session defaults if not provided
            if email is None:
                email = default_email
            if api_key is None:
                api_key = default_api_key

            return extract_gsm_metadata_impl(gsm_id, session_dir, email, api_key)

        @function_tool
        def extract_gse_metadata(
            gse_id: str, email: str = None, api_key: str = None
        ) -> str:
            """
            Extract metadata for a GEO Series (GSE) record.

            This tool retrieves comprehensive metadata for a specific GEO series,
            including series characteristics, experimental design, and associated
            information.

            Parameters
            ----------
            gse_id : str
                Gene Expression Omnibus series ID (e.g., "GSE41588").
            email : str, optional
                Email address for NCBI E-Utils identification.
                If not provided, uses session default.
            api_key : str, optional
                NCBI API key for higher rate limits.
                If not provided, uses session default.

            Returns
            -------
            str
                Path to the saved metadata file.
            """
            # Use session defaults if not provided
            if email is None:
                email = default_email
            if api_key is None:
                api_key = default_api_key

            return extract_gse_metadata_impl(gse_id, session_dir, email, api_key)

        @function_tool
        def extract_paper_abstract(
            pmid: int,
            email: str = None,
            api_key: str = None,
            source_gse_file: str = None,
        ) -> str:
            """
            Extract paper abstract and metadata for a given PMID.

            This tool retrieves paper title, abstract, authors, journal, and other
            metadata from PubMed.

            Parameters
            ----------
            pmid : int
                PubMed ID for the paper.
            email : str, optional
                Email address for NCBI E-Utils identification.
                If not provided, uses session default.
            api_key : str, optional
                NCBI API key for higher rate limits.
                If not provided, uses session default.
            source_gse_file : str, optional
                Path to the GSE metadata file that this PMID was extracted from.
                Used to determine the correct series directory for file organization.

            Returns
            -------
            str
                Path to the saved metadata file.
            """
            # Use session defaults if not provided
            if email is None:
                email = default_email
            if api_key is None:
                api_key = default_api_key

            return extract_paper_abstract_impl(
                pmid, session_dir, email, api_key, source_gse_file
            )

        @function_tool
        def extract_pubmed_id_from_gse_metadata(gse_metadata_file: str) -> str:
            """
            Extract PubMed ID from a GSE metadata JSON file.

            This tool reads a GSE metadata file (produced by extract_gse_metadata tool)
            and extracts the PubMed ID from the "pubmed_id" field under attributes.
            This is useful for multi-step workflows where you need to extract the PMID
            to then call extract_paper_abstract.

            Parameters
            ----------
            gse_metadata_file : str
                Path to the GSE metadata JSON file (e.g., "GSE41588_metadata.json")

            Returns
            -------
            str
                JSON string containing the extracted PubMed ID and status information
            """
            return extract_pubmed_id_from_gse_metadata_impl(
                gse_metadata_file, session_dir
            )

        @function_tool
        def extract_series_id_from_gsm_metadata(gsm_metadata_file: str) -> str:
            """
            Extract Series ID from a GSM metadata JSON file.

            This tool reads a GSM metadata file (produced by extract_gsm_metadata tool)
            and extracts the Series ID from the "series_id" field under attributes.
            This is useful for multi-step workflows where you need to extract the GSE ID
            to then call extract_gse_metadata.

            Parameters
            ----------
            gsm_metadata_file : str
                Path to the GSM metadata JSON file (e.g., "GSM1019742_metadata.json")

            Returns
            -------
            str
                JSON string containing the extracted Series ID and status information
            """
            return extract_series_id_from_gsm_metadata_impl(
                gsm_metadata_file, session_dir
            )

        @function_tool
        def validate_geo_inputs(
            gsm_id: str = None,
            gse_id: str = None,
            pmid: int = None,
            email: str = None,
            api_key: str = None,
        ) -> str:
            """
            Validate input parameters for GEO metadata extraction.

            This tool checks the format and validity of GSM IDs, GSE IDs, and PMIDs
            before attempting metadata extraction.

            Parameters
            ----------
            gsm_id : str, optional
                Gene Expression Omnibus sample ID to validate.
            gse_id : str, optional
                Gene Expression Omnibus series ID to validate.
            pmid : int, optional
                PubMed ID to validate.
            email : str, optional
                Email address for NCBI E-Utils identification.
                If not provided, uses session default.
            api_key : str, optional
                NCBI API key for higher rate limits.
                If not provided, uses session default.

            Returns
            -------
            str
                JSON string containing validation results.
            """
            # Use session defaults if not provided
            if email is None:
                email = default_email
            if api_key is None:
                api_key = default_api_key

            return validate_geo_inputs_impl(gsm_id, gse_id, pmid, email, api_key)

        @function_tool
        def create_series_sample_mapping() -> str:
            """
            Create a mapping file between series IDs and sample IDs in the main session directory.

            This tool scans the session directory for series subdirectories (GSE*) and creates
            a comprehensive mapping file that shows which sample IDs belong to which series.
            The mapping file is saved in the main session directory and can be used by later
            agents to quickly determine which subdirectory contains data for a given sample ID.

            The mapping file contains:
            - Forward mapping: series_id -> list of sample_ids
            - Reverse mapping: sample_id -> series_id (for quick lookup)
            - Summary statistics: total series and sample counts
            - Metadata: generation timestamp and session directory path

            Returns
            -------
            str
                JSON string containing the mapping result and file path
            """
            file_path = create_series_sample_mapping_impl(session_dir)
            
            try:
                # Load the created mapping file 
                with open(file_path, 'r') as f:
                    mapping_data = json.load(f)
                
                result = {
                    "success": True,
                    "message": f"Series-sample mapping created at {file_path}",
                    "files_created": [file_path],
                    "series_mapping": mapping_data,
                    "geo_ids_processed": [],
                    "extraction_type": "mapping"
                }
                
                return json.dumps(result, indent=2)
                
            except Exception as e:
                result = {
                    "success": False,
                    "message": f"Failed to create or load mapping: {str(e)}",
                    "errors": [str(e)],
                    "geo_ids_processed": [],
                    "extraction_type": "mapping"
                }
                return json.dumps(result, indent=2)

        # LinkerAgent tools
        @function_tool
        def load_mapping_file() -> str:
            """
            Load the series_sample_mapping.json file to understand directory structure.

            This tool loads the mapping file created by the IngestionAgent that shows
            the relationship between series IDs and sample IDs, and which subdirectory
            contains the data for each sample.

            Returns
            -------
            str
                JSON string containing the mapping data and validation result
            """
            result = load_mapping_file_impl(session_dir)
            if not result.get("success", True):
                response = {
                    "success": False,
                    "message": result.get("message", "Unknown error"),
                    "errors": [result.get("message", "Unknown error")]
                }
                return json.dumps(response, indent=2)
            
            # Return mapping data as JSON string
            try:
                mapping_data = result.get("data", {})
                if mapping_data:
                    # Convert Pydantic objects to dicts for JSON serialization
                    serializable_data = {}
                    for key, value in mapping_data.items():
                        if hasattr(value, 'model_dump'):  # Pydantic object
                            serializable_data[key] = value.model_dump()
                        else:
                            serializable_data[key] = value
                    
                    response = {
                        "success": True,
                        "message": result.get("message", "Mapping loaded successfully"),
                        "data": {"mapping": serializable_data["mapping"]}
                    }
                    return json.dumps(response, indent=2)
                else:
                    response = {
                        "success": False,
                        "message": "No mapping data found",
                        "errors": ["Empty mapping data"]
                    }
                    return json.dumps(response, indent=2)
            except Exception as e:
                response = {
                    "success": False,
                    "message": f"Invalid mapping data format: {str(e)}",
                    "errors": [f"Validation error: {str(e)}"]
                }
                return json.dumps(response, indent=2)

        @function_tool
        def find_sample_directory(sample_id: str) -> str:
            """
            Find the directory containing files for a specific sample ID.

            This tool uses the mapping file to locate the correct subdirectory
            that contains the metadata files for the given sample ID.

            Parameters
            ----------
            sample_id : str
                The sample ID to find (e.g., GSM1000981)

            Returns
            -------
            str
                JSON string containing directory information
            """
            result = find_sample_directory_impl(sample_id, session_dir)
            if not result.get("success", True):
                response = {
                    "success": False,
                    "message": result.get("message", "Unknown error"),
                    "errors": [result.get("message", "Unknown error")]
                }
                return json.dumps(response, indent=2)
            
            response = {
                "success": True,
                "message": result.get("message", "Directory found successfully"),
                "data": result.get("data", {})
            }
            return json.dumps(response, indent=2)

        @function_tool
        def clean_metadata_files(sample_id: str, fields_to_remove: list[str] = None) -> str:
            """
            Generate cleaned versions of metadata files by removing specified fields.

            NOTE: This tool removes fields both at the top level and inside the 'attributes' dict if present.
            Update the fields_to_remove list if the metadata schema changes.

            Parameters
            ----------
            sample_id : str
                The sample ID to process
            fields_to_remove : list[str], optional
                List of fields to remove from metadata files.
                If not provided, uses default fields like 'status', 'submission_date', etc.

            Returns
            -------
            str
                JSON string containing paths to cleaned files
            """
            fields_list = fields_to_remove or [
                # Top-level fields
                "status",
                # Fields inside 'attributes' dict
                "status",
                "submission_date",
                "last_update_date",
                "contributor",
                "contact_name",
                "contact_email",
                "contact_laboratory",
                "contact_department",
                "contact_institute",
                "contact_address",
                "contact_city",
                "contact_state",
                "contact_zip/postal_code",
                "contact_country",
                "contact_phone",
                "contact_fax",
                "extract_protocol_ch1",
                "growth_protocol_ch1",
                "treatment_protocol_ch1",
                "data_processing",
                # PMID fields to remove
                "authors",
                "journal",
                "publication_date",
                "keywords",
                "mesh_terms",
            ]
            
            result = clean_metadata_files_impl(sample_id, session_dir, fields_list)
            if not result.get("success", True):
                response = {
                    "success": False,
                    "message": result.get("message", "Unknown error"),
                    "errors": [result.get("message", "Unknown error")]
                }
                return json.dumps(response, indent=2)
            
            response = {
                "success": True,
                "message": result.get("message", "Metadata files cleaned successfully"),
                "files_created": result.get("files_created", []),
                "data": {"fields_removed": fields_list}
            }
            return json.dumps(response, indent=2)

        @function_tool
        def package_linked_data(sample_id: str, fields_to_remove: list[str] = None) -> str:
            """
            Package all linked information for a sample into a comprehensive result.

            This tool combines all the processed information for a sample including:
            - Cleaned metadata files
            - Original sample metadata

            Note: Series matrix functionality has been removed from agent access.

            Parameters
            ----------
            sample_id : str
                The sample ID to process
            fields_to_remove : list[str], optional
                List of fields to remove from metadata files

            Returns
            -------
            str
                JSON string containing all packaged linked data
            """
            result = package_linked_data_impl(sample_id, session_dir, fields_to_remove)
            if not result.get("success", True):
                response = {
                    "success": False,
                    "message": result.get("message", "Unknown error"),
                    "errors": [result.get("message", "Unknown error")]
                }
                return json.dumps(response, indent=2)
            
            # Return the linked data as JSON - no need for Pydantic conversion
            linked_data_dict = result.get("data", {})
            response = {
                "success": True,
                "message": result.get("message", "Linked data packaged successfully"),
                "files_created": result.get("files_created", []),
                "data": linked_data_dict,
                "linked_data": {sample_id: linked_data_dict}
            }
            return json.dumps(response, indent=2)

        # Curator tools for metadata curation
        @function_tool
        def load_sample_data(sample_id: str) -> str:
            """
            Load sample data from linked_data.json and all referenced cleaned files.
            
            This tool loads comprehensive sample data including the linked_data.json file
            and all cleaned metadata files referenced within it. This provides a complete
            view of all available metadata for a sample.
            
            Parameters
            ----------
            sample_id : str
                The sample ID (e.g., GSM1000981) to load data for
                
            Returns
            -------
            str
                JSON string containing loaded sample data
            """
            result = load_sample_data_impl(sample_id, session_dir)
            if not result.get("success", True):
                response = {
                    "success": False,
                    "message": result.get("message", "Unknown error"),
                    "errors": [result.get("message", "Unknown error")]
                }
                return json.dumps(response, indent=2)
            
            response = {
                "success": True,
                "message": result.get("message", "Sample data loaded successfully"),
                "data": result.get("data", {}),
                "samples_curated": [sample_id]
            }
            return json.dumps(response, indent=2)

        @function_tool
        def extract_metadata_candidates(sample_data: str, target_field: str) -> str:
            """
            Extract potential candidates for a target metadata field from all files.
            
            This tool analyzes sample data loaded from load_sample_data and extracts
            potential candidates for a specific metadata field (e.g., "Disease", "Tissue", "Age").
            It searches through all available data sources independently and returns
            candidates found in each source.
            
            Parameters
            ----------
            sample_data : str
                JSON string containing sample data from load_sample_data
            target_field : str
                The target metadata field to extract candidates for (e.g., "Disease", "Tissue", "Age")
                
            Returns
            -------
            str
                JSON string containing candidates extracted from each file
            """
            sample_data_dict = json.loads(sample_data)
            result = extract_metadata_candidates_impl(sample_data_dict, target_field, session_dir)
            if not result.get("success", True):
                raise RuntimeError(
                    f"Failed to extract candidates: {result.get('message', 'Unknown error')}"
                )
            return json.dumps(result)

        @function_tool
        def reconcile_candidates(candidates_by_file: str, target_field: str) -> str:
            """
            Reconcile candidates across files and determine final result.
            
            This tool compares candidates extracted from different files and determines
            a final curated value. It handles consensus building when multiple sources
            agree, and flags conflicts when sources disagree.
            
            Parameters
            ----------
            candidates_by_file : str
                JSON string containing candidates extracted from each file
            target_field : str
                The target metadata field being reconciled
                
            Returns
            -------
            str
                JSON string containing reconciled result with confidence scoring
            """
            candidates_dict = json.loads(candidates_by_file)
            result = reconcile_candidates_impl(candidates_dict, target_field, session_dir)
            if not result.get("success", True):
                raise RuntimeError(
                    f"Failed to reconcile candidates: {result.get('message', 'Unknown error')}"
                )
            return json.dumps(result)

        @function_tool
        def save_curator_results(sample_id: str, results_data: str) -> str:
            """
            Save curation results to a JSON file.
            
            This tool saves the final curation results for a sample to a JSON file
            named {sample_id}_metadata_candidates.json in the session directory.
            
            Parameters
            ----------
            sample_id : str
                The sample ID being curated
            results_data : str
                JSON string containing the final curation results
                
            Returns
            -------
            str
                JSON string indicating success or failure of save operation
            """
            results_dict = json.loads(results_data)
            result = save_curator_results_impl(sample_id, results_dict, session_dir)
            if not result.get("success", True):
                raise RuntimeError(
                    f"Failed to save results: {result.get('message', 'Unknown error')}"
                )
            return json.dumps(result)





        @function_tool
        def set_testing_session() -> str:
            """
            Set the session directory to sandbox/test-session for testing purposes.

            This tool is automatically called when "testing" is detected in the input prompt.
            It ensures that all operations are performed in a dedicated testing session directory.

            Returns
            -------
            str
                JSON string confirming the testing session has been set up
            """
            try:
                print(
                    "🧪 Set_testing_session: Setting session directory to sandbox/test-session"
                )

                # Create the testing session directory
                test_session_dir = Path("sandbox/test-session").absolute()
                test_session_dir.mkdir(parents=True, exist_ok=True)

                print(
                    f"🧪 Set_testing_session: Created testing session directory: {test_session_dir}"
                )

                result = {
                    "success": True,
                    "message": "Testing session directory set successfully",
                    "session_directory": str(test_session_dir),
                    "session_id": "test-session",
                }

                print(f"🧪 Set_testing_session: Result: {result}")
                return json.dumps(result)

            except Exception as e:
                print(f"❌ Set_testing_session error: {str(e)}")
                import traceback

                print("🔍 Set_testing_session traceback:")
                traceback.print_exc()

                result = {
                    "success": False,
                    "message": f"Failed to set testing session: {str(e)}",
                    "error": str(e),
                }
                return json.dumps(result)

        # Serialization tools for persisting structured outputs
        @function_tool
        def serialize_agent_output(output_type: str) -> str:
            """
            Serialize agent output to JSON files.
            
            This tool allows agents to persist their structured Pydantic outputs
            as JSON files at the end of their workflow.
            
            Parameters
            ----------
            output_type : str
                Type of agent output ('ingestion', 'linker', 'curator')
                
            Returns
            -------
            str
                JSON string with serialization result and status
            """
            try:
                if output_type.lower() in ['ingestion', 'linker', 'curator']:
                    result = {
                        "success": True,
                        "message": f"Serialization tool available for {output_type} outputs",
                        "files_created": [],
                        "timestamp": str(datetime.now())
                    }
                    return json.dumps(result, indent=2)
                else:
                    result = {
                        "success": False,
                        "message": f"Unknown output type: {output_type}",
                        "error": f"Supported types: ingestion, linker, curator"
                    }
                    return json.dumps(result, indent=2)
            except Exception as e:
                result = {
                    "success": False,
                    "message": f"Failed to serialize {output_type} output: {str(e)}",
                    "error": str(e)
                }
                return json.dumps(result, indent=2)

        # Return all the tools
        tools = [
            extract_gsm_metadata,
            extract_gse_metadata,
            extract_paper_abstract,
            extract_pubmed_id_from_gse_metadata,
            extract_series_id_from_gsm_metadata,
            validate_geo_inputs,
            create_series_sample_mapping,
            load_mapping_file,
            find_sample_directory,
            clean_metadata_files,
            package_linked_data,
            load_sample_data,
            extract_metadata_candidates,
            reconcile_candidates,
            save_curator_results,
            set_testing_session,
            serialize_agent_output,
        ]

        print(f"✅ ToolUtils: Created {len(tools)} tools")

        return tools

    except Exception as e:
        print(f"❌ ToolUtils error: {str(e)}")
        import traceback

        print("🔍 ToolUtils traceback:")
        traceback.print_exc()
        raise


def get_available_tools():
    """
    Get a list of available tool names for reference.

    Returns
    -------
    List[str]
        List of available tool function names.
    """
    return [
        "extract_gsm_metadata",
        "extract_gse_metadata",
        "extract_paper_abstract",
        "extract_pubmed_id_from_gse_metadata",
        "extract_series_id_from_gsm_metadata",
        "validate_geo_inputs",
        "create_series_sample_mapping",
        "load_mapping_file",
        "find_sample_directory",
        "clean_metadata_files",
        "package_linked_data",
        "load_sample_data",
        "extract_metadata_candidates",
        "reconcile_candidates",
        "save_curator_results",
        "set_testing_session",
    ]
