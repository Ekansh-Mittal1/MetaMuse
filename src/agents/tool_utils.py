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

from datetime import datetime

# Import serialization tools

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
    process_multiple_samples_impl,
    create_curation_data_package_impl,
    # Legacy series matrix tools - removed from agent access
    # download_series_matrix_impl,
    # extract_matrix_metadata_impl,
    # extract_sample_metadata_impl,
    package_linked_data_impl,
)

from src.tools.curator_tools import (
    save_curation_results_impl,
    load_curation_data_for_samples_impl,
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
                with open(file_path, "r") as f:
                    mapping_data = json.load(f)

                result = {
                    "success": True,
                    "message": f"Series-sample mapping created at {file_path}",
                    "files_created": [file_path],
                    "series_mapping": mapping_data,
                    "geo_ids_processed": [],
                    "extraction_type": "mapping",
                }

                return json.dumps(result, indent=2)

            except Exception as e:
                result = {
                    "success": False,
                    "message": f"Failed to create or load mapping: {str(e)}",
                    "errors": [str(e)],
                    "geo_ids_processed": [],
                    "extraction_type": "mapping",
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
                    "errors": [result.get("message", "Unknown error")],
                }
                return json.dumps(response, indent=2)

            # Return mapping data as JSON string
            try:
                mapping_data = result.get("data", {})
                if mapping_data:
                    # Convert Pydantic objects to dicts for JSON serialization
                    serializable_data = {}
                    for key, value in mapping_data.items():
                        if hasattr(value, "model_dump"):  # Pydantic object
                            serializable_data[key] = value.model_dump()
                        else:
                            serializable_data[key] = value

                    response = {
                        "success": True,
                        "message": result.get("message", "Mapping loaded successfully"),
                        "data": {"mapping": serializable_data["mapping"]},
                    }
                    return json.dumps(response, indent=2)
                else:
                    response = {
                        "success": False,
                        "message": "No mapping data found",
                        "errors": ["Empty mapping data"],
                    }
                    return json.dumps(response, indent=2)
            except Exception as e:
                response = {
                    "success": False,
                    "message": f"Invalid mapping data format: {str(e)}",
                    "errors": [f"Validation error: {str(e)}"],
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
                    "errors": [result.get("message", "Unknown error")],
                }
                return json.dumps(response, indent=2)

            response = {
                "success": True,
                "message": result.get("message", "Directory found successfully"),
                "data": result.get("data", {}),
            }
            return json.dumps(response, indent=2)

        @function_tool
        def clean_metadata_files(
            sample_id: str, fields_to_remove: list[str] = None
        ) -> str:
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
                    "errors": [result.get("message", "Unknown error")],
                }
                return json.dumps(response, indent=2)

            response = {
                "success": True,
                "message": result.get("message", "Metadata files cleaned successfully"),
                "files_created": result.get("files_created", []),
                "data": {"fields_removed": fields_list},
            }
            return json.dumps(response, indent=2)

        @function_tool
        def package_linked_data(
            sample_id: str, fields_to_remove: list[str] = None
        ) -> str:
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
                    "errors": [result.get("message", "Unknown error")],
                }
                return json.dumps(response, indent=2)

            # Return the linked data as JSON - no need for Pydantic conversion
            linked_data_dict = result.get("data", {})
            response = {
                "success": True,
                "message": result.get("message", "Linked data packaged successfully"),
                "files_created": result.get("files_created", []),
                "data": linked_data_dict,
                "linked_data": {sample_id: linked_data_dict},
            }
            return json.dumps(response, indent=2)

        @function_tool
        def create_curation_data_package(
            sample_id: str, fields_to_remove: list[str] = None
        ) -> str:
            """
            Create a CurationDataPackage with cleaned metadata from all sources.

            This tool processes a sample ID to create a comprehensive data package
            containing cleaned metadata from series, sample, and abstract sources.

            Parameters
            ----------
            sample_id : str
                The sample ID to process (e.g., "GSM1000981")
            fields_to_remove : list[str], optional
                List of fields to remove from metadata files

            Returns
            -------
            str
                JSON string containing CurationDataPackage object
            """
            result = create_curation_data_package_impl(
                sample_id, session_dir, fields_to_remove
            )
            if not result.get("success", True):
                response = {
                    "success": False,
                    "message": result.get("message", "Unknown error"),
                    "errors": [result.get("message", "Unknown error")],
                }
                return json.dumps(response, indent=2)

            # Extract the CurationDataPackage from the result
            curation_package = result.get("data", {}).get("curation_package")
            if curation_package:
                response = {
                    "success": True,
                    "message": result.get(
                        "message", "Curation data package created successfully"
                    ),
                    "curation_package": curation_package.model_dump()
                    if hasattr(curation_package, "model_dump")
                    else curation_package,
                    "files_created": result.get("files_created", []),
                }
            else:
                response = {
                    "success": False,
                    "message": "No curation package found in result",
                    "errors": ["Missing curation package data"],
                }
            return json.dumps(response, indent=2)

        @function_tool
        def process_multiple_samples(
            sample_ids: list[str], fields_to_remove: list[str] = None
        ) -> str:
            """
            Process multiple sample IDs at once (clean and package for all samples).

            This tool processes multiple samples efficiently by cleaning metadata files
            and packaging linked data for each sample in the list.

            Parameters
            ----------
            sample_ids : list[str]
                List of sample IDs to process (e.g., ["GSM1000981", "GSM1002543"])
            fields_to_remove : list[str], optional
                List of fields to remove from metadata files

            Returns
            -------
            str
                JSON string containing processing summary for all samples
            """
            result = process_multiple_samples_impl(
                sample_ids, session_dir, fields_to_remove
            )
            if not result.get("success", True):
                response = {
                    "success": False,
                    "message": result.get("message", "Unknown error"),
                    "errors": [result.get("message", "Unknown error")],
                }
                return json.dumps(response, indent=2)

            response = {
                "success": True,
                "message": result.get(
                    "message", "Multiple samples processed successfully"
                ),
                "files_created": result.get("files_created", []),
                "data": result.get("data", {}),
                "summary": result.get("data", {}).get("summary", {}),
            }
            return json.dumps(response, indent=2)

        # Curator tools for metadata curation
        @function_tool
        def save_curation_results(curation_results_json: str) -> str:
            """
            Save curation results to a file.

            This tool saves the curation results to a JSON file in the session directory.

            Parameters
            ----------
            curation_results_json : str
                JSON string containing the curation results to save.

            Returns
            -------
            str
                JSON string containing the save operation result.
            """
            print("🔧 CURATOR TOOL: save_curation_results")

            try:
                # Parse the JSON string to get the list of curation results
                import json
                from src.models.curation_models import CurationResult

                print("🔍 DEBUG: Attempting to parse JSON...")

                # Clean the JSON string to handle potential issues
                cleaned_json = curation_results_json.strip()

                # Try to extract valid JSON if there's extra data
                # Look for the main JSON array/object structure
                if cleaned_json.startswith("[") and "}]" in cleaned_json:
                    # Find the end of the JSON array
                    end_pos = cleaned_json.rfind("}]") + 2
                    cleaned_json = cleaned_json[:end_pos]
                    print(
                        f"🔍 DEBUG: Cleaned JSON to remove extra data, new length: {len(cleaned_json)}"
                    )
                elif cleaned_json.startswith("{") and cleaned_json.count("{") > 0:
                    # For single objects, find the matching closing brace
                    brace_count = 0
                    end_pos = 0
                    for i, char in enumerate(cleaned_json):
                        if char == "{":
                            brace_count += 1
                        elif char == "}":
                            brace_count -= 1
                            if brace_count == 0:
                                end_pos = i + 1
                                break
                    if end_pos > 0:
                        cleaned_json = cleaned_json[:end_pos]
                        print(
                            f"🔍 DEBUG: Cleaned JSON object, new length: {len(cleaned_json)}"
                        )

                print(f"🔍 DEBUG: Final cleaned JSON length: {len(cleaned_json)}")

                curation_data_list = json.loads(cleaned_json)
                print(
                    f"🔍 DEBUG: Successfully parsed JSON. Type: {type(curation_data_list)}, Length: {len(curation_data_list) if isinstance(curation_data_list, list) else 'N/A'}"
                )

                curation_results = []

                # Handle both single objects and lists
                if isinstance(curation_data_list, dict):
                    print("🔍 DEBUG: Converting single dict to list")
                    curation_data_list = [curation_data_list]

                for i, curation_data in enumerate(curation_data_list):
                    print(f"🔍 DEBUG: Processing item {i}: {type(curation_data)}")
                    curation_result = CurationResult(**curation_data)
                    curation_results.append(curation_result)

                result = save_curation_results_impl(curation_results, session_dir)
                print(f"✅ Saved curation results for {len(curation_results)} samples")
                return json.dumps(result, indent=2)

            except json.JSONDecodeError as e:
                print(f"🔍 DEBUG: JSON parsing error at position {e.pos}: {e.msg}")
                start_pos = max(0, e.pos - 50)
                end_pos = min(len(curation_results_json), e.pos + 50)
                context = curation_results_json[start_pos:end_pos]
                print(f"🔍 DEBUG: Context around error: {repr(context)}")
                result = {
                    "success": False,
                    "message": f"Error saving curation results: {str(e)}",
                    "error": str(e),
                    "debug_info": {
                        "json_length": len(curation_results_json),
                        "error_position": e.pos,
                        "error_message": e.msg,
                    },
                }
                return json.dumps(result, indent=2)
            except Exception as e:
                print(f"🔍 DEBUG: Other error: {type(e).__name__}: {str(e)}")
                result = {
                    "success": False,
                    "message": f"Error saving curation results: {str(e)}",
                    "error": str(e),
                }
                return json.dumps(result, indent=2)

        @function_tool
        def load_curation_data_for_samples(sample_ids_json: str) -> str:
            """
            Load curation data for multiple samples from the session directory.

            This tool loads the necessary data for curation from the session directory
            when using SimpleCuratorHandoff (which doesn't include the full data).

            Parameters
            ----------
            sample_ids_json : str
                JSON string containing a list of sample IDs to load data for

            Returns
            -------
            str
                JSON string with curation packages data
            """
            try:
                import json as json_module

                sample_ids = json_module.loads(sample_ids_json)
                result = load_curation_data_for_samples_impl(sample_ids, session_dir)
                return json.dumps(result, indent=2)
            except Exception as e:
                response = {
                    "success": False,
                    "message": f"Error loading curation data: {str(e)}",
                    "error": str(e),
                }
                return json.dumps(response, indent=2)

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
                Type of agent output ('ingestion', 'linker', 'curator') or format ('json', 'csv')

            Returns
            -------
            str
                JSON string with serialization result and status
            """
            print(f"🔧 TOOL: serialize_agent_output - output_type: {output_type}")
            print(f"🔍 DEBUG: Received output_type: {repr(output_type)}")
            print(
                "🔍 DEBUG: Expected types: ['ingestion', 'linker', 'curator', 'json', 'csv']"
            )

            try:
                if output_type.lower() in ["ingestion", "linker", "curator"]:
                    result = {
                        "success": True,
                        "message": f"Serialization tool available for {output_type} outputs",
                        "files_created": [],
                        "timestamp": str(datetime.now()),
                    }
                    return json.dumps(result, indent=2)
                elif output_type.lower() in ["json", "csv"]:
                    # Handle format-based serialization requests
                    result = {
                        "success": True,
                        "message": f"Agent output serialized in {output_type.upper()} format",
                        "format": output_type.lower(),
                        "timestamp": str(datetime.now()),
                        "notes": "Output has been processed and is available in the session directory",
                    }
                    return json.dumps(result, indent=2)
                else:
                    print(
                        f"🔍 DEBUG: output_type '{output_type}' not in supported types"
                    )
                    result = {
                        "success": False,
                        "message": f"Unknown output type: {output_type}",
                        "error": "Supported types: ingestion, linker, curator, json, csv",
                        "debug_info": {
                            "received_type": output_type,
                            "supported_types": [
                                "ingestion",
                                "linker",
                                "curator",
                                "json",
                                "csv",
                            ],
                        },
                    }
                    return json.dumps(result, indent=2)
            except Exception as e:
                print(
                    f"🔍 DEBUG: Error in serialize_agent_output: {type(e).__name__}: {str(e)}"
                )
                result = {
                    "success": False,
                    "message": f"Error in serialization: {str(e)}",
                    "error": str(e),
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
            create_curation_data_package,
            process_multiple_samples,
            save_curation_results,
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
        "create_curation_data_package",
        "process_multiple_samples",
        "save_curation_results",
        "set_testing_session",
    ]


def get_curator_tools(session_dir: str | Path) -> list:
    """
    Creates a suite of curation-specific tools that are bound to a specific session directory.

    These tools are specifically for the CuratorAgent and do not include data intake tools
    since the data should already be available from the data_intake workflow.

    Parameters
    ----------
    session_dir : str or Path
        The directory path for the session.

    Returns
    -------
    list
        A list of session-bound curation tools.
    """
    try:
        session_dir = str(session_dir)

        @function_tool
        def load_mapping_file() -> str:
            """
            Load the series-sample mapping file for the current session.

            This tool loads the mapping file that contains the relationship between
            series and sample IDs for the current session.

            Returns
            -------
            str
                JSON string containing the mapping information.
            """
            print("🔧 CURATOR TOOL: load_mapping_file")
            return load_mapping_file_impl(session_dir)

        @function_tool
        def find_sample_directory(sample_id: str) -> str:
            """
            Find the directory containing metadata for a specific sample.

            This tool locates the directory that contains the metadata files
            for a given sample ID within the current session.

            Parameters
            ----------
            sample_id : str
                The sample ID to find (e.g., "GSM1000981").

            Returns
            -------
            str
                JSON string containing the directory information.
            """
            print(f"🔧 CURATOR TOOL: find_sample_directory - sample_id: {sample_id}")
            return find_sample_directory_impl(sample_id, session_dir)

        @function_tool
        def clean_metadata_files(
            sample_id: str, fields_to_remove: list[str] = None
        ) -> str:
            """
            Clean metadata files for a specific sample by removing specified fields.

            This tool processes the metadata files for a sample and removes
            specified fields to create cleaned versions for curation.

            Parameters
            ----------
            sample_id : str
                The sample ID to process (e.g., "GSM1000981").
            fields_to_remove : list[str], optional
                List of field names to remove from metadata files.
                If not provided, uses a default list of common fields to remove.

            Returns
            -------
            str
                JSON string containing the cleaning results.
            """
            print(
                f"🔧 CURATOR TOOL: clean_metadata_files - sample_id: {sample_id}, fields_to_remove: {fields_to_remove}"
            )
            return clean_metadata_files_impl(sample_id, session_dir, fields_to_remove)

        @function_tool
        def package_linked_data(
            sample_id: str, fields_to_remove: list[str] = None
        ) -> str:
            """
            Package all linked information for a sample into a comprehensive result.

            This tool combines all available metadata for a sample into a single
            package that can be used for curation tasks.

            Parameters
            ----------
            sample_id : str
                The sample ID to package (e.g., "GSM1000981").
            fields_to_remove : list[str], optional
                List of field names to remove from metadata files.
                If not provided, uses a default list of common fields to remove.

            Returns
            -------
            str
                JSON string containing the packaged data.
            """
            print(
                f"🔧 CURATOR TOOL: package_linked_data - sample_id: {sample_id}, fields_to_remove: {fields_to_remove}"
            )
            return package_linked_data_impl(sample_id, session_dir, fields_to_remove)

        @function_tool
        def process_multiple_samples(
            sample_ids: list[str], fields_to_remove: list[str] = None
        ) -> str:
            """
            Process multiple sample IDs at once (clean and package for all samples).

            This tool processes multiple samples simultaneously, creating cleaned
            metadata packages for each sample.

            Parameters
            ----------
            sample_ids : list[str]
                List of sample IDs to process (e.g., ["GSM1000981", "GSM1021412"]).
            fields_to_remove : list[str], optional
                List of field names to remove from metadata files.
                If not provided, uses a default list of common fields to remove.

            Returns
            -------
            str
                JSON string containing the processing results for all samples.
            """
            print(
                f"🔧 CURATOR TOOL: process_multiple_samples - sample_ids: {sample_ids}, fields_to_remove: {fields_to_remove}"
            )
            return process_multiple_samples_impl(
                sample_ids, session_dir, fields_to_remove
            )

        @function_tool
        def save_curation_results(curation_results_json: str) -> str:
            """
            Save curation results to a file.

            This tool saves the curation results to a JSON file in the session directory.

            Parameters
            ----------
            curation_results_json : str
                JSON string containing the curation results to save.

            Returns
            -------
            str
                JSON string containing the save operation result.
            """
            print("🔧 CURATOR TOOL: save_curation_results")
            print(f"🔍 DEBUG: Received JSON length: {len(curation_results_json)}")
            print(f"🔍 DEBUG: First 200 chars: {repr(curation_results_json[:200])}")
            print(f"🔍 DEBUG: Last 200 chars: {repr(curation_results_json[-200:])}")

            try:
                # Parse the JSON string to get the list of curation results
                import json
                from src.models.curation_models import CurationResult

                print("🔍 DEBUG: Attempting to parse JSON...")

                # Clean the JSON string to handle potential issues
                cleaned_json = curation_results_json.strip()

                # Try to extract valid JSON if there's extra data
                # Look for the main JSON array/object structure
                if cleaned_json.startswith("[") and "}]" in cleaned_json:
                    # Find the end of the JSON array
                    end_pos = cleaned_json.rfind("}]") + 2
                    cleaned_json = cleaned_json[:end_pos]
                    print(
                        f"🔍 DEBUG: Cleaned JSON to remove extra data, new length: {len(cleaned_json)}"
                    )
                elif cleaned_json.startswith("{") and cleaned_json.count("{") > 0:
                    # For single objects, find the matching closing brace
                    brace_count = 0
                    end_pos = 0
                    for i, char in enumerate(cleaned_json):
                        if char == "{":
                            brace_count += 1
                        elif char == "}":
                            brace_count -= 1
                            if brace_count == 0:
                                end_pos = i + 1
                                break
                    if end_pos > 0:
                        cleaned_json = cleaned_json[:end_pos]
                        print(
                            f"🔍 DEBUG: Cleaned JSON object, new length: {len(cleaned_json)}"
                        )

                print(f"🔍 DEBUG: Final cleaned JSON length: {len(cleaned_json)}")
                print(
                    f"🔍 DEBUG: Last 100 chars of cleaned JSON: {repr(cleaned_json[-100:])}"
                )

                curation_data_list = json.loads(cleaned_json)
                print(
                    f"🔍 DEBUG: Successfully parsed JSON. Type: {type(curation_data_list)}, Length: {len(curation_data_list) if isinstance(curation_data_list, list) else 'N/A'}"
                )

                curation_results = []

                # Handle both single objects and lists
                if isinstance(curation_data_list, dict):
                    print("🔍 DEBUG: Converting single dict to list")
                    curation_data_list = [curation_data_list]

                for i, curation_data in enumerate(curation_data_list):
                    print(f"🔍 DEBUG: Processing item {i}: {type(curation_data)}")
                    curation_result = CurationResult(**curation_data)
                    curation_results.append(curation_result)

                result = save_curation_results_impl(curation_results, session_dir)
                print(f"✅ Saved curation results for {len(curation_results)} samples")
                return json.dumps(result, indent=2)

            except json.JSONDecodeError as e:
                print(f"🔍 DEBUG: JSON parsing error at position {e.pos}: {e.msg}")
                if hasattr(e, "colno"):
                    start_pos = max(0, e.pos - 50)
                    end_pos = min(len(curation_results_json), e.pos + 50)
                    context = curation_results_json[start_pos:end_pos]
                    print(f"🔍 DEBUG: Context around error: {repr(context)}")
                result = {
                    "success": False,
                    "message": f"Error saving curation results: {str(e)}",
                    "error": str(e),
                    "debug_info": {
                        "json_length": len(curation_results_json),
                        "error_position": e.pos,
                        "error_message": e.msg,
                    },
                }
                return json.dumps(result, indent=2)
            except Exception as e:
                print(f"🔍 DEBUG: Other error: {type(e).__name__}: {str(e)}")
                result = {
                    "success": False,
                    "message": f"Error saving curation results: {str(e)}",
                    "error": str(e),
                }
                return json.dumps(result, indent=2)

        @function_tool
        def load_curation_data_for_samples(sample_ids_json: str) -> str:
            """
            Load curation data for specific samples.

            This tool loads the curation data for the specified samples from
            the session directory.

            Parameters
            ----------
            sample_ids_json : str
                JSON string containing a list of sample IDs.

            Returns
            -------
            str
                JSON string containing the loaded curation data.
            """
            print("🔧 CURATOR TOOL: load_curation_data_for_samples")
            return load_curation_data_for_samples_impl(sample_ids_json, session_dir)

        @function_tool
        def set_testing_session() -> str:
            """
            Set up a testing session for development and debugging.

            This tool creates a testing session with predefined data for
            development and debugging purposes.

            Returns
            -------
            str
                JSON string containing the testing session setup result.
            """
            print("🔧 CURATOR TOOL: set_testing_session")
            try:
                result = {
                    "success": True,
                    "message": "Testing session setup completed",
                    "session_directory": str(session_dir),
                    "timestamp": str(datetime.now()),
                }
                return json.dumps(result, indent=2)
            except Exception as e:
                result = {
                    "success": False,
                    "message": f"Failed to set up testing session: {str(e)}",
                    "error": str(e),
                }
                return json.dumps(result, indent=2)

        @function_tool
        def serialize_agent_output(output_type: str) -> str:
            """
            Serialize agent output to a specific format.

            This tool serializes the agent's output to a specific format
            for further processing or storage.

            Parameters
            ----------
            output_type : str
                The type of output format to use (e.g., "json", "csv") or agent type ("ingestion", "linker", "curator").

            Returns
            -------
            str
                JSON string containing the serialized output.
            """
            print(
                f"🔧 CURATOR TOOL: serialize_agent_output - output_type: {output_type}"
            )
            print(f"🔍 DEBUG: Received output_type: {repr(output_type)}")
            print(
                "🔍 DEBUG: Expected types: ['ingestion', 'linker', 'curator', 'json', 'csv']"
            )

            try:
                if output_type.lower() in ["ingestion", "linker", "curator"]:
                    result = {
                        "success": True,
                        "message": f"Serialization tool available for {output_type} outputs",
                        "files_created": [],
                        "timestamp": str(datetime.now()),
                    }
                    return json.dumps(result, indent=2)
                elif output_type.lower() in ["json", "csv"]:
                    # Handle format-based serialization requests
                    result = {
                        "success": True,
                        "message": f"Agent output serialized in {output_type.upper()} format",
                        "format": output_type.lower(),
                        "timestamp": str(datetime.now()),
                        "notes": "Output has been processed and is available in the session directory",
                    }
                    return json.dumps(result, indent=2)
                else:
                    print(
                        f"🔍 DEBUG: output_type '{output_type}' not in supported types"
                    )
                    result = {
                        "success": False,
                        "message": f"Unknown output type: {output_type}",
                        "error": "Supported types: ingestion, linker, curator, json, csv",
                        "debug_info": {
                            "received_type": output_type,
                            "supported_types": [
                                "ingestion",
                                "linker",
                                "curator",
                                "json",
                                "csv",
                            ],
                        },
                    }
                    return json.dumps(result, indent=2)
            except Exception as e:
                print(
                    f"🔍 DEBUG: Error in serialize_agent_output: {type(e).__name__}: {str(e)}"
                )
                result = {
                    "success": False,
                    "message": f"Error in serialization: {str(e)}",
                    "error": str(e),
                }
                return json.dumps(result, indent=2)

        # Track calls to prevent repeated data access
        _call_count = {"get_data_intake_context": 0}
        _first_call_data = {"data": None}

        @function_tool
        def get_data_intake_context() -> str:
            """
            Get the data intake context from the hybrid pipeline.

            This tool provides access to the complete structured output from the data_intake workflow
            when the CuratorAgent is being used as part of the hybrid pipeline.

            Returns
            -------
            str
                JSON string containing the data intake output structure including cleaned metadata.
            """
            _call_count["get_data_intake_context"] += 1
            call_num = _call_count["get_data_intake_context"]

            print(f"🔧 CURATOR TOOL: get_data_intake_context (call #{call_num})")

            # DEBUG: Detailed call analysis for workflow violation investigation
            import traceback
            import inspect

            print("🔍 CALL STACK DEBUG:")
            print(f"   📋 Call number: {call_num}")
            print(
                f"   📋 Function args: {inspect.getargvalues(inspect.currentframe())}"
            )
            print(f"   📋 Call stack depth: {len(traceback.extract_stack())}")

            # Print first few stack frames to understand call source
            stack = traceback.extract_stack()
            print("   📋 Recent call stack:")
            for i, frame in enumerate(stack[-5:]):  # Last 5 frames
                print(f"      {i}: {frame.filename}:{frame.lineno} in {frame.name}")
                print(f"         {frame.line}")

            print("🔍 TOOL STATE DEBUG:")
            print(f"   📋 Previous call count: {_call_count}")
            print(f"   📋 Has cached data: {_first_call_data['data'] is not None}")

            # COMPLETELY BLOCK ANY REPEATED CALLS - NO MERCY!
            if call_num > 1:
                print(
                    f"🚫 FATAL ERROR: REPEATED CALL #{call_num} - COMPLETELY BLOCKED!"
                )
                print("🚫 WORKFLOW VIOLATION: This tool can ONLY be called ONCE!")
                print("🚫 You already received the data on your first call!")
                print("🚫 STOP calling this tool and start your curation analysis NOW!")

                # NO MERCY - completely refuse ANY repeated calls
                result = {
                    "success": False,
                    "message": f"FATAL WORKFLOW VIOLATION: get_data_intake_context called {call_num} times. This tool can ONLY be called ONCE.",
                    "error": "CRITICAL BLOCKING: You already received all data on call #1. Use that data for your analysis.",
                    "instruction": "IMMEDIATELY stop calling tools and perform curation analysis with the data from your first call.",
                    "call_count": call_num,
                    "blocked": True,
                    "fatal_violation": True,
                    "next_action": "STOP calling tools. START curation analysis with previous data.",
                }
                return json.dumps(result, indent=2)

            try:
                # Import the curator module to access the stored data_intake_output
                import src.agents.curator as curator_module

                if (
                    hasattr(curator_module, "_data_intake_output")
                    and curator_module._data_intake_output
                ):
                    print("✅ First call successful - accessing data intake context")
                    print("🔄 REMEMBER: This is your ONLY call to this tool")
                    print(
                        "🔄 Use this data for all your analysis - DO NOT call this tool again"
                    )

                    # DEBUG: Show what data is being returned
                    result = curator_module._data_intake_output.model_dump()
                    print(
                        f"🔍 DEBUG: Data intake contains {len(result.get('curation_packages', []))} packages"
                    )
                    if result.get("curation_packages"):
                        package = result["curation_packages"][0]
                        print(
                            f"🔍 DEBUG: Sample ID: {package.get('sample_id', 'Unknown')}"
                        )
                        print(
                            f"🔍 DEBUG: Has sample metadata: {package.get('sample_metadata') is not None}"
                        )
                        print(
                            f"🔍 DEBUG: Has series metadata: {package.get('series_metadata') is not None}"
                        )
                        print(
                            f"🔍 DEBUG: Has abstract metadata: {package.get('abstract_metadata') is not None}"
                        )

                    # Convert the LinkerOutput to a dictionary and return as JSON
                    result = curator_module._data_intake_output.model_dump()

                    # Add a summary of the cleaned metadata for easier access
                    if result.get("cleaned_series_metadata"):
                        result["cleaned_metadata_summary"] = {
                            "series_count": len(result["cleaned_series_metadata"]),
                            "sample_count": len(
                                result.get("cleaned_sample_metadata", {})
                            ),
                            "abstract_count": len(
                                result.get("cleaned_abstract_metadata", {})
                            ),
                            "available_series_ids": list(
                                result["cleaned_series_metadata"].keys()
                            ),
                            "available_sample_ids": list(
                                result.get("cleaned_sample_metadata", {}).keys()
                            ),
                            "available_pmids": list(
                                result.get("cleaned_abstract_metadata", {}).keys()
                            ),
                        }

                    # Add strong workflow instruction
                    result["workflow_instruction"] = (
                        "CRITICAL: This is your ONLY call to get_data_intake_context. Use this data for all analysis. DO NOT call this tool again. Proceed directly to curation analysis."
                    )
                    result["next_step"] = (
                        "Stop calling tools and start internal curation analysis NOW"
                    )

                    # Cache the data for potential mercy return
                    response_json = json.dumps(result, indent=2, default=str)
                    _first_call_data["data"] = response_json

                    return response_json
                else:
                    result = {
                        "success": False,
                        "message": "No data intake output available. This tool is only available in hybrid pipeline mode.",
                        "available": False,
                    }
                    return json.dumps(result, indent=2)
            except Exception as e:
                result = {
                    "success": False,
                    "message": f"Failed to get data intake context: {str(e)}",
                    "error": str(e),
                }
                return json.dumps(result, indent=2)

        # In hybrid mode, only provide essential tools to prevent confusion
        return [
            get_data_intake_context,  # Primary tool for data access
            save_curation_results,  # Final output tool
        ]

    except Exception as e:
        print(f"❌ Error creating curator tools: {str(e)}")
        return []
