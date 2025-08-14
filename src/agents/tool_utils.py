"""
Tool utilities for agents to use GEO metadata extraction functionality.

This module exposes the GEO metadata extraction tools as function tools
that can be used by agents in the system. All logic is delegated to
the actual tool implementations in src.tools.
"""

from __future__ import annotations

from pathlib import Path
import os
import json

from agents import function_tool
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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
    package_linked_data_impl,
)

from src.tools.curator_tools import (
    save_curation_results_impl,
    load_curation_data_for_samples_impl,
    get_data_intake_context_impl,
    serialize_agent_output_impl,
    set_testing_session_impl,
)

from src.tools.normalizer_tools import (
    semantic_search_candidates_impl,
)

from src.models.normalization_models import BatchNormalizationResult


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
            pmid: str, email: str = None, api_key: str = None
        ) -> str:
            """
            Extract abstract and metadata for a PubMed paper.

            This tool retrieves the abstract, title, authors, and other metadata
            for a specific PubMed paper using its PMID.

            Parameters
            ----------
            pmid : str
                PubMed ID (e.g., "23902433").
            email : str, optional
                Email address for NCBI E-Utils identification.
                If not provided, uses session default.
            api_key : str, optional
                NCBI API key for higher rate limits.
                If not provided, uses session default.

            Returns
            -------
            str
                Path to the saved abstract metadata file.
            """
            # Use session defaults if not provided
            if email is None:
                email = default_email
            if api_key is None:
                api_key = default_api_key

            return extract_paper_abstract_impl(pmid, session_dir, email, api_key)

        @function_tool
        def extract_pubmed_id_from_gse_metadata(gse_metadata_file: str) -> str:
            """
            Extract PubMed ID from a GSE metadata file.

            This tool parses a GSE metadata file and extracts the associated
            PubMed ID if available.

            Parameters
            ----------
            gse_metadata_file : str
                Path to the GSE metadata JSON file.

            Returns
            -------
            str
                The extracted PubMed ID or an error message.
            """
            return extract_pubmed_id_from_gse_metadata_impl(gse_metadata_file)

        @function_tool
        def extract_series_id_from_gsm_metadata(gsm_metadata_file: str) -> str:
            """
            Extract series ID from a GSM metadata file.

            This tool parses a GSM metadata file and extracts the associated
            series ID.

            Parameters
            ----------
            gsm_metadata_file : str
                Path to the GSM metadata JSON file.

            Returns
            -------
            str
                The extracted series ID or an error message.
            """
            return extract_series_id_from_gsm_metadata_impl(gsm_metadata_file)

        @function_tool
        def validate_geo_inputs(
            gsm_id: str = None,
            gse_id: str = None,
            pmid: str = None,
            target_field: str = None,
        ) -> str:
            """
            Validate GEO and PubMed inputs for proper format.

            This tool validates that provided IDs follow the correct format
            and patterns expected by NCBI services.

            Parameters
            ----------
            gsm_id : str, optional
                GEO Sample ID to validate.
            gse_id : str, optional
                GEO Series ID to validate.
            pmid : str, optional
                PubMed ID to validate.
            target_field : str, optional
                Target metadata field to validate.

            Returns
            -------
            str
                JSON string with validation results.
            """
            return validate_geo_inputs_impl(gsm_id, gse_id, pmid, target_field)

        @function_tool
        def create_series_sample_mapping() -> str:
            """
            Create a mapping between series and samples in the session directory.

            This tool scans the session directory for GEO metadata files and
            creates a mapping structure showing the relationship between
            series (GSE) and samples (GSM).

            Returns
            -------
            str
                JSON string with the series-sample mapping.
            """
            return create_series_sample_mapping_impl(session_dir)

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
            return load_mapping_file_impl(session_dir)

        @function_tool
        def find_sample_directory(sample_id: str) -> str:
            """
            Find the directory containing files for a specific sample ID.

            This tool searches the session directory structure to locate
            where files for a given sample ID are stored.

            Parameters
            ----------
            sample_id : str
                The sample ID to search for (e.g., "GSM1019742").

            Returns
            -------
            str
                JSON string with the directory path or error message.
            """
            return find_sample_directory_impl(session_dir, sample_id)

        @function_tool
        def clean_metadata_files(
            sample_id: str,
            target_field: str,
            keep_fields: str = None,
        ) -> str:
            """
            Clean and prepare metadata files for curation.

            This tool processes metadata files for a sample, removing unnecessary
            fields and preparing the data for curation analysis.

            Parameters
            ----------
            sample_id : str
                The sample ID to process.
            target_field : str
                The target metadata field for curation.
            keep_fields : str, optional
                Comma-separated list of additional fields to keep.

            Returns
            -------
            str
                JSON string with cleaning results.
            """
            keep_fields_list = None
            if keep_fields:
                keep_fields_list = [field.strip() for field in keep_fields.split(",")]

            return clean_metadata_files_impl(
                session_dir, sample_id, target_field, keep_fields_list
            )

        @function_tool
        def package_linked_data(
            sample_id: str,
            target_field: str,
        ) -> str:
            """
            Package linked data for a sample into a curation-ready format.

            This tool takes processed metadata files and packages them into
            a structured format suitable for curation analysis.

            Parameters
            ----------
            sample_id : str
                The sample ID to package.
            target_field : str
                The target metadata field for curation.

            Returns
            -------
            str
                JSON string with packaging results.
            """
            return package_linked_data_impl(session_dir, sample_id, target_field)

        @function_tool
        def create_curation_data_package(
            sample_id: str,
            target_field: str,
        ) -> str:
            """
            Create a complete curation data package for a sample.

            This tool creates a comprehensive data package that includes all
            relevant metadata for curation analysis of a specific target field.

            Parameters
            ----------
            sample_id : str
                The sample ID to create a package for.
            target_field : str
                The target metadata field for curation.

            Returns
            -------
            str
                JSON string with the curation data package.
            """
            return create_curation_data_package_impl(
                session_dir, sample_id, target_field
            )

        @function_tool
        def process_multiple_samples(
            sample_ids_json: str,
            target_field: str,
        ) -> str:
            """
            Process multiple samples for curation.

            This tool processes multiple samples simultaneously, preparing them
            for curation analysis.

            Parameters
            ----------
            sample_ids_json : str
                JSON string containing a list of sample IDs.
            target_field : str
                The target metadata field for curation.

            Returns
            -------
            str
                JSON string with processing results for all samples.
            """
            return process_multiple_samples_impl(
                session_dir, sample_ids_json, target_field
            )

        @function_tool
        def save_curation_results(curation_results_json: str) -> str:
            """
            Save curation results to the session directory.

            This tool persists the results of curation analysis to appropriate
            files in the session directory.

            Parameters
            ----------
            curation_results_json : str
                JSON string containing the curation results.

            Returns
            -------
            str
                JSON string with save operation results.
            """
            try:
                # Parse the JSON string to get the list of curation results
                import json as json_module
                from src.models.curation_models import CurationResult

                # Clean the JSON string to handle potential issues
                cleaned_json = curation_results_json.strip()

                # Try to extract valid JSON if there's extra data
                # Look for the main JSON array/object structure
                if cleaned_json.startswith("[") and "}]" in cleaned_json:
                    # Find the end of the JSON array
                    end_pos = cleaned_json.rfind("}]") + 2
                    cleaned_json = cleaned_json[:end_pos]
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

                curation_data_list = json_module.loads(cleaned_json)
                curation_results = []

                # Handle both single objects and lists
                if isinstance(curation_data_list, dict):
                    curation_data_list = [curation_data_list]

                for i, curation_data in enumerate(curation_data_list):
                    try:
                        curation_result = CurationResult(**curation_data)
                        curation_results.append(curation_result)
                    except Exception as validation_error:
                        print(
                            f"🔍 VALIDATION ERROR for item {i}: {type(validation_error).__name__}: {str(validation_error)}"
                        )
                        print(
                            f"🔍 VALIDATION ERROR data keys: {list(curation_data.keys()) if isinstance(curation_data, dict) else 'Not a dict'}"
                        )
                        raise validation_error

                # Call the implementation with correct parameters: (curation_results, session_dir)
                result = save_curation_results_impl(curation_results, session_dir)
                return json_module.dumps(result, indent=2)

            except json_module.JSONDecodeError as e:
                print(
                    f"🔍 VALIDATION ERROR - JSON parsing failed at position {e.pos}: {e.msg}"
                )
                start_pos = max(0, e.pos - 50)
                end_pos = min(len(curation_results_json), e.pos + 50)
                context = curation_results_json[start_pos:end_pos]
                print(f"🔍 VALIDATION ERROR - Context around error: {repr(context)}")
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
                return json_module.dumps(result, indent=2)
            except Exception as e:
                print(
                    f"🔍 VALIDATION ERROR - Other error: {type(e).__name__}: {str(e)}"
                )
                result = {
                    "success": False,
                    "message": f"Error saving curation results: {str(e)}",
                    "error": str(e),
                }
                return json_module.dumps(result, indent=2)

        @function_tool
        def load_curation_data_for_samples(sample_ids_json: str) -> str:
            """
            Load curation data for multiple samples.

            This tool loads and aggregates curation data for a list of samples
            from the session directory.

            Parameters
            ----------
            sample_ids_json : str
                JSON string containing a list of sample IDs.

            Returns
            -------
            str
                JSON string with the loaded curation data.
            """
            return load_curation_data_for_samples_impl(session_dir, sample_ids_json)

        @function_tool
        def set_testing_session() -> str:
            """
            Set up a testing session for the current workflow.

            This tool configures the session for testing purposes, ensuring
            proper test environment setup.

            Returns
            -------
            str
                JSON string with testing session setup results.
            """
            result = set_testing_session_impl()
            return json.dumps(result, indent=2)

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
            result = serialize_agent_output_impl(output_type)
            return json.dumps(result, indent=2)

        return [
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
            load_curation_data_for_samples,
            set_testing_session,
            serialize_agent_output,
        ]

    except Exception as e:
        print(f"❌ Error creating session tools: {str(e)}")
        return []


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
            return load_mapping_file_impl(session_dir)

        @function_tool
        def find_sample_directory(sample_id: str) -> str:
            """
            Find the directory containing files for a specific sample ID.

            This tool searches the session directory structure to locate
            where files for a given sample ID are stored.

            Parameters
            ----------
            sample_id : str
                The sample ID to search for (e.g., "GSM1019742").

            Returns
            -------
            str
                JSON string with the directory path or error message.
            """
            return find_sample_directory_impl(session_dir, sample_id)

        @function_tool
        def clean_metadata_files(
            sample_id: str,
            target_field: str,
            keep_fields: str = None,
        ) -> str:
            """
            Clean and prepare metadata files for curation.

            This tool processes metadata files for a sample, removing unnecessary
            fields and preparing the data for curation analysis.

            Parameters
            ----------
            sample_id : str
                The sample ID to process.
            target_field : str
                The target metadata field for curation.
            keep_fields : str, optional
                Comma-separated list of additional fields to keep.

            Returns
            -------
            str
                JSON string with cleaning results.
            """
            keep_fields_list = None
            if keep_fields:
                keep_fields_list = [field.strip() for field in keep_fields.split(",")]

            return clean_metadata_files_impl(
                session_dir, sample_id, target_field, keep_fields_list
            )

        @function_tool
        def package_linked_data(
            sample_id: str,
            target_field: str,
        ) -> str:
            """
            Package linked data for a sample into a curation-ready format.

            This tool takes processed metadata files and packages them into
            a structured format suitable for curation analysis.

            Parameters
            ----------
            sample_id : str
                The sample ID to package.
            target_field : str
                The target metadata field for curation.

            Returns
            -------
            str
                JSON string with packaging results.
            """
            return package_linked_data_impl(session_dir, sample_id, target_field)

        @function_tool
        def process_multiple_samples(
            sample_ids_json: str,
            target_field: str,
        ) -> str:
            """
            Process multiple samples for curation.

            This tool processes multiple samples simultaneously, preparing them
            for curation analysis.

            Parameters
            ----------
            sample_ids_json : str
                JSON string containing a list of sample IDs.
            target_field : str
                The target metadata field for curation.

            Returns
            -------
            str
                JSON string with processing results for all samples.
            """
            return process_multiple_samples_impl(
                session_dir, sample_ids_json, target_field
            )

        @function_tool
        def save_curation_results(curation_results_json: str) -> str:
            """
            Save curation results to the session directory.

            This tool persists the results of curation analysis to appropriate
            files in the session directory.

            Parameters
            ----------
            curation_results_json : str
                JSON string containing the curation results.

            Returns
            -------
            str
                JSON string with save operation results.
            """
            try:
                # Parse the JSON string to get the list of curation results
                import json as json_module
                from src.models.curation_models import CurationResult

                # Clean the JSON string to handle potential issues
                cleaned_json = curation_results_json.strip()

                # Try to extract valid JSON if there's extra data
                # Look for the main JSON array/object structure
                if cleaned_json.startswith("[") and "}]" in cleaned_json:
                    # Find the end of the JSON array
                    end_pos = cleaned_json.rfind("}]") + 2
                    cleaned_json = cleaned_json[:end_pos]
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

                curation_data_list = json_module.loads(cleaned_json)
                curation_results = []

                # Handle both single objects and lists
                if isinstance(curation_data_list, dict):
                    curation_data_list = [curation_data_list]

                for i, curation_data in enumerate(curation_data_list):
                    try:
                        curation_result = CurationResult(**curation_data)
                        curation_results.append(curation_result)
                    except Exception as validation_error:
                        print(
                            f"🔍 VALIDATION ERROR for item {i}: {type(validation_error).__name__}: {str(validation_error)}"
                        )
                        print(
                            f"🔍 VALIDATION ERROR data keys: {list(curation_data.keys()) if isinstance(curation_data, dict) else 'Not a dict'}"
                        )
                        raise validation_error

                # Call the implementation with correct parameters: (curation_results, session_dir)
                result = save_curation_results_impl(curation_results, session_dir)
                return json_module.dumps(result, indent=2)

            except json_module.JSONDecodeError as e:
                print(
                    f"🔍 VALIDATION ERROR - JSON parsing failed at position {e.pos}: {e.msg}"
                )
                start_pos = max(0, e.pos - 50)
                end_pos = min(len(curation_results_json), e.pos + 50)
                context = curation_results_json[start_pos:end_pos]
                print(f"🔍 VALIDATION ERROR - Context around error: {repr(context)}")
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
                return json_module.dumps(result, indent=2)
            except Exception as e:
                print(
                    f"🔍 VALIDATION ERROR - Other error: {type(e).__name__}: {str(e)}"
                )
                result = {
                    "success": False,
                    "message": f"Error saving curation results: {str(e)}",
                    "error": str(e),
                }
                return json_module.dumps(result, indent=2)

        @function_tool
        def load_curation_data_for_samples(sample_ids_json: str) -> str:
            """
            Load curation data for multiple samples.

            This tool loads and aggregates curation data for a list of samples
            from the session directory.

            Parameters
            ----------
            sample_ids_json : str
                JSON string containing a list of sample IDs.

            Returns
            -------
            str
                JSON string with the loaded curation data.
            """
            return load_curation_data_for_samples_impl(session_dir, sample_ids_json)

        @function_tool
        def set_testing_session() -> str:
            """
            Set up a testing session for the current workflow.

            This tool configures the session for testing purposes, ensuring
            proper test environment setup.

            Returns
            -------
            str
                JSON string with testing session setup results.
            """
            result = set_testing_session_impl()
            return json.dumps(result, indent=2)

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
            result = serialize_agent_output_impl(output_type)
            return json.dumps(result, indent=2)

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
            result = get_data_intake_context_impl()
            return json.dumps(result, indent=2)

        return []  # NO TOOLS - Data passed directly in input

    except Exception as e:
        print(f"❌ Error creating curator tools: {str(e)}")
        return []


def get_normalizer_tools(session_dir: str | Path) -> list:
    """
    Creates a semantic search tool for the NormalizerAgent.

    This is a light wrapper that exposes the semantic search tool defined in src/tools
    to the NormalizerAgent.

    Parameters
    ----------
    session_dir : str or Path
        The directory path for the session (used for context).

    Returns
    -------
    list
        A list containing the semantic search tool wrapper.
    """
    try:
        session_dir = str(session_dir)

        @function_tool
        def semantic_search_candidates(
            curation_results_file: str,
            target_field: str = "Disease",
            top_k: int = 2,
            min_score: float = 0.5,
        ) -> BatchNormalizationResult:
            """
            Perform semantic search on extracted candidates from a curation results file.

            This tool reads a JSON file containing CurationResult objects, performs
            semantic similarity search, and returns a fully formed BatchNormalizationResult.

            Parameters
            ----------
            curation_results_file : str
                Path to the JSON file containing a list of CurationResult objects.
            target_field : str, default "Disease"
                The target metadata field (e.g., "Disease", "Tissue", "Age").
            top_k : int, default 5
                Number of top matches to return per candidate.
            min_score : float, default 0.5
                Minimum similarity score threshold for matches.

            Returns
            -------
            BatchNormalizationResult
                The structured output object containing all normalization results.
            """

            try:
                # Call the implementation in src/tools
                result = semantic_search_candidates_impl(
                    curation_results_file=curation_results_file,
                    target_field=target_field,
                    ontologies=None,  # Let the tool determine ontology automatically
                    top_k=top_k,
                    min_score=min_score,
                )

                return result

            except Exception as e:
                print(f"❌ [TOOL_CALL] semantic_search_candidates error: {str(e)}")
                # In case of error, we might want to return a specific error structure
                # For now, re-raising the exception might be cleaner.
                raise

        return [
            semantic_search_candidates,
        ]

    except Exception as e:
        print(f"❌ Error creating normalizer tools: {str(e)}")
        return []
