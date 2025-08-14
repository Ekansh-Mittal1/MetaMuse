"""
Batch processing tools for extracting data from MetaMuse agent outputs.

This module provides utility functions for extracting specific metadata fields
from the JSON outputs of different pipeline stages (Data Intake, Curation, Normalization).
Used by the batch_targets workflow to process multiple target fields efficiently.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Union
from src.models.agent_outputs import LinkerOutput, CuratorOutput


def extract_direct_fields_from_data_intake(
    data_intake_output: Union[Dict[str, Any], LinkerOutput, str, Path],
    sample_ids: List[str],
) -> Dict[str, Dict[str, Any]]:
    """
    Extract direct fields from data intake output that don't require curation/normalization.

    These fields are: Organism (platform_organism), PubMed ID (pubmed_id),
    Instrument (platform_id + instrument_model).

    Parameters
    ----------
    data_intake_output : Union[Dict, LinkerOutput, str, Path]
        Data intake output (JSON dict, LinkerOutput object, or file path)
    sample_ids : List[str]
        List of sample IDs to extract data for

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Dictionary mapping sample_id -> {field_name: field_data}

    Example
    -------
    {
        "GSM1006725": {
            "organism": {"value": "Homo sapiens", "source": "platform_organism"},
            "pubmed_id": {"value": "23382218", "source": "pubmed_id"},
            "platform_id": {"value": "GPL11154", "source": "platform_id"},
            "instrument_model": {"value": "Illumina HiSeq 2000", "source": "instrument_model"}
        }
    }
    """

    # Load data intake output if it's a file path
    if isinstance(data_intake_output, (str, Path)):
        with open(data_intake_output, "r") as f:
            data = json.load(f)
    elif isinstance(data_intake_output, LinkerOutput):
        data = data_intake_output.model_dump()
    else:
        # Check if it's a Pydantic model and convert to dict
        if hasattr(data_intake_output, "model_dump"):
            data = data_intake_output.model_dump()
        else:
            data = data_intake_output

    # Add null check for curation_packages to prevent crashes
    curation_packages = data.get("curation_packages", [])
    if not curation_packages:  # Handles None, empty list, etc.
        # Use proper logging instead of print
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"⚠️No curation packages found for samples: {sample_ids}")
        # Return empty results structure instead of crashing
        return {sample_id: {} for sample_id in sample_ids}

    results = {}

    # Extract data for each sample
    for sample_id in sample_ids:
        sample_results = {}

        # Find the curation package for this sample
        curation_package = None
        for package in curation_packages:
            if package.get("sample_id") == sample_id:
                curation_package = package
                break

        if not curation_package:
            # Use proper logging instead of print
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ No curation package found for sample {sample_id}")
            results[sample_id] = {}
            continue

        # Extract organism from platform_organism
        organism_value = None
        series_metadata = curation_package.get("series_metadata", {})
        if series_metadata and isinstance(series_metadata, dict):  # Add type check
            content = series_metadata.get("content", [])
            if isinstance(content, list):  # Ensure content is a list
                for item in content:
                    if isinstance(item, dict) and item.get("key") == "platform_organism":
                        organism_value = item.get("value")
                        break

        if organism_value:
            sample_results["organism"] = {
                "value": organism_value,
                "source": "platform_organism",
                "confidence": 1.0,
            }

        # Extract PubMed ID
        pubmed_value = None
        if series_metadata and isinstance(series_metadata, dict):  # Add type check
            content = series_metadata.get("content", [])
            if isinstance(content, list):  # Ensure content is a list
                for item in content:
                    if isinstance(item, dict) and item.get("key") == "pubmed_id":
                        pubmed_value = item.get("value")
                        break

        if pubmed_value:
            sample_results["pubmed_id"] = {
                "value": pubmed_value,
                "source": "pubmed_id",
                "confidence": 1.0,
            }

        # Extract platform_id
        platform_id_value = None
        if series_metadata and isinstance(series_metadata, dict):  # Add type check
            content = series_metadata.get("content", [])
            if isinstance(content, list):  # Ensure content is a list
                for item in content:
                    if isinstance(item, dict) and item.get("key") == "platform_id":
                        platform_id_value = item.get("value")
                        break

        if platform_id_value:
            sample_results["platform_id"] = {
                "value": platform_id_value,
                "source": "platform_id",
                "confidence": 1.0,
            }

        # Extract instrument_model
        instrument_value = None
        sample_metadata = curation_package.get("sample_metadata", {})
        if sample_metadata and isinstance(sample_metadata, dict):  # Add type check
            content = sample_metadata.get("content", [])
            if isinstance(content, list):  # Ensure content is a list
                for item in content:
                    if isinstance(item, dict) and item.get("key") == "instrument_model":
                        instrument_value = item.get("value")
                        break

        # If not found in sample metadata, try series metadata
        if not instrument_value and series_metadata and isinstance(series_metadata, dict):  # Add type check
            content = series_metadata.get("content", [])
            if isinstance(content, list):  # Ensure content is a list
                for item in content:
                    if isinstance(item, dict) and item.get("key") == "instrument_model":
                        instrument_value = item.get("value")
                        break

        if instrument_value:
            sample_results["instrument_model"] = {
                "value": instrument_value,
                "source": "instrument_model",
                "confidence": 1.0,
            }

        # Extract series_id from the curation package
        # Handle both CurationDataPackage objects and dictionaries
        if hasattr(curation_package, 'series_id'):
            # It's a CurationDataPackage object
            series_id_value = curation_package.series_id
        elif isinstance(curation_package, dict):
            # It's a dictionary (e.g., from JSON)
            series_id_value = curation_package.get("series_id")
        else:
            # Fallback
            series_id_value = None
            
        if series_id_value:
            sample_results["series_id"] = {
                "value": series_id_value,
                "source": "series_id",
                "confidence": 1.0,
            }

        results[sample_id] = sample_results

    return results


def extract_curation_candidates(
    curator_output: Union[Dict[str, Any], CuratorOutput, str, Path],
    target_field: str,
    sample_ids: List[str],
    error_tracker=None,
) -> Dict[str, Dict[str, Any]]:
    """
    Extract final candidates from curation output for a specific target field.

    Parameters
    ----------
    curator_output : Union[Dict, CuratorOutput, str, Path]
        Curation output (JSON dict, CuratorOutput object, or file path)
    target_field : str
        Target field that was curated (e.g., 'Disease', 'Tissue', 'Cell Line')
    sample_ids : List[str]
        List of sample IDs to extract data for
    error_tracker : object, optional
        Error tracker object with methods for tracking missing results

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Dictionary mapping sample_id -> {candidates, best_candidate}

    Example
    -------
    {
        "GSM1006725": {
            "candidates": [list of candidate objects],
            "best_candidate": {best candidate object},
            "candidate_count": 5
        }
    }
    """

    # Load curation output if it's a file path
    if isinstance(curator_output, (str, Path)):
        with open(curator_output, "r") as f:
            data = json.load(f)
    elif isinstance(curator_output, CuratorOutput):
        data = curator_output.model_dump()
    else:
        # Check if it's a Pydantic model and convert to dict
        if hasattr(curator_output, "model_dump"):
            data = curator_output.model_dump()
        else:
            data = curator_output

    results = {}

    # Extract data for each sample
    for sample_id in sample_ids:
        sample_results = {
            "candidates": [],
            "best_candidate": None,
            "candidate_count": 0,
        }

        # Find the curation result for this sample
        curation_result = None
        for result in data.get("curation_results", []):
            if result.get("sample_id") == sample_id:
                curation_result = result
                break

        if not curation_result:
            # Use proper logging instead of print
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ No curation result found for sample {sample_id}, target field {target_field}")
            
            # Track missing curation result for rerun capability
            if error_tracker and hasattr(error_tracker, 'track_missing_result'):
                error_tracker.track_missing_result(
                    sample_id=sample_id,
                    target_field=target_field,
                    result_type="curation",
                    reason="no_curation_result_in_output"
                )
            
            results[sample_id] = sample_results
            continue

        # Extract candidates by source (preserve original breakdown)
        series_candidates = curation_result.get("series_candidates", [])
        sample_candidates = curation_result.get("sample_candidates", [])
        abstract_candidates = curation_result.get("abstract_candidates", [])

        # Add source attribution to each candidate
        for candidate in series_candidates:
            candidate["candidate_source"] = "series"
        for candidate in sample_candidates:
            candidate["candidate_source"] = "sample"
        for candidate in abstract_candidates:
            candidate["candidate_source"] = "abstract"

        # Combine all candidates for best candidate selection
        all_candidates = series_candidates + sample_candidates + abstract_candidates
        
        # Sort candidates by confidence (highest first)
        all_candidates.sort(key=lambda x: x.get("confidence", 0), reverse=True)

        # Store candidates with source breakdown preserved (no combined list)
        sample_results["series_candidates"] = series_candidates
        sample_results["sample_candidates"] = sample_candidates  
        sample_results["abstract_candidates"] = abstract_candidates
        sample_results["candidate_count"] = len(all_candidates)

        # Get best candidate (highest confidence)
        if all_candidates:
            sample_results["best_candidate"] = all_candidates[0]

        results[sample_id] = sample_results

    return results


def extract_normalization_results(
    normalizer_output: Union[Dict[str, Any], str, Path],
    target_field: str,
    sample_ids: List[str],
    error_tracker=None,
) -> Dict[str, Dict[str, Any]]:
    """
    Extract normalization results for a specific target field.

    Parameters
    ----------
    normalizer_output : Union[Dict, str, Path]
        Normalization output (JSON dict or file path)
    target_field : str
        Target field that was normalized (e.g., 'Disease', 'Tissue', 'Organ')
    sample_ids : List[str]
        List of sample IDs to extract data for
    error_tracker : object, optional
        Error tracker object with methods for tracking missing results

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Dictionary mapping sample_id -> {normalized_term, term_id, confidence}

    Example
    -------
    {
        "GSM1006725": {
            "normalized_term": "embryonic stem cell",
            "term_id": "CLO:0000030",
            "confidence": 0.95,
            "ontology": "CLO",
            "original_value": "H1-hESC"
        }
    }
    """

    # Load normalization output if it's a file path
    if isinstance(normalizer_output, (str, Path)):
        with open(normalizer_output, "r") as f:
            data = json.load(f)
    else:
        # Check if it's a Pydantic model and convert to dict
        if hasattr(normalizer_output, "model_dump"):
            data = normalizer_output.model_dump()
        else:
            data = normalizer_output

    results = {}

    # Extract data for each sample
    for sample_id in sample_ids:
        sample_results = {
            "normalized_term": None,
            "term_id": None,
            "confidence": 0.0,
            "ontology": None,
            "original_value": None,
        }

        # Find the normalization result for this sample
        sample_result = None
        for result in data.get("sample_results", []):
            if result.get("sample_id") == sample_id:
                sample_result = result.get("result", {})
                break

        if not sample_result:
            # Use proper logging instead of print
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ No normalization result found for sample {sample_id}, target field {target_field}")
            
            # Track missing normalization result for rerun capability
            if error_tracker and hasattr(error_tracker, 'track_missing_result'):
                error_tracker.track_missing_result(
                    sample_id=sample_id,
                    target_field=target_field,
                    result_type="normalization",
                    reason="no_normalization_result_in_output"
                )
            
            results[sample_id] = sample_results
            continue

        # Get the normalization result fields
        final_candidate = sample_result.get("final_candidate")
        final_confidence = sample_result.get("final_confidence", 0.0)
        final_normalized_term = sample_result.get("final_normalized_term")
        final_normalized_id = sample_result.get("final_normalized_id")
        final_ontology = sample_result.get("final_ontology")

        # Use final_normalized results if available, otherwise fall back to final_candidate
        if final_normalized_term and final_normalized_id:
            sample_results["normalized_term"] = final_normalized_term
            sample_results["term_id"] = final_normalized_id
            sample_results["confidence"] = final_confidence
            sample_results["ontology"] = final_ontology
            sample_results["original_value"] = final_candidate
        elif final_candidate:
            # Fallback: if we have a final_candidate but no normalized results,
            # use the candidate as the original value and check if it was prenormalized
            sample_results["original_value"] = final_candidate
            sample_results["confidence"] = final_confidence

            # Try to extract prenormalized info from final_candidates list
            final_candidates = sample_result.get("final_candidates", [])
            if final_candidates:
                best_candidate = max(
                    final_candidates, key=lambda x: x.get("confidence", 0)
                )
                prenormalized = best_candidate.get("prenormalized", "")
                if "(" in prenormalized and ")" in prenormalized:
                    # Parse prenormalized format like "parkinson's disease (MONDO:0005180)"
                    term_part = prenormalized.split("(")[0].strip()
                    id_part = prenormalized.split("(")[1].split(")")[0].strip()
                    ontology_part = id_part.split(":")[0] if ":" in id_part else ""

                    sample_results["normalized_term"] = term_part
                    sample_results["term_id"] = id_part
                    sample_results["ontology"] = ontology_part

        results[sample_id] = sample_results

    return results


def combine_target_field_results(
    sample_ids: List[str],
    direct_fields: Dict[str, Dict[str, Any]],
    curation_results: Dict[
        str, Dict[str, Dict[str, Any]]
    ],  # target_field -> sample_id -> results
    normalization_results: Dict[
        str, Dict[str, Dict[str, Any]]
    ],  # target_field -> sample_id -> results
) -> Dict[str, Dict[str, Any]]:
    """
    Combine all target field results into a unified structure.

    Parameters
    ----------
    sample_ids : List[str]
        List of sample IDs
    direct_fields : Dict[str, Dict[str, Any]]
        Direct fields from data intake (sample_id -> field_data)
    curation_results : Dict[str, Dict[str, Dict[str, Any]]]
        Curation results (target_field -> sample_id -> results)
    normalization_results : Dict[str, Dict[str, Dict[str, Any]]]
        Normalization results (target_field -> sample_id -> results)

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Combined results (sample_id -> all_field_data)
    """

    combined_results = {}

    for sample_id in sample_ids:
        sample_data = {
            "sample_id": sample_id,
            "direct_fields": direct_fields.get(sample_id, {}),
            "curated_fields": {},
            "normalized_fields": {},
        }

        # Add curation results
        for target_field, field_results in curation_results.items():
            if sample_id in field_results:
                sample_data["curated_fields"][
                    target_field.lower().replace(" ", "_")
                ] = field_results[sample_id]

        # Add normalization results
        for target_field, field_results in normalization_results.items():
            if sample_id in field_results:
                sample_data["normalized_fields"][
                    target_field.lower().replace(" ", "_")
                ] = field_results[sample_id]

        combined_results[sample_id] = sample_data

    return combined_results


def save_batch_results(
    results: Dict[str, Any],
    session_directory: str,
    filename: str = "batch_targets_output.json",
) -> str:
    """
    Save batch processing results to a JSON file.

    Parameters
    ----------
    results : Dict[str, Any]
        Results to save
    session_directory : str
        Session directory path
    filename : str
        Output filename

    Returns
    -------
    str
        Path to saved file
    """

    output_path = Path(session_directory) / filename

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    return str(output_path)


def create_target_field_subdirectories(
    session_directory: str, target_fields: List[str]
) -> Dict[str, str]:
    """
    Create subdirectories for each target field in the session directory.

    Parameters
    ----------
    session_directory : str
        Base session directory
    target_fields : List[str]
        List of target fields to create subdirectories for

    Returns
    -------
    Dict[str, str]
        Mapping of target_field -> subdirectory_path
    """

    session_path = Path(session_directory)
    subdirs = {}

    for field in target_fields:
        # Convert field name to safe directory name
        dir_name = field.lower().replace(" ", "_").replace("/", "_")
        subdir_path = session_path / dir_name
        subdir_path.mkdir(exist_ok=True)
        subdirs[field] = str(subdir_path)

    return subdirs
