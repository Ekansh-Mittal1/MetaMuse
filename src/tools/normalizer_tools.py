#!/usr/bin/env python3
"""
Normalization tools for metadata normalization using ontology semantic search.

This module provides tools for normalizing metadata candidates against biomedical
ontologies using semantic similarity search with transformer models.
"""

import sys
import os
import json
import glob
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

# Add the normalization module to the path
sys.path.append(str(Path(__file__).parent.parent / "normalization"))

from semantic_search import OntologySemanticSearch
from src.models import (
    OntologyMatch,
    OntologyMatchCandidate,
    CandidateWithMatches,
    ToolNormalizationOutput,
    NormalizedCandidate,
    NormalizationResult,
    ExtractedCandidate,
    CurationResult,
    BatchNormalizationResult,
    SampleResultEntry,
    KeyValue,
)
from src.models.curation_models import DiseaseCurationResult


class NormalizationError(Exception):
    """Custom exception for normalization errors."""

    pass


def get_default_ontologies_for_field(target_field: str) -> List[str]:
    """Get default ontologies for a target field."""
    ontology_mapping = get_ontology_mapping()
    ontologies = ontology_mapping.get(target_field.lower(), ["mondo"])

    # If no exact match, try case-insensitive partial matching
    if not ontologies or ontologies == ["mondo"]:
        target_lower = target_field.lower()
        for field_key, field_ontologies in ontology_mapping.items():
            if target_lower in field_key or field_key in target_lower:
                ontologies = field_ontologies
                break

    return ontologies


def find_candidates_files_impl(session_dir: str) -> Dict[str, Any]:
    """
    Find all candidates JSON files in the session directory.

    This function scans the session directory for files matching the pattern
    *_candidates.json that contain CurationResult data to be normalized.

    Parameters
    ----------
    session_dir : str
        The session directory to search

    Returns
    -------
    Dict[str, Any]
        Dictionary containing information about found candidates files
    """
    # Look for candidates files in the session directory and subdirectories
    pattern = os.path.join(session_dir, "**", "*_candidates.json")
    candidates_files = glob.glob(pattern, recursive=True)

    if not candidates_files:
        return {
            "success": False,
            "message": "No candidates files found in session directory",
            "session_directory": session_dir,
            "searched_pattern": pattern,
        }

    # Extract information about each file
    file_info = []
    for file_path in candidates_files:
        rel_path = os.path.relpath(file_path, session_dir)
        file_size = os.path.getsize(file_path)

        # Try to extract sample_id and target_field from filename
        filename = os.path.basename(file_path)
        if filename.endswith("_candidates.json"):
            base_name = filename[:-16]  # Remove "_candidates.json"
            parts = base_name.split("_")
            if len(parts) >= 2:
                sample_id = parts[0]
                target_field = "_".join(parts[1:])
            else:
                sample_id = base_name
                target_field = "unknown"
        else:
            sample_id = "unknown"
            target_field = "unknown"

        file_info.append(
            {
                "file_path": file_path,
                "relative_path": rel_path,
                "file_size": file_size,
                "sample_id": sample_id,
                "target_field": target_field,
                "filename": filename,
            }
        )

    return {
        "success": True,
        "candidates_files": file_info,
        "total_files": len(file_info),
        "session_directory": session_dir,
    }


def batch_normalize_session_impl(
    session_dir: str,
    target_field: str = "Disease",
    ontologies: Optional[List[str]] = None,
    top_k: int = 2,
    min_score: float = 0.0,
) -> Dict[str, Any]:
    """
    Normalize all candidates files in the session directory for a specific target field.

    This function finds all candidates files matching the target field, normalizes them
    using semantic search, and generates a comprehensive batch result.

    Parameters
    ----------
    session_dir : str
        The session directory containing candidates files
    target_field : str, default "Disease"
        The target metadata field to normalize (e.g., "Disease", "Tissue")
    ontologies : List[str], optional
        List of ontologies to search. If None, uses defaults based on target field
    top_k : int, default 5
        Number of top matches to return per ontology
    min_score : float, default 0.0
        Minimum similarity score threshold for matches

    Returns
    -------
    Dict[str, Any]
        Dictionary containing the batch normalization results
    """
    # Check for enum target fields that don't need normalization
    if target_field.lower() in ["sampletype", "sample_type"]:
        raise NormalizationError(
            f"Target field '{target_field}' is an enum field and does not require normalization. "
            f"This field should be processed by the curator agent only."
        )
    
    # Find all candidates files for the target field
    field_pattern = target_field.lower()
    pattern = os.path.join(session_dir, "**", f"*_{field_pattern}_candidates.json")
    candidates_files = glob.glob(pattern, recursive=True)

    if not candidates_files:
        return {
            "success": False,
            "message": f"No candidates files found for target field '{target_field}'",
            "session_directory": session_dir,
            "searched_pattern": pattern,
        }

    print(
        f"🔄 Batch normalizing {len(candidates_files)} files for field '{target_field}'"
    )

    # Process each file using the normalizer implementation
    sample_results = {}
    total_candidates_normalized = 0
    successful_normalizations = 0
    processing_errors = []

    for file_path in candidates_files:
        try:
            # Generate output file path
            output_file_path = file_path.replace("_candidates.json", "_normalized.json")

            # Delegate to the actual implementation
            result = normalize_candidates_file(
                candidates_file_path=file_path,
                output_file_path=output_file_path,
                ontologies=ontologies,
                top_k=top_k,
                min_score=min_score,
            )

            # Extract sample ID from filename
            filename = os.path.basename(file_path)
            sample_id = (
                filename.split("_")[0]
                if "_" in filename
                else filename.replace("_candidates.json", "")
            )

            sample_results[sample_id] = {
                "input_file": file_path,
                "output_file": output_file_path,
                "result": result,
                "candidates_count": result.total_candidates,
                "normalized_count": result.total_normalized,
                "status": "success",
            }

            total_candidates_normalized += result.total_candidates
            successful_normalizations += 1

            print(
                f"✅ {sample_id}: {result.total_normalized}/{result.total_candidates} candidates normalized"
            )

        except NormalizationError as e:
            sample_id = os.path.basename(file_path).split("_")[0]
            error_info = {
                "sample_id": sample_id,
                "file_path": file_path,
                "error": str(e),
                "error_type": "NormalizationError",
            }
            processing_errors.append(error_info)
            sample_results[sample_id] = {
                "input_file": file_path,
                "status": "error",
                "error": str(e),
            }
            print(f"❌ {sample_id}: Normalization error - {str(e)}")

        except Exception as e:
            sample_id = os.path.basename(file_path).split("_")[0]
            error_info = {
                "sample_id": sample_id,
                "file_path": file_path,
                "error": str(e),
                "error_type": "UnexpectedError",
            }
            processing_errors.append(error_info)
            sample_results[sample_id] = {
                "input_file": file_path,
                "status": "error",
                "error": str(e),
            }
            print(f"❌ {sample_id}: Unexpected error - {str(e)}")

    # Create batch result
    batch_result = BatchNormalizationResult(
        sample_results=sample_results,
        session_directory=session_dir,
        target_field=target_field,
        total_samples_processed=len(candidates_files),
        successful_normalizations=successful_normalizations,
        failed_normalizations=len(processing_errors),
        ontologies_searched=ontologies or get_default_ontologies_for_field(target_field),
        normalization_method="semantic_search",
        normalization_timestamp=datetime.now().isoformat(),
        normalization_tool_version="1.0.0",
        processing_summary=[
            KeyValue(key="top_k", value=str(top_k)),
            KeyValue(key="min_score", value=str(min_score)),
            KeyValue(key="total_candidates", value=str(total_candidates_normalized)),
        ],
    )

    # Save batch result
    batch_output_path = os.path.join(
        session_dir, f"batch_normalization_{target_field.lower()}_results.json"
    )
    with open(batch_output_path, "w") as f:
        json.dump(batch_result.dict(), f, indent=2)

    print(
        f"📊 Batch normalization complete: {successful_normalizations}/{len(candidates_files)} files processed successfully"
    )

    return {
        "success": True,
        "batch_result": batch_result.dict(),
        "batch_output_file": batch_output_path,
        "summary": {
            "total_files": len(candidates_files),
            "successful": successful_normalizations,
            "failed": len(processing_errors),
            "total_candidates": total_candidates_normalized,
        },
    }


def get_default_ontologies_for_field(target_field: str) -> List[str]:
    """Get default ontologies for a given target field."""
    ontology_mapping = get_ontology_mapping()
    return ontology_mapping.get(target_field.lower(), ["mondo", "efo"])


def get_ontology_mapping() -> Dict[str, List[str]]:
    """
    Get the mapping of target fields to appropriate ontologies.

    Returns:
        Dict[str, List[str]]: Mapping of field names to prioritized list of ontologies
    """
    return {
        "disease": ["mondo"],
        "tissue": ["uberon", "cl"],
        "cell_line": ["clo"],
        "cell line": ["clo"],
        "ethnicity": ["hancestro"],
        "developmental_stage": ["hsapdv"],
        "development_stage": ["hsapdv"],
        "gender": ["pato"],
        # Legacy mappings for backwards compatibility
        "organ": ["uberon"],
        "cell_type": ["efo"],
        "phenotype": ["pato"],
        "age": ["pato"],
        "ancestry": ["hancestro"],
        "anatomy": ["uberon"],
        "pathology": ["mondo"],
        "organism_part": ["uberon"],
        "sex": ["pato"],
        "strain": ["efo"],
    }


def get_available_ontologies() -> Dict[str, Dict[str, Any]]:
    """
    Get information about available ontologies and their dictionary files.

    Returns:
        Dict[str, Dict[str, Any]]: Information about available ontologies
    """
    field_to_dict = {
        "mondo": "mondo_terms.json",
        "efo": "efo_terms.json",
        "pato": "pato_terms.json",
        "uberon": "uberon_terms.json",
        "hancestro": "hancestro_terms.json",
        "hsapdv": "hsapdv_terms.json",
        "dron": "dron_terms.json",
        "clo": "clo_terms.json",
    }

    ontologies_info = {}
    dict_dir = Path(__file__).parent.parent / "normalization" / "dictionaries"

    for field, dict_file in field_to_dict.items():
        dict_path = dict_dir / dict_file
        if dict_path.exists():
            size_mb = dict_path.stat().st_size / (1024 * 1024)
            ontologies_info[field] = {
                "dictionary_file": dict_file,
                "file_size_mb": round(size_mb, 2),
                "available": True,
                "path": str(dict_path),
            }
        else:
            ontologies_info[field] = {
                "dictionary_file": dict_file,
                "available": False,
                "path": str(dict_path),
            }

    return ontologies_info


def semantic_search_ontology(
    query: str, ontology: str, top_k: int = 2, min_score: float = 0.5
) -> List[OntologyMatch]:
    """
    Perform semantic search against a specific ontology.

    Args:
        query (str): The query text to search for
        ontology (str): The ontology to search in (e.g., 'mondo', 'efo')
        top_k (int): Number of top results to return
        min_score (float): Minimum similarity score threshold (default 0.0)

    Returns:
        List[OntologyMatch]: List of ontology matches

    Raises:
        NormalizationError: If ontology is not available or search fails
    """
    available_ontologies = get_available_ontologies()

    if ontology not in available_ontologies:
        raise NormalizationError(f"Ontology '{ontology}' not recognized")

    if not available_ontologies[ontology]["available"]:
        raise NormalizationError(f"Ontology '{ontology}' dictionary not available")

    dict_path = available_ontologies[ontology]["path"]

    try:
        # Initialize semantic search
        semantic_search = OntologySemanticSearch(dict_path)

        # Load or build index
        semantic_search.load_index()

        # Perform search
        results = semantic_search.search(query, k=top_k)

        # Convert to OntologyMatch objects
        matches = []
        for term, ont_id, score in results:
            if score >= min_score:
                matches.append(
                    OntologyMatch(
                        term=term, term_id=ont_id, score=score, ontology=ontology
                    )
                )

        return matches

    except Exception as e:
        raise NormalizationError(f"Error searching ontology '{ontology}': {str(e)}")


def semantic_search_candidates_impl(
    curation_results_file: str,
    target_field: str = "Disease",
    ontologies: Optional[List[str]] = None,
    top_k: int = 2,
    min_score: float = 0.5,
) -> List[ToolNormalizationOutput]:
    """
    Perform semantic search on extracted candidates from a curation results file
    and return a list of ToolNormalizationOutput objects for LLM-based selection.
    
    The tool performs semantic search and returns the top 5 matches per candidate
    WITHOUT scores, allowing the LLM agent to make the final selection based on
    context, rationale, and biomedical knowledge.
    """

    # Check for enum target fields that don't need normalization
    if target_field.lower() in ["sampletype", "sample_type", "assay_type"]:
        raise NormalizationError(
            f"Target field '{target_field}' is an enum field and does not require normalization. "
            f"This field should be processed by the curator agent only."
        )

    try:
        # 1. Load appropriate curation result objects from the specified file
        with open(curation_results_file, "r") as f:
            curation_data = json.load(f)

        # Extract curation_results array from the output structure
        if isinstance(curation_data, dict) and "curation_results" in curation_data:
            curation_results_data = curation_data["curation_results"]
        else:
            curation_results_data = curation_data

        # Parse based on target field type - try DiseaseCurationResult for disease field
        curation_results = []
        if target_field.lower() == "disease":
            # Try to parse as DiseaseCurationResult, fall back to CurationResult
            for data in curation_results_data:
                try:
                    # Try DiseaseCurationResult first
                    disease_result = DiseaseCurationResult(**data)
                    # Convert DiseaseExtractedCandidate to ExtractedCandidate for processing
                    # Create a synthetic CurationResult for compatibility
                    synthetic_result = CurationResult(
                        tool_name=disease_result.tool_name,
                        sample_id=disease_result.sample_id,
                        target_field=disease_result.target_field,
                        series_candidates=[
                            ExtractedCandidate(
                                value=c.value,
                                confidence=c.confidence,
                                source=c.source,
                                context=c.context,
                                rationale=c.rationale,
                                prenormalized=f"{c.value} ({c.condition.value})"
                            ) for c in disease_result.series_candidates
                        ],
                        sample_candidates=[
                            ExtractedCandidate(
                                value=c.value,
                                confidence=c.confidence,
                                source=c.source,
                                context=c.context,
                                rationale=c.rationale,
                                prenormalized=f"{c.value} ({c.condition.value})"
                            ) for c in disease_result.sample_candidates
                        ],
                        abstract_candidates=[
                            ExtractedCandidate(
                                value=c.value,
                                confidence=c.confidence,
                                source=c.source,
                                context=c.context,
                                rationale=c.rationale,
                                prenormalized=f"{c.value} ({c.condition.value})"
                            ) for c in disease_result.abstract_candidates
                        ],
                        final_candidates=[
                            ExtractedCandidate(
                                value=c.value,
                                confidence=c.confidence,
                                source=c.source,
                                context=c.context,
                                rationale=c.rationale,
                                prenormalized=f"{c.value} ({c.condition.value})"
                            ) for c in disease_result.final_candidates
                        ],
                        sources_processed=disease_result.sources_processed,
                        processing_notes=disease_result.processing_notes,
                    )
                    curation_results.append(synthetic_result)
                except Exception:
                    # Fall back to regular CurationResult
                    curation_results.append(CurationResult(**data))
        else:
            # Parse as regular CurationResult for all other fields
            curation_results = [CurationResult(**data) for data in curation_results_data]

        # Determine ontologies to use if not specified
        if ontologies is None:
            ontologies = get_default_ontologies_for_field(target_field)

        # 3. Perform semantic search for all candidates and build output structure
        tool_outputs = []
        
        for curation_result in curation_results:
            candidates_with_matches = []
            
            # Process only the final_candidates (top 3) for this sample
            for candidate in curation_result.final_candidates:
                # Handle "None reported" cases - provide empty matches
                if candidate.value == "None reported":
                    candidates_with_matches.append(
                        CandidateWithMatches(
                            value=candidate.value,
                            confidence=candidate.confidence,
                            source=candidate.source,
                            context=candidate.context,
                            rationale=candidate.rationale,
                            prenormalized=candidate.prenormalized,
                            ontology_matches=[],  # No matches for "None reported"
                        )
                    )
                    continue
                
                # Handle "healthy" value for disease fields - provide synthetic match
                if target_field.lower() in ["disease"] and candidate.value.lower() == "healthy":
                    synthetic_match = OntologyMatchCandidate(
                        term="healthy control",
                        term_id="MONDO:0005047",
                        ontology="mondo"
                    )
                    candidates_with_matches.append(
                        CandidateWithMatches(
                            value=candidate.value,
                            confidence=candidate.confidence,
                            source=candidate.source,
                            context=candidate.context,
                            rationale=candidate.rationale,
                            prenormalized=candidate.prenormalized,
                            ontology_matches=[synthetic_match],
                        )
                    )
                    continue
                
                # Handle legacy "control [healthy]" format
                if target_field.lower() in ["disease"] and "control [healthy]" in candidate.value.lower():
                    synthetic_match = OntologyMatchCandidate(
                        term="healthy control",
                        term_id="MONDO:0005047",
                        ontology="mondo"
                    )
                    candidates_with_matches.append(
                        CandidateWithMatches(
                            value=candidate.value,
                            confidence=candidate.confidence,
                            source=candidate.source,
                            context=candidate.context,
                            rationale=candidate.rationale,
                            prenormalized=candidate.prenormalized,
                            ontology_matches=[synthetic_match],
                        )
                    )
                    continue
                
                # Perform semantic search and collect matches
                all_matches = []
                for ontology in ontologies:
                    searcher = OntologySemanticSearch(
                        f"src/normalization/dictionaries/{ontology}_terms.json"
                    )
                    searcher.load_index()
                    matches = searcher.search(candidate.value, k=top_k)
                    for term, term_id, score in matches:
                        if score >= min_score:
                            all_matches.append((term, term_id, ontology, score))
                
                # Sort by score and take top 5
                all_matches.sort(key=lambda x: x[3], reverse=True)
                top_5_matches = all_matches[:5]
                
                # Convert to OntologyMatchCandidate WITHOUT scores
                ontology_matches_no_scores = [
                    OntologyMatchCandidate(
                        term=match[0],
                        term_id=match[1],
                        ontology=match[2],
                        definition=None  # Could add later if needed
                    )
                    for match in top_5_matches
                ]
                
                # Create CandidateWithMatches
                candidates_with_matches.append(
                    CandidateWithMatches(
                        value=candidate.value,
                        confidence=candidate.confidence,
                        source=candidate.source,
                        context=candidate.context,
                        rationale=candidate.rationale,
                        prenormalized=candidate.prenormalized,
                        ontology_matches=ontology_matches_no_scores,
                    )
                )
            
            # Extract original candidate values for this sample
            original_candidates = []
            if curation_result.series_candidates:
                original_candidates.extend([c.value for c in curation_result.series_candidates])
            if curation_result.sample_candidates:
                original_candidates.extend([c.value for c in curation_result.sample_candidates])
            if curation_result.abstract_candidates:
                original_candidates.extend([c.value for c in curation_result.abstract_candidates])
            
            # Remove duplicates while preserving order
            seen = set()
            unique_original_candidates = []
            for candidate in original_candidates:
                if candidate not in seen:
                    seen.add(candidate)
                    unique_original_candidates.append(candidate)
            
            # Create ToolNormalizationOutput for this sample
            tool_output = ToolNormalizationOutput(
                sample_id=curation_result.sample_id,
                target_field=target_field,
                candidates_with_matches=candidates_with_matches,
                ontologies_searched=ontologies,
                original_candidates=unique_original_candidates,
                sources_processed=curation_result.sources_processed,
                processing_notes=curation_result.processing_notes,
            )
            tool_outputs.append(tool_output)
        
        # Return the list of tool outputs for LLM-based selection
        return tool_outputs

    except Exception as e:
        print(f"❌ semantic_search_candidates_impl error: {str(e)}")
        import traceback

        traceback.print_exc()
        # Re-raise to be caught by the tool wrapper
        raise


# Keep the old implementation for backwards compatibility with direct function calls
def semantic_search_candidates_impl_legacy(
    curation_results_file: str,
    target_field: str = "Disease",
    ontologies: Optional[List[str]] = None,
    top_k: int = 2,
    min_score: float = 0.5,
) -> BatchNormalizationResult:
    """
    Legacy implementation that returns BatchNormalizationResult.
    This is kept for backwards compatibility with scripts that call this directly.
    """
    # Check for enum target fields that don't need normalization
    if target_field.lower() in ["sampletype", "sample_type", "assay_type"]:
        raise NormalizationError(
            f"Target field '{target_field}' is an enum field and does not require normalization. "
            f"This field should be processed by the curator agent only."
        )

    try:
        # 1. Load appropriate curation result objects from the specified file
        with open(curation_results_file, "r") as f:
            curation_data = json.load(f)

        # Extract curation_results array from the output structure
        if isinstance(curation_data, dict) and "curation_results" in curation_data:
            curation_results_data = curation_data["curation_results"]
        else:
            curation_results_data = curation_data

        # Parse based on target field type
        if target_field.lower() == "disease":
            # Parse as DiseaseCurationResult
            curation_results = [DiseaseCurationResult(**data) for data in curation_results_data]
            
            # 2. Extract only final_candidates (top 3 per sample) from DiseaseExtractedCandidate
            all_candidates = []
            for res in curation_results:
                for candidate in res.final_candidates:
                    # Convert DiseaseExtractedCandidate to ExtractedCandidate for normalization
                    # We use the disease value as the candidate value
                    all_candidates.append(ExtractedCandidate(
                        value=candidate.value,  # The disease name
                        confidence=candidate.confidence,
                        source=candidate.source,
                        context=candidate.context,
                        rationale=candidate.rationale,
                        prenormalized=f"{candidate.value} (Control: {candidate.condition.value})"
                    ))
        else:
            # Parse as regular CurationResult
            curation_results = [CurationResult(**data) for data in curation_results_data]
            
            # 2. Extract only final_candidates (top 3 per sample)
            all_candidates = []
            for res in curation_results:
                all_candidates.extend(res.final_candidates)

        # Determine ontologies to use if not specified
        if ontologies is None:
            ontologies = get_default_ontologies_for_field(target_field)

        # 3. Perform semantic search for all candidates (return top 5 matches per candidate)
        normalized_candidates_map = {}
        for candidate in all_candidates:
            # Handle "None reported" cases - skip semantic search
            if candidate.value == "None reported":
                normalized_candidates_map[candidate.value] = NormalizedCandidate(
                    **candidate.model_dump(),
                    top_ontology_matches=[],
                    best_match=None,
                    normalization_confidence=None,
                    normalization_notes=["No normalization attempted - curator reported no candidates found"],
                )
                continue
            
            # Handle "healthy" value for disease fields - skip semantic search but mark as successful
            # This handles the new disease structure where healthy controls have value="healthy" 
            if target_field.lower() in ["disease"] and candidate.value.lower() == "healthy":
                # Create a synthetic ontology match for healthy controls
                synthetic_match = OntologyMatch(
                    term="healthy control",
                    term_id="MONDO:0005047", 
                    score=1.0,
                    ontology="mondo"
                )
                normalized_candidates_map[candidate.value] = NormalizedCandidate(
                    **candidate.model_dump(),
                    top_ontology_matches=[synthetic_match],
                    best_match=synthetic_match,
                    normalization_confidence=1.0,
                    normalization_notes=["Synthetic normalization for healthy/control samples"],
                )
                continue
            
            # Handle legacy "control [healthy]" format for backwards compatibility
            if target_field.lower() in ["disease"] and "control [healthy]" in candidate.value.lower():
                # Create a synthetic ontology match for control [healthy]
                synthetic_match = OntologyMatch(
                    term="healthy control",
                    term_id="MONDO:0005047", 
                    score=1.0,
                    ontology="mondo"
                )
                normalized_candidates_map[candidate.value] = NormalizedCandidate(
                    **candidate.model_dump(),
                    top_ontology_matches=[synthetic_match],
                    best_match=synthetic_match,
                    normalization_confidence=1.0,
                    normalization_notes=["Synthetic normalization for healthy/control samples (legacy format)"],
                )
                continue
            
            # Collect all matches across ontologies and get top 5
            all_matches = []
            for ontology in ontologies:
                searcher = OntologySemanticSearch(
                    f"src/normalization/dictionaries/{ontology}_terms.json"
                )
                searcher.load_index()
                matches = searcher.search(candidate.value, k=top_k)
                for term, term_id, score in matches:
                    if score >= min_score:
                        # Clamp score to avoid floating-point precision issues
                        clamped_score = min(score, 1.0)
                        all_matches.append(
                            OntologyMatch(
                                term=term,
                                term_id=term_id,
                                score=clamped_score,
                                ontology=ontology,
                            )
                        )

            # Sort by score and take top 5
            all_matches.sort(key=lambda x: x.score, reverse=True)
            top_5_matches = all_matches[:5]

            # Calculate overall normalization confidence as highest score
            normalization_confidence = top_5_matches[0].score if top_5_matches else None

            # Set best_match to the top match for legacy compatibility
            best_match = top_5_matches[0] if top_5_matches else None

            normalized_candidates_map[candidate.value] = NormalizedCandidate(
                **candidate.model_dump(),
                top_ontology_matches=top_5_matches,
                best_match=best_match,
                normalization_confidence=normalization_confidence,
                normalization_notes=[],
            )

        # 4. Construct the final BatchNormalizationResult object
        sample_results = []
        successful_normalizations = 0
        for res in curation_results:
            norm_series = [
                normalized_candidates_map[c.value]
                for c in res.series_candidates
                if c.value in normalized_candidates_map
                and (normalized_candidates_map[c.value].best_match or c.value in ["None reported", "control [healthy]"])
            ]
            norm_sample = [
                normalized_candidates_map[c.value]
                for c in res.sample_candidates
                if c.value in normalized_candidates_map
                and (normalized_candidates_map[c.value].best_match or c.value in ["None reported", "control [healthy]"])
            ]
            norm_abstract = [
                normalized_candidates_map[c.value]
                for c in res.abstract_candidates
                if c.value in normalized_candidates_map
                and (normalized_candidates_map[c.value].best_match or c.value in ["None reported", "control [healthy]"])
            ]

            # Separate different types of candidates
            all_norm_candidates = norm_series + norm_sample + norm_abstract
            normalizable_candidates = [c for c in all_norm_candidates if c.value not in ["None reported", "control [healthy]"]]
            control_healthy_candidates = [c for c in all_norm_candidates if c.value == "control [healthy]"]
            none_reported_candidates = [c for c in all_norm_candidates if c.value == "None reported"]
            
            if normalizable_candidates:
                successful_normalizations += len(normalizable_candidates)
                best_overall_match = max(
                    normalizable_candidates, key=lambda c: c.normalization_confidence or 0.0
                )
            elif control_healthy_candidates:
                # If only "control [healthy]" candidates exist, use the first one as best match
                best_overall_match = control_healthy_candidates[0]
            elif none_reported_candidates:
                # If only "None reported" candidates exist, use the first one as best match
                best_overall_match = none_reported_candidates[0]
            else:
                best_overall_match = None

            # Create final normalized candidates from the normalized final_candidates
            final_normalized_candidates = []
            for candidate in res.final_candidates:
                if candidate.value in normalized_candidates_map:
                    final_normalized_candidates.append(
                        normalized_candidates_map[candidate.value]
                    )

            # Legacy fields for backward compatibility
            final_candidate = (
                res.final_candidates[0].value if res.final_candidates else None
            )
            final_confidence = (
                res.final_candidates[0].confidence if res.final_candidates else None
            )
            # Handle special cases for final normalized terms
            if best_overall_match and best_overall_match.value == "None reported":
                final_normalized_term = "None reported"
                final_normalized_id = "None reported"
                final_ontology = "None reported"
            elif best_overall_match and best_overall_match.value == "control [healthy]":
                final_normalized_term = "sterile"
                final_normalized_id = "MONDO:0005047"
                final_ontology = "mondo"
            else:
                final_normalized_term = (
                    best_overall_match.best_match.term
                    if best_overall_match and best_overall_match.best_match
                    else None
                )
                final_normalized_id = (
                    best_overall_match.best_match.term_id
                    if best_overall_match and best_overall_match.best_match
                    else None
                )
                final_ontology = (
                    best_overall_match.best_match.ontology
                    if best_overall_match and best_overall_match.best_match
                    else None
                )

            # Extract original candidate values from the curation result
            original_candidates = []
            if res.series_candidates:
                original_candidates.extend([c.value for c in res.series_candidates])
            if res.sample_candidates:
                original_candidates.extend([c.value for c in res.sample_candidates])
            if res.abstract_candidates:
                original_candidates.extend([c.value for c in res.abstract_candidates])
            
            # Remove duplicates while preserving order
            seen = set()
            unique_original_candidates = []
            for candidate in original_candidates:
                if candidate not in seen:
                    seen.add(candidate)
                    unique_original_candidates.append(candidate)

            # Create the normalization result with both new and legacy fields
            normalization_result = NormalizationResult(
                # Basic identification
                tool_name="NormalizerAgent",
                sample_id=res.sample_id,
                target_field=res.target_field,
                
                # Input candidates (minimal reference to original extraction)
                original_candidates=unique_original_candidates,
                
                # Normalization results - the core output
                normalized_candidates=final_normalized_candidates,
                
                # Best normalization result
                best_normalized_result=final_normalized_candidates[0] if final_normalized_candidates else None,
                
                # Normalization-specific metadata
                normalization_method="semantic_search",
                ontologies_searched=ontologies,
                normalization_timestamp=datetime.now().isoformat(),
                normalization_tool_version="1.0.0",
                
                # Processing metadata
                sources_processed=res.sources_processed,
                processing_notes=res.processing_notes,
                
                # Quality indicators
                normalization_success=len(final_normalized_candidates) > 0,
                normalization_confidence=final_normalized_candidates[0].normalization_confidence if final_normalized_candidates else None,
                
                # Legacy fields for backward compatibility (deprecated)
                final_normalized_term=final_normalized_term,
                final_normalized_id=final_normalized_id,
                final_ontology=final_ontology,
            )

            # Wrap in SampleResultEntry as expected by BatchNormalizationResult
            sample_entry = SampleResultEntry(
                sample_id=res.sample_id, result=normalization_result
            )
            sample_results.append(sample_entry)

        session_dir = str(Path(curation_results_file).parent)

        return BatchNormalizationResult(
            sample_results=sample_results,
            session_directory=session_dir,
            target_field=target_field,
            total_samples_processed=len(all_candidates),
            successful_normalizations=successful_normalizations,
            failed_normalizations=len(all_candidates) - successful_normalizations,
            ontologies_searched=ontologies,
            normalization_method="semantic_search",
            normalization_timestamp=datetime.now().isoformat(),
            normalization_tool_version="1.0.0",
            processing_summary=[
                KeyValue(key="ontologies_used", value=", ".join(ontologies)),
                KeyValue(key="min_score_threshold", value=str(min_score)),
            ],
        )

    except Exception as e:
        print(f"❌ semantic_search_candidates_impl error: {str(e)}")
        import traceback

        traceback.print_exc()
        # Re-raise to be caught by the tool wrapper
        raise


def normalize_candidate_value(
    candidate: ExtractedCandidate,
    target_field: str,
    ontologies: Optional[List[str]] = None,
    top_k: int = 2,
    min_score: float = 0.0,
) -> NormalizedCandidate:
    """
    Normalize a single candidate value against appropriate ontologies.

    Args:
        candidate (ExtractedCandidate): The candidate to normalize
        target_field (str): The target metadata field
        ontologies (Optional[List[str]]): Specific ontologies to search
        top_k (int): Number of top matches to return per ontology
        min_score (float): Minimum similarity score threshold (default 0.0)

    Returns:
        NormalizedCandidate: The normalized candidate with ontology matches
    """
    # Check for boolean target fields that don't need normalization
    if target_field.lower() in ["sampletype", "sample_type"]:
        raise NormalizationError(
            f"Target field '{target_field}' is an enum field and does not require normalization. "
            f"This field should be processed by the curator agent only."
        )

    # Determine which ontologies to search
    if ontologies is None:
        field_ontology_map = get_ontology_mapping()
        field_key = target_field.lower().replace(" ", "_")
        ontologies = field_ontology_map.get(
            field_key, ["mondo", "efo"]
        )  # Default fallback

    all_matches = []
    normalization_notes = []

    # Search each ontology
    for ontology in ontologies:
        try:
            matches = semantic_search_ontology(
                query=candidate.value,
                ontology=ontology,
                top_k=top_k,
                min_score=min_score,
            )
            all_matches.extend(matches)

            if matches:
                normalization_notes.append(
                    f"Found {len(matches)} matches in {ontology}"
                )
            else:
                normalization_notes.append(f"No matches above threshold in {ontology}")

        except NormalizationError as e:
            normalization_notes.append(f"Error searching {ontology}: {str(e)}")
            continue

    # Sort all matches by score (descending)
    all_matches.sort(key=lambda x: x.score, reverse=True)

    # Take top 5 matches and determine overall confidence
    top_5_matches = all_matches[:5]
    best_match = top_5_matches[0] if top_5_matches else None
    normalization_confidence = best_match.score if best_match else 0.0

    # Create normalized candidate with both new and legacy fields
    normalized_candidate = NormalizedCandidate(
        value=candidate.value,
        confidence=candidate.confidence,
        source=candidate.source,
        context=candidate.context,
        rationale=candidate.rationale,
        prenormalized=candidate.prenormalized,
        top_ontology_matches=top_5_matches,
        best_match=best_match,  # Legacy field for compatibility
        normalization_confidence=normalization_confidence,
        normalization_notes=normalization_notes,
    )

    return normalized_candidate


def normalize_curation_result(
    curation_result: CurationResult,
    ontologies: Optional[List[str]] = None,
    top_k: int = 2,
    min_score: float = 0.0,
) -> NormalizationResult:
    """
    Normalize all candidates in a CurationResult.

    Args:
        curation_result (CurationResult): The curation result to normalize
        ontologies (Optional[List[str]]): Specific ontologies to search
        top_k (int): Number of top matches to return per ontology
        min_score (float): Minimum similarity score threshold (default 0.0)

    Returns:
        NormalizationResult: The normalized result
    """
    # Check for boolean target fields that don't need normalization
    if curation_result.target_field.lower() in ["sampletype", "sample_type"]:
        raise NormalizationError(
            f"Target field '{curation_result.target_field}' is an enum field and does not require normalization. "
            f"This field should be processed by the curator agent only."
        )

    # Extract original candidate values from the curation result
    original_candidates = []
    if curation_result.series_candidates:
        original_candidates.extend([c.value for c in curation_result.series_candidates])
    if curation_result.sample_candidates:
        original_candidates.extend([c.value for c in curation_result.sample_candidates])
    if curation_result.abstract_candidates:
        original_candidates.extend([c.value for c in curation_result.abstract_candidates])
    
    # Remove duplicates while preserving order
    seen = set()
    unique_original_candidates = []
    for candidate in original_candidates:
        if candidate not in seen:
            seen.add(candidate)
            unique_original_candidates.append(candidate)

    # Normalize only the final_candidates (top 3) - this is the core functionality
    final_normalized_candidates = []
    for candidate in curation_result.final_candidates:
        normalized = normalize_candidate_value(
            candidate, curation_result.target_field, ontologies, top_k, min_score
        )
        final_normalized_candidates.append(normalized)

    normalization_method = "semantic_search"

    # Determine which ontologies were searched
    ontologies_searched = ontologies or []
    if not ontologies_searched:
        field_ontology_map = get_ontology_mapping()
        field_key = curation_result.target_field.lower().replace(" ", "_")
        ontologies_searched = field_ontology_map.get(field_key, ["mondo", "efo"])

    # Determine legacy fields for backward compatibility
    final_candidate = None
    final_confidence = None
    final_normalized_term = None
    final_normalized_id = None
    final_ontology = None

    # Set legacy fields from first final_candidate if available
    if curation_result.final_candidates:
        top_candidate = curation_result.final_candidates[0]
        final_candidate = top_candidate.value
        final_confidence = top_candidate.confidence

        # Set legacy normalized fields from first normalized candidate if available
        if final_normalized_candidates:
            top_normalized = final_normalized_candidates[0]
            if top_normalized.best_match:
                final_normalized_term = top_normalized.best_match.term
                final_normalized_id = top_normalized.best_match.term_id
                final_ontology = top_normalized.best_match.ontology

    # Create the normalization result with both new and legacy fields
    normalization_result = NormalizationResult(
        # Basic identification
        tool_name="NormalizerAgent",
        sample_id=curation_result.sample_id,
        target_field=curation_result.target_field,
        
        # Input candidates (minimal reference to original extraction)
        original_candidates=unique_original_candidates,
        
        # Normalization results - the core output
        normalized_candidates=final_normalized_candidates,
        
        # Best normalization result
        best_normalized_result=final_normalized_candidates[0] if final_normalized_candidates else None,
        
        # Normalization-specific metadata
        normalization_method=normalization_method,
        ontologies_searched=ontologies_searched,
        normalization_timestamp=datetime.now().isoformat(),
        normalization_tool_version="1.0.0",
        
        # Processing metadata
        sources_processed=curation_result.sources_processed,
        processing_notes=curation_result.processing_notes,
        
        # Quality indicators
        normalization_success=len(final_normalized_candidates) > 0,
        normalization_confidence=final_normalized_candidates[0].normalization_confidence if final_normalized_candidates else None,
        
        # Legacy fields for backward compatibility (deprecated)
        final_normalized_term=final_normalized_term,
        final_normalized_id=final_normalized_id,
        final_ontology=final_ontology,
    )

    return normalization_result


def load_curation_result_from_file(file_path: str) -> CurationResult:
    """
    Load a CurationResult from a JSON file.

    Args:
        file_path (str): Path to the curation result JSON file

    Returns:
        CurationResult: The loaded curation result

    Raises:
        NormalizationError: If file cannot be loaded or parsed
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        return CurationResult(**data)

    except Exception as e:
        raise NormalizationError(
            f"Error loading curation result from {file_path}: {str(e)}"
        )


def save_normalization_result(result: NormalizationResult, file_path: str) -> None:
    """
    Save a NormalizationResult to a JSON file.

    Args:
        result (NormalizationResult): The normalization result to save
        file_path (str): Path where to save the result

    Raises:
        NormalizationError: If file cannot be saved
    """
    try:
        with open(file_path, "w") as f:
            json.dump(result.model_dump(), f, indent=2)

    except Exception as e:
        raise NormalizationError(
            f"Error saving normalization result to {file_path}: {str(e)}"
        )


def normalize_candidates_file(
    candidates_file_path: str,
    output_file_path: str,
    ontologies: Optional[List[str]] = None,
    top_k: int = 2,
    min_score: float = 0.5,
) -> NormalizationResult:
    """
    Normalize candidates from a JSON file and save the result.

    Args:
        candidates_file_path (str): Path to the candidates JSON file
        output_file_path (str): Path where to save the normalized result
        ontologies (Optional[List[str]]): Specific ontologies to search
        top_k (int): Number of top matches to return per ontology
        min_score (float): Minimum similarity score threshold (default 0.0)

    Returns:
        NormalizationResult: The normalization result

    Raises:
        NormalizationError: If processing fails
    """
    # Load the curation result
    curation_result = load_curation_result_from_file(candidates_file_path)

    # Normalize it
    normalization_result = normalize_curation_result(
        curation_result, ontologies, top_k, min_score
    )

    # Save the result
    save_normalization_result(normalization_result, output_file_path)

    return normalization_result


# Example usage and testing
if __name__ == "__main__":
    # Quiet testing - only show errors
    try:
        ontologies = get_available_ontologies()
        mapping = get_ontology_mapping()
        # Test basic functionality without verbose output
        available_onts = [k for k, v in ontologies.items() if v["available"]]
        if available_onts:
            test_queries = [("diabetes", "mondo"), ("heart", "uberon")]
            for query, ontology in test_queries:
                if ontology in available_onts:
                    semantic_search_ontology(query, ontology, top_k=3)
    except Exception as e:
        print(f"❌ Testing error: {e}")
