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
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

# Add the normalization module to the path
sys.path.append(str(Path(__file__).parent.parent / "normalization"))

from semantic_search import OntologySemanticSearch
from src.models import (
    OntologyMatch,
    NormalizedCandidate,
    NormalizationResult,
    NormalizationRequest,
    ExtractedCandidate,
    CurationResult,
    BatchNormalizationResult,
)


class NormalizationError(Exception):
    """Custom exception for normalization errors."""
    pass


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
            "searched_pattern": pattern
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
        
        file_info.append({
            "file_path": file_path,
            "relative_path": rel_path,
            "file_size": file_size,
            "sample_id": sample_id,
            "target_field": target_field,
            "filename": filename
        })
    
    return {
        "success": True,
        "candidates_files": file_info,
        "total_files": len(file_info),
        "session_directory": session_dir
    }


def batch_normalize_session_impl(
    session_dir: str,
    target_field: str = "Disease",
    ontologies: Optional[List[str]] = None,
    top_k: int = 5,
    min_score: float = 0.5
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
    min_score : float, default 0.5
        Minimum similarity score threshold for matches

    Returns
    -------
    Dict[str, Any]
        Dictionary containing the batch normalization results
    """
    # Find all candidates files for the target field
    field_pattern = target_field.lower()
    pattern = os.path.join(session_dir, "**", f"*_{field_pattern}_candidates.json")
    candidates_files = glob.glob(pattern, recursive=True)
    
    if not candidates_files:
        return {
            "success": False,
            "message": f"No candidates files found for target field '{target_field}'",
            "session_directory": session_dir,
            "searched_pattern": pattern
        }
    
    print(f"🔄 Batch normalizing {len(candidates_files)} files for field '{target_field}'")
    print(f"🔍 Ontologies: {ontologies or 'auto-detected'}")
    
    # Process each file using the normalizer implementation
    sample_results = {}
    total_candidates_normalized = 0
    successful_normalizations = 0
    processing_errors = []
    
    for file_path in candidates_files:
        try:
            # Generate output file path
            output_file_path = file_path.replace("_candidates.json", "_normalized.json")
            
            print(f"📄 Processing: {os.path.basename(file_path)}")
            
            # Delegate to the actual implementation
            result = normalize_candidates_file(
                candidates_file_path=file_path,
                output_file_path=output_file_path,
                ontologies=ontologies,
                top_k=top_k,
                min_score=min_score
            )
            
            # Extract sample ID from filename
            filename = os.path.basename(file_path)
            sample_id = filename.split("_")[0] if "_" in filename else filename.replace("_candidates.json", "")
            
            sample_results[sample_id] = {
                "input_file": file_path,
                "output_file": output_file_path,
                "result": result,
                "candidates_count": result.total_candidates,
                "normalized_count": result.total_normalized,
                "status": "success"
            }
            
            total_candidates_normalized += result.total_candidates
            successful_normalizations += 1
            
            print(f"✅ {sample_id}: {result.total_normalized}/{result.total_candidates} candidates normalized")
            
        except NormalizationError as e:
            sample_id = os.path.basename(file_path).split("_")[0]
            error_info = {
                "sample_id": sample_id,
                "file_path": file_path,
                "error": str(e),
                "error_type": "NormalizationError"
            }
            processing_errors.append(error_info)
            sample_results[sample_id] = {
                "input_file": file_path,
                "status": "error",
                "error": str(e)
            }
            print(f"❌ {sample_id}: Normalization error - {str(e)}")
            
        except Exception as e:
            sample_id = os.path.basename(file_path).split("_")[0]
            error_info = {
                "sample_id": sample_id,
                "file_path": file_path,
                "error": str(e),
                "error_type": "UnexpectedError"
            }
            processing_errors.append(error_info)
            sample_results[sample_id] = {
                "input_file": file_path,
                "status": "error",
                "error": str(e)
            }
            print(f"❌ {sample_id}: Unexpected error - {str(e)}")
    
    # Create batch result
    batch_result = BatchNormalizationResult(
        session_id=os.path.basename(session_dir),
        target_field=target_field,
        total_files_processed=len(candidates_files),
        successful_normalizations=successful_normalizations,
        failed_normalizations=len(processing_errors),
        total_candidates_normalized=total_candidates_normalized,
        processing_errors=processing_errors,
        sample_results=sample_results,
        normalization_timestamp=datetime.now().isoformat(),
        ontologies_used=ontologies or get_default_ontologies_for_field(target_field),
        parameters={
            "top_k": top_k,
            "min_score": min_score
        }
    )
    
    # Save batch result
    batch_output_path = os.path.join(session_dir, f"batch_normalization_{target_field.lower()}_results.json")
    with open(batch_output_path, 'w') as f:
        json.dump(batch_result.dict(), f, indent=2)
    
    print(f"📊 Batch normalization complete: {successful_normalizations}/{len(candidates_files)} files processed successfully")
    print(f"💾 Batch results saved to: {batch_output_path}")
    
    return {
        "success": True,
        "batch_result": batch_result.dict(),
        "batch_output_file": batch_output_path,
        "summary": {
            "total_files": len(candidates_files),
            "successful": successful_normalizations,
            "failed": len(processing_errors),
            "total_candidates": total_candidates_normalized
        }
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
        "disease": ["mondo", "efo"],
        "tissue": ["uberon", "efo"],
        "organ": ["uberon"],
        "cell_type": ["clo", "efo"],
        "phenotype": ["pato", "efo"],
        "age": ["pato", "hsapdv"],
        "development_stage": ["hsapdv", "efo"],
        "ancestry": ["hancestro"],
        "drug": ["dron"],
        "treatment": ["dron", "efo"],
        "compound": ["dron"],
        "anatomy": ["uberon"],
        "pathology": ["mondo", "pato"],
        "organism_part": ["uberon"],
        "developmental_stage": ["hsapdv"],
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
        "clo": "clo_terms.json"
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
                "path": str(dict_path)
            }
        else:
            ontologies_info[field] = {
                "dictionary_file": dict_file,
                "available": False,
                "path": str(dict_path)
            }
    
    return ontologies_info


def semantic_search_ontology(
    query: str, 
    ontology: str, 
    top_k: int = 5,
    min_score: float = 0.5
) -> List[OntologyMatch]:
    """
    Perform semantic search against a specific ontology.
    
    Args:
        query (str): The query text to search for
        ontology (str): The ontology to search in (e.g., 'mondo', 'efo')
        top_k (int): Number of top results to return
        min_score (float): Minimum similarity score threshold
        
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
                matches.append(OntologyMatch(
                    term=term,
                    term_id=ont_id,
                    score=score,
                    ontology=ontology
                ))
        
        return matches
        
    except Exception as e:
        raise NormalizationError(f"Error searching ontology '{ontology}': {str(e)}")


def normalize_candidate_value(
    candidate: ExtractedCandidate,
    target_field: str,
    ontologies: Optional[List[str]] = None,
    top_k: int = 5,
    min_score: float = 0.5
) -> NormalizedCandidate:
    """
    Normalize a single candidate value against appropriate ontologies.
    
    Args:
        candidate (ExtractedCandidate): The candidate to normalize
        target_field (str): The target metadata field
        ontologies (Optional[List[str]]): Specific ontologies to search
        top_k (int): Number of top matches to return per ontology
        min_score (float): Minimum similarity score threshold
        
    Returns:
        NormalizedCandidate: The normalized candidate with ontology matches
    """
    # Determine which ontologies to search
    if ontologies is None:
        field_ontology_map = get_ontology_mapping()
        field_key = target_field.lower().replace(" ", "_")
        ontologies = field_ontology_map.get(field_key, ["mondo", "efo"])  # Default fallback
    
    all_matches = []
    normalization_notes = []
    
    # Search each ontology
    for ontology in ontologies:
        try:
            matches = semantic_search_ontology(
                query=candidate.value,
                ontology=ontology,
                top_k=top_k,
                min_score=min_score
            )
            all_matches.extend(matches)
            
            if matches:
                normalization_notes.append(f"Found {len(matches)} matches in {ontology}")
            else:
                normalization_notes.append(f"No matches above threshold in {ontology}")
                
        except NormalizationError as e:
            normalization_notes.append(f"Error searching {ontology}: {str(e)}")
            continue
    
    # Sort all matches by score (descending)
    all_matches.sort(key=lambda x: x.score, reverse=True)
    
    # Determine best match and overall confidence
    best_match = all_matches[0] if all_matches else None
    normalization_confidence = best_match.score if best_match else 0.0
    
    # Create normalized candidate
    normalized_candidate = NormalizedCandidate(
        value=candidate.value,
        confidence=candidate.confidence,
        source=candidate.source,
        context=candidate.context,
        rationale=candidate.rationale,
        prenormalized=candidate.prenormalized,
        ontology_matches=all_matches,
        best_match=best_match,
        normalization_confidence=normalization_confidence,
        normalization_notes=normalization_notes
    )
    
    return normalized_candidate


def normalize_curation_result(
    curation_result: CurationResult,
    ontologies: Optional[List[str]] = None,
    top_k: int = 5,
    min_score: float = 0.5
) -> NormalizationResult:
    """
    Normalize all candidates in a CurationResult.
    
    Args:
        curation_result (CurationResult): The curation result to normalize
        ontologies (Optional[List[str]]): Specific ontologies to search
        top_k (int): Number of top matches to return per ontology
        min_score (float): Minimum similarity score threshold
        
    Returns:
        NormalizationResult: The normalized result
    """
    # Normalize candidates from each source
    normalized_series_candidates = []
    for candidate in curation_result.series_candidates:
        normalized = normalize_candidate_value(
            candidate, curation_result.target_field, ontologies, top_k, min_score
        )
        normalized_series_candidates.append(normalized)
    
    normalized_sample_candidates = []
    for candidate in curation_result.sample_candidates:
        normalized = normalize_candidate_value(
            candidate, curation_result.target_field, ontologies, top_k, min_score
        )
        normalized_sample_candidates.append(normalized)
    
    normalized_abstract_candidates = []
    for candidate in curation_result.abstract_candidates:
        normalized = normalize_candidate_value(
            candidate, curation_result.target_field, ontologies, top_k, min_score
        )
        normalized_abstract_candidates.append(normalized)
    
    # Determine final normalized result
    all_normalized_candidates = (
        normalized_series_candidates + 
        normalized_sample_candidates + 
        normalized_abstract_candidates
    )
    
    final_normalized_term = None
    final_normalized_id = None
    final_ontology = None
    normalization_method = "semantic_search"
    
    # Choose the best normalized match based on original confidence and normalization confidence
    if all_normalized_candidates:
        # Weight by original confidence * normalization confidence
        best_candidate = max(
            all_normalized_candidates,
            key=lambda x: x.confidence * (x.normalization_confidence or 0.0)
        )
        
        if best_candidate.best_match:
            final_normalized_term = best_candidate.best_match.term
            final_normalized_id = best_candidate.best_match.term_id
            final_ontology = best_candidate.best_match.ontology
    
    # Determine which ontologies were searched
    ontologies_searched = ontologies or []
    if not ontologies_searched:
        field_ontology_map = get_ontology_mapping()
        field_key = curation_result.target_field.lower().replace(" ", "_")
        ontologies_searched = field_ontology_map.get(field_key, ["mondo", "efo"])
    
    # Create the normalization result by copying all fields from curation_result
    # and adding the normalization-specific fields
    normalization_result = NormalizationResult(
        # Copy all fields from CurationResult
        tool_name=curation_result.tool_name,
        sample_id=curation_result.sample_id,
        target_field=curation_result.target_field,
        series_candidates=curation_result.series_candidates,
        sample_candidates=curation_result.sample_candidates,
        abstract_candidates=curation_result.abstract_candidates,
        final_candidate=curation_result.final_candidate,
        final_confidence=curation_result.final_confidence,
        reconciliation_needed=curation_result.reconciliation_needed,
        reconciliation_reason=curation_result.reconciliation_reason,
        sources_processed=curation_result.sources_processed,
        processing_notes=curation_result.processing_notes,
        # Add normalization-specific fields
        normalized_series_candidates=normalized_series_candidates,
        normalized_sample_candidates=normalized_sample_candidates,
        normalized_abstract_candidates=normalized_abstract_candidates,
        final_normalized_term=final_normalized_term,
        final_normalized_id=final_normalized_id,
        final_ontology=final_ontology,
        normalization_method=normalization_method,
        ontologies_searched=ontologies_searched,
        normalization_timestamp=datetime.now().isoformat(),
        normalization_tool_version="1.0.0"
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
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        return CurationResult(**data)
        
    except Exception as e:
        raise NormalizationError(f"Error loading curation result from {file_path}: {str(e)}")


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
        with open(file_path, 'w') as f:
            json.dump(result.model_dump(), f, indent=2)
            
    except Exception as e:
        raise NormalizationError(f"Error saving normalization result to {file_path}: {str(e)}")


def normalize_candidates_file(
    candidates_file_path: str,
    output_file_path: str,
    ontologies: Optional[List[str]] = None,
    top_k: int = 5,
    min_score: float = 0.5
) -> NormalizationResult:
    """
    Normalize candidates from a JSON file and save the result.
    
    Args:
        candidates_file_path (str): Path to the candidates JSON file
        output_file_path (str): Path where to save the normalized result
        ontologies (Optional[List[str]]): Specific ontologies to search
        top_k (int): Number of top matches to return per ontology
        min_score (float): Minimum similarity score threshold
        
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
    # Test the normalization tools
    print("Testing normalization tools...")
    
    # Test available ontologies
    print(f"\n{'='*60}")
    print("Available Ontologies:")
    print(f"{'='*60}")
    
    ontologies = get_available_ontologies()
    for field, info in ontologies.items():
        if info['available']:
            print(f"  {field}: {info['dictionary_file']} ({info['file_size_mb']} MB)")
        else:
            print(f"  {field}: {info['dictionary_file']} (not available)")
    
    # Test ontology mapping
    print(f"\n{'='*60}")
    print("Target Field -> Ontology Mapping:")
    print(f"{'='*60}")
    
    mapping = get_ontology_mapping()
    for field, onts in mapping.items():
        print(f"  {field}: {', '.join(onts)}")
    
    # Test semantic search if ontologies are available
    available_onts = [k for k, v in ontologies.items() if v['available']]
    if available_onts:
        print(f"\n{'='*60}")
        print("Testing Semantic Search:")
        print(f"{'='*60}")
        
        test_queries = [
            ("diabetes", "mondo"),
            ("heart", "uberon"),
            ("cancer", "efo"),
        ]
        
        for query, ontology in test_queries:
            if ontology in available_onts:
                try:
                    matches = semantic_search_ontology(query, ontology, top_k=3)
                    print(f"\nQuery: '{query}' in {ontology}")
                    for i, match in enumerate(matches, 1):
                        print(f"  {i}. {match.term} → {match.term_id} (score: {match.score:.4f})")
                except NormalizationError as e:
                    print(f"Error: {e}")
    else:
        print("\nNo ontology dictionaries available for testing.")



