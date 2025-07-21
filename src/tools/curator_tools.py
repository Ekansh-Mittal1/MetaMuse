"""
Curator tools for metadata curation and candidate extraction.

This module provides tools for the CuratorAgent to perform metadata
curation tasks on GEO samples, including extracting candidate values
for specific metadata fields and reconciling conflicts.
"""

import json
import os
import re
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from openai import OpenAI
from pydantic import BaseModel, Field

# Import new Pydantic models
from src.models import (
    CuratorResult,
    CandidateExtraction,
    ModelSerializer
)


class ExtractionCandidate(BaseModel):
    """Pydantic model for a single extracted candidate."""
    
    value: str = Field(..., description="The extracted candidate value")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score between 0.0 and 1.0")
    context: str = Field(..., description="Brief context where the candidate was found")


class ExtractionResponse(BaseModel):
    """Pydantic model for LLM extraction response."""
    
    candidates: List[ExtractionCandidate] = Field(default_factory=list, description="List of extracted candidates")


def load_extraction_template(target_field: str) -> str:
    """
    Load extraction template for a specific target field.
    
    Parameters
    ----------
    target_field : str
        The target metadata field (e.g., "Disease", "Tissue", "Age", "Treatment")
        
    Returns
    -------
    str
        The loaded template content
    """
    template_file = Path(__file__).parent.parent / "prompts" / "extraction_templates" / f"{target_field.lower()}.md"
    
    if not template_file.exists():
        # Fallback to generic template if specific one doesn't exist
        raise FileNotFoundError(f"No extraction template found for field: {target_field}")
    
    with open(template_file, 'r', encoding='utf-8') as f:
        return f.read()


class CuratorTools:
    """
    Tools for metadata curation and candidate extraction.
    
    This class provides methods to:
    - Load sample data from linked_data.json files
    - Extract metadata candidates for specific fields using LLM calls
    - Reconcile candidates across multiple files
    - Save curation results
    """
    
    def __init__(self, session_dir: str):
        """
        Initialize CuratorTools.
        
        Parameters
        ----------
        session_dir : str
            Path to the session directory containing sample data
        """
        self.session_dir = Path(session_dir)
        
        # Initialize LLM client
        self.llm_client = OpenAI(
            base_url=os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
            api_key=os.getenv("OPENROUTER_API_KEY"),
            default_headers={
                "HTTP-Referer": "localhost",
                "X-Title": "MetaMuse Curator",
                "X-App-Name": "MetaMuse",
            },
        )
        self.model_name = "google/gemini-2.5-flash"
    
    def load_sample_data(self, sample_id: str) -> CuratorResult:
        """
        Load sample data from linked_data.json and all referenced cleaned files.
        
        Parameters
        ----------
        sample_id : str
            The sample ID (e.g., GSM1000981) to load data for
            
        Returns
        -------
        CuratorResult
            Result containing loaded data from all relevant files
        """
        try:
            # First load the mapping file to find the correct series directory
            mapping_file = self.session_dir / "series_sample_mapping.json"
            if not mapping_file.exists():
                return CuratorResult(
                    success=False,
                    message=f"Mapping file not found: {mapping_file}"
                )
            
            with open(mapping_file, 'r') as f:
                mapping_data = json.load(f)
            
            # Find series for this sample
            reverse_mapping = mapping_data.get("reverse_mapping", {})
            if sample_id not in reverse_mapping:
                return CuratorResult(
                    success=False,
                    message=f"Sample {sample_id} not found in mapping"
                )
            
            series_id = reverse_mapping[sample_id]
            series_dir = self.session_dir / series_id
            
            # Load the linked_data.json file
            linked_data_file = series_dir / f"{sample_id}_linked_data.json"
            if not linked_data_file.exists():
                return CuratorResult(
                    success=False,
                    message=f"Linked data file not found: {linked_data_file}"
                )
            
            with open(linked_data_file, 'r') as f:
                linked_data = json.load(f)
            
            # Load all cleaned files referenced in the linked_data
            cleaned_files_data = {}
            cleaned_files = linked_data.get("cleaned_files", [])
            
            for cleaned_file_path in cleaned_files:
                cleaned_file = Path(cleaned_file_path)
                if cleaned_file.exists():
                    with open(cleaned_file, 'r') as f:
                        cleaned_files_data[cleaned_file.name] = json.load(f)
                else:
                    print(f"Warning: Cleaned file not found: {cleaned_file}")
            
            # Compile all data
            all_data = {
                "sample_id": sample_id,
                "series_id": series_id,
                "linked_data": linked_data,
                "cleaned_files": cleaned_files_data,
                "series_directory": str(series_dir)
            }
            
            return CuratorResult(
                success=True,
                message=f"Successfully loaded data for sample {sample_id}",
                data=all_data
            )
            
        except Exception as e:
            error_msg = f"Error loading sample data: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ CURATOR ERROR: {error_msg}")
            return CuratorResult(
                success=False,
                message=error_msg
            )
    
    def extract_metadata_candidates(
        self, 
        sample_data: Dict[str, Any], 
        target_field: str
    ) -> CuratorResult:
        """
        Extract potential candidates for the target metadata field from all files using LLM.
        
        Parameters
        ----------
        sample_data : Dict[str, Any]
            The sample data loaded from load_sample_data
        target_field : str
            The target metadata field to extract candidates for (e.g., "Disease", "Tissue", "Age")
            
        Returns
        -------
        CuratorResult
            Result containing candidates extracted from each file
        """
        try:
            candidates_by_file = {}
            
            # Extract from linked_data.json sample metadata
            linked_data = sample_data.get("linked_data", {})
            sample_metadata = linked_data.get("sample_metadata", {})
            
            linked_candidates = self._extract_candidates_with_llm(
                sample_metadata, target_field
            )
            if linked_candidates:
                candidates_by_file["linked_data.json"] = linked_candidates
            
            # Extract from each cleaned file
            cleaned_files = sample_data.get("cleaned_files", {})
            for filename, file_data in cleaned_files.items():
                file_candidates = self._extract_candidates_with_llm(
                    file_data, target_field
                )
                if file_candidates:
                    candidates_by_file[filename] = file_candidates
            
            return CuratorResult(
                success=True,
                message=f"Extracted candidates for {target_field} from {len(candidates_by_file)} files",
                candidates=candidates_by_file
            )
            
        except Exception as e:
            error_msg = f"Error extracting candidates: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ CURATOR ERROR: {error_msg}")
            return CuratorResult(
                success=False,
                message=error_msg
            )
    
    def _extract_candidates_with_llm(
        self, 
        data: Dict[str, Any], 
        target_field: str
    ) -> List[Dict[str, Any]]:
        """
        Extract candidates for a target field from a data structure using LLM.
        
        Parameters
        ----------
        data : Dict[str, Any]
            The data structure to search
        target_field : str
            The target field to extract candidates for
            
        Returns
        -------
        List[Dict[str, Any]]
            List of candidate dictionaries with value, confidence, and context
        """
        try:
            # Convert data to searchable text
            flattened_text = self._flatten_to_text(data)
            
            # Skip if no meaningful text content
            if len(flattened_text.strip()) < 10:
                return []
            
            # Load extraction template
            template = load_extraction_template(target_field)
            
            # Create LLM prompt
            prompt = f"{template}\n\n## Text to Analyze:\n{flattened_text}"
            
            # Make LLM call
            response = self.llm_client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": "You are a metadata extraction specialist. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=1000
            )
            
            # Parse response
            response_text = response.choices[0].message.content.strip()
            
            # Extract JSON from response (handle markdown code blocks)
            if "```json" in response_text:
                json_start = response_text.find("```json") + 7
                json_end = response_text.find("```", json_start)
                response_text = response_text[json_start:json_end]
            elif "```" in response_text:
                json_start = response_text.find("```") + 3
                json_end = response_text.rfind("```")
                response_text = response_text[json_start:json_end]
            
            # Parse JSON and validate with Pydantic
            response_data = json.loads(response_text)
            extraction_response = ExtractionResponse(**response_data)
            
            # Convert to dictionary format expected by existing code
            candidates = []
            for candidate in extraction_response.candidates:
                candidates.append({
                    "value": candidate.value,
                    "confidence": candidate.confidence,
                    "context": candidate.context
                })
            
            return candidates
            
        except Exception as e:
            print(f"❌ LLM extraction error for {target_field}: {str(e)}")
            return []
    
    def _flatten_to_text(self, data: Any, prefix: str = "") -> str:
        """
        Flatten a nested data structure to searchable text.
        
        Parameters
        ----------
        data : Any
            The data to flatten
        prefix : str
            Prefix for nested keys
            
        Returns
        -------
        str
            Flattened text representation
        """
        text_parts = []
        
        if isinstance(data, dict):
            for key, value in data.items():
                full_key = f"{prefix}.{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    text_parts.append(self._flatten_to_text(value, full_key))
                else:
                    text_parts.append(f"{full_key}: {str(value)}")
        elif isinstance(data, list):
            for i, item in enumerate(data):
                text_parts.append(self._flatten_to_text(item, f"{prefix}[{i}]"))
        else:
            text_parts.append(str(data))
        
        return " ".join(text_parts)
    

    
    def reconcile_candidates(
        self, 
        candidates_by_file: Dict[str, List[Dict[str, Any]]], 
        target_field: str
    ) -> CuratorResult:
        """
        Dummy reconciliation function that returns 'reconciliation required'.
        
        Parameters
        ----------
        candidates_by_file : Dict[str, List[Dict[str, Any]]]
            Candidates extracted from each file with confidence scores
        target_field : str
            The target metadata field
            
        Returns
        -------
        CuratorResult
            Result indicating reconciliation is required
        """
        try:
            # Dummy reconciliation - always returns "reconciliation required"
            final_result = {
                "target_field": target_field,
                "reconciliation_status": "reconciliation required",
                "candidates_by_file": candidates_by_file,
                "total_files_processed": len(candidates_by_file),
                "total_candidates": sum(len(candidates) for candidates in candidates_by_file.values())
            }
            
            return CuratorResult(
                success=True,
                message=f"Candidates extracted for {target_field} - reconciliation required",
                data=final_result
            )
            
        except Exception as e:
            error_msg = f"Error reconciling candidates: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ CURATOR ERROR: {error_msg}")
            return CuratorResult(
                success=False,
                message=error_msg
            )
    
    def _normalize_candidate(self, candidate: str) -> str:
        """
        Normalize a candidate for comparison.
        
        Parameters
        ----------
        candidate : str
            The candidate to normalize
            
        Returns
        -------
        str
            Normalized candidate
        """
        # Convert to lowercase, remove extra spaces, and standardize
        normalized = candidate.lower().strip()
        normalized = re.sub(r'\s+', ' ', normalized)
        
        # Common normalizations for diseases
        disease_normalizations = {
            'dlbcl': 'diffuse large b cell lymphoma',
            'diffuse large b-cell lymphoma': 'diffuse large b cell lymphoma',
            'breast ca': 'breast cancer',
            'lung ca': 'lung cancer',
        }
        
        return disease_normalizations.get(normalized, normalized)
    
    def reconcile_candidates_placeholder(
        self, 
        sample_id: str, 
        target_field: str, 
        conflicting_data: Dict[str, Any]
    ) -> CuratorResult:
        """
        Placeholder function for reconciliation when there are conflicts.
        
        This function will be implemented later with more sophisticated
        reconciliation logic, potentially including external APIs or
        manual review workflows.
        
        Parameters
        ----------
        sample_id : str
            The sample ID
        target_field : str
            The target metadata field
        conflicting_data : Dict[str, Any]
            The conflicting candidate data
            
        Returns
        -------
        CuratorResult
            Result with placeholder reconciliation
        """
        return CuratorResult(
            success=True,
            message=f"Placeholder reconciliation for {sample_id} field {target_field}",
            data={
                "target_field": target_field,
                "final_candidate": "NEEDS_MANUAL_REVIEW",
                "confidence": "manual_review_required",
                "reconciliation_method": "placeholder",
                "original_conflicts": conflicting_data
            }
        )
    
    def save_curator_results(
        self, 
        sample_id: str, 
        results_data: Dict[str, Any]
    ) -> CuratorResult:
        """
        Save curation results to a JSON file.
        
        Parameters
        ----------
        sample_id : str
            The sample ID
        results_data : Dict[str, Any]
            The results data to save
            
        Returns
        -------
        CuratorResult
            Result indicating success or failure of save operation
        """
        try:
            output_file = self.session_dir / f"{sample_id}_metadata_candidates.json"
            
            # Add metadata to results
            final_results = {
                "sample_id": sample_id,
                "curation_timestamp": json.dumps(None),  # Will be set by JSON encoder
                "curation_results": results_data
            }
            
            with open(output_file, 'w') as f:
                json.dump(final_results, f, indent=2, default=str)
            
            return CuratorResult(
                success=True,
                message=f"Successfully saved results for {sample_id}",
                files_created=[str(output_file)]
            )
            
        except Exception as e:
            error_msg = f"Error saving results: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(f"❌ CURATOR ERROR: {error_msg}")
            return CuratorResult(
                success=False,
                message=error_msg
            )


# Implementation functions for tool_utils.py
def load_sample_data_impl(sample_id: str, session_dir: str) -> Dict[str, Any]:
    """
    Load sample data from linked_data.json and all referenced cleaned files.
    
    Parameters
    ----------
    sample_id : str
        The sample ID to load data for
    session_dir : str
        Path to the session directory
        
    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and data
    """
    tools = CuratorTools(session_dir)
    result = tools.load_sample_data(sample_id)
    return {
        "success": result.success,
        "message": result.message,
        "data": result.data
    }


def extract_metadata_candidates_impl(
    sample_data: Dict[str, Any], 
    target_field: str, 
    session_dir: str
) -> Dict[str, Any]:
    """
    Extract potential candidates for the target metadata field from all files.
    
    Parameters
    ----------
    sample_data : Dict[str, Any]
        The sample data loaded from load_sample_data
    target_field : str
        The target metadata field to extract candidates for
    session_dir : str
        Path to the session directory
        
    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and candidates
    """
    tools = CuratorTools(session_dir)
    result = tools.extract_metadata_candidates(sample_data, target_field)
    return {
        "success": result.success,
        "message": result.message,
        "candidates": result.candidates
    }


def reconcile_candidates_impl(
    candidates_by_file: Dict[str, List[str]], 
    target_field: str, 
    session_dir: str
) -> Dict[str, Any]:
    """
    Reconcile candidates across files and determine final result.
    
    Parameters
    ----------
    candidates_by_file : Dict[str, List[str]]
        Candidates extracted from each file
    target_field : str
        The target metadata field
    session_dir : str
        Path to the session directory
        
    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and reconciled data
    """
    tools = CuratorTools(session_dir)
    result = tools.reconcile_candidates(candidates_by_file, target_field)
    return {
        "success": result.success,
        "message": result.message,
        "data": result.data
    }


def save_curator_results_impl(
    sample_id: str, 
    results_data: Dict[str, Any], 
    session_dir: str
) -> Dict[str, Any]:
    """
    Save curation results to a JSON file.
    
    Parameters
    ----------
    sample_id : str
        The sample ID
    results_data : Dict[str, Any]
        The results data to save
    session_dir : str
        Path to the session directory
        
    Returns
    -------
    Dict[str, Any]
        Result dictionary with success status and files created
    """
    tools = CuratorTools(session_dir)
    result = tools.save_curator_results(sample_id, results_data)
    return {
        "success": result.success,
        "message": result.message,
        "files_created": result.files_created
    } 