"""
Shared batch output utilities for consistent output format between eval and classic modes.

This module provides common utilities for:
- BatchContext: Encapsulates batch metadata
- build_batch_targets_output: Creates standardized output format
- filter_normalization_for_batch: Filters normalization data for specific batch
- write_batch_targets_output: Atomically writes batch output to correct directory
"""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from src.tools.batch_processing_tools import convert_normalization_data_to_unified_format


@dataclass
class BatchContext:
    """Encapsulates batch metadata for consistent batch handling."""
    
    session_directory: Path
    sample_type: str
    batch_idx: int
    batch_samples: List[str]
    
    def __post_init__(self):
        """Ensure session_directory is a Path object."""
        if isinstance(self.session_directory, str):
            self.session_directory = Path(self.session_directory)
    
    @property
    def batch_name(self) -> str:
        """Get the batch name in standard format."""
        return f"{self.sample_type}_batch_{self.batch_idx}"
    
    @property
    def batch_dir(self) -> Path:
        """Get the batch directory path."""
        return self.session_directory / "conditional_processing" / self.batch_name
    
    @property
    def batch_targets_file(self) -> Path:
        """Get the path to batch_targets_output.json."""
        return self.batch_dir / "batch_targets_output.json"


def _safe_serialize(obj: Any) -> Any:
    """
    Safely serialize an object to JSON-compatible format.
    
    Handles Pydantic models, dataclasses, and other complex objects.
    """
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    
    if isinstance(obj, list):
        return [_safe_serialize(item) for item in obj]
    
    if isinstance(obj, dict):
        return {str(k): _safe_serialize(v) for k, v in obj.items()}
    
    # Pydantic v2 models
    if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
        try:
            return _safe_serialize(obj.model_dump())
        except Exception:
            pass
    
    # Pydantic v1 models
    if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
        try:
            return _safe_serialize(obj.dict())
        except Exception:
            pass
    
    # Fallback to string representation
    try:
        return str(obj)
    except Exception:
        return None


def filter_normalization_for_batch(
    normalization_data: Dict[str, Dict[str, Any]],
    batch_samples: List[str],
) -> Dict[str, Dict[str, Any]]:
    """
    Filter normalization results to only include specified samples.
    
    Parameters
    ----------
    normalization_data : Dict[str, Dict[str, Any]]
        Full normalization data in format: {field_name: {sample_id: result}}
    batch_samples : List[str]
        List of sample IDs to include in the filtered result
        
    Returns
    -------
    Dict[str, Dict[str, Any]]
        Filtered normalization data containing only the specified samples
    """
    if not normalization_data or not batch_samples:
        return {}
    
    batch_samples_set = set(batch_samples)
    filtered = {}
    
    for field_name, samples_data in normalization_data.items():
        if not isinstance(samples_data, dict):
            continue
        
        filtered_samples = {
            sample_id: result
            for sample_id, result in samples_data.items()
            if sample_id in batch_samples_set
        }
        
        if filtered_samples:
            filtered[field_name] = filtered_samples
    
    return filtered


def build_batch_targets_output(
    batch_context: BatchContext,
    conditional_result: Optional[Any] = None,
    normalization_data: Optional[Dict[str, Dict[str, Any]]] = None,
    execution_time_seconds: float = 0.0,
    target_fields_processed: Optional[List[str]] = None,
    normalization_fields_processed: Optional[List[str]] = None,
    not_applicable_fields: Optional[List[str]] = None,
    additional_fields: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build the standard batch_targets_output.json structure.
    
    This function produces a consistent output format that works with both
    eval and classic modes, including both flat and nested normalization formats.
    
    Parameters
    ----------
    batch_context : BatchContext
        Batch metadata
    conditional_result : Optional[Any]
        Conditional processing result (ConditionalProcessingResult or dict)
    normalization_data : Optional[Dict[str, Dict[str, Any]]]
        Normalization data in format: {field_name: {sample_id: {normalized_term, term_id, ...}}}
    execution_time_seconds : float
        Execution time for this batch
    target_fields_processed : Optional[List[str]]
        List of target fields that were processed for curation
    normalization_fields_processed : Optional[List[str]]
        List of fields that were normalized
    not_applicable_fields : Optional[List[str]]
        List of fields not applicable for this sample type
    additional_fields : Optional[Dict[str, Any]]
        Additional fields to include in the output
        
    Returns
    -------
    Dict[str, Any]
        Complete batch_targets_output.json structure
    """
    # Build base output structure
    output: Dict[str, Any] = {
        "success": True,
        "batch_name": batch_context.batch_name,
        "sample_type": batch_context.sample_type,
        "batch_samples": list(batch_context.batch_samples),
        "execution_time_seconds": execution_time_seconds,
        "batch_directory": str(batch_context.batch_dir),
        "target_fields_processed": target_fields_processed or [],
        "normalization_fields_processed": normalization_fields_processed or [],
        "timestamp": datetime.now().isoformat(),
    }
    
    # Add not_applicable_fields if provided
    if not_applicable_fields:
        output["not_applicable_fields"] = not_applicable_fields
    
    # Add conditional_result if provided
    if conditional_result is not None:
        output["conditional_result"] = _safe_serialize(conditional_result)
    
    # Add normalization data in BOTH formats for compatibility
    if normalization_data and isinstance(normalization_data, dict) and len(normalization_data) > 0:
        # 1. Flat format at top level (for CSV extraction - preferred)
        # This is the format that classic mode uses: {field_name: {sample_id: {normalized_term, ...}}}
        for field_name, samples_data in normalization_data.items():
            if isinstance(samples_data, dict):
                output[field_name] = samples_data
        
        # 2. Nested format for backward compatibility (eval mode legacy format)
        # This is the format: {"normalization_results": {field_name: {"sample_results": [...]}}}
        try:
            unified_format = convert_normalization_data_to_unified_format(normalization_data)
            if unified_format and "normalization_results" in unified_format:
                output["normalization_results"] = unified_format["normalization_results"]
        except Exception as e:
            print(f"⚠️ Warning: Failed to convert normalization data to unified format: {e}")
    
    # Add any additional fields
    if additional_fields:
        output.update(additional_fields)
    
    return output


def _atomic_write_json(path: Path, data: Dict[str, Any], indent: int = 2) -> None:
    """
    Write JSON atomically to avoid truncated/corrupted files.
    
    Uses a temporary file and atomic rename to ensure file integrity.
    """
    # Ensure parent directory exists
    path.parent.mkdir(parents=True, exist_ok=True)
    
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    
    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent)
            f.flush()
            # Sync to disk
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        
        # Atomic rename
        try:
            tmp_path.replace(path)
        except Exception:
            # Fallback for systems that don't support replace
            tmp_path.rename(path)
    except Exception as e:
        # Clean up temp file on failure
        try:
            tmp_path.unlink()
        except Exception:
            pass
        raise e


def write_batch_targets_output(
    batch_context: BatchContext,
    output_data: Dict[str, Any],
    merge_with_existing: bool = True,
) -> Path:
    """
    Write batch_targets_output.json to the batch directory atomically.
    
    Parameters
    ----------
    batch_context : BatchContext
        Batch metadata
    output_data : Dict[str, Any]
        Data to write
    merge_with_existing : bool
        If True and file exists, merge with existing data (new data takes precedence)
        
    Returns
    -------
    Path
        Path to the written file
    """
    # Ensure batch directory exists
    batch_context.batch_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = batch_context.batch_targets_file
    
    # Optionally merge with existing data
    if merge_with_existing and output_file.exists():
        try:
            with output_file.open("r", encoding="utf-8") as f:
                existing_data = json.load(f)
            
            # Merge: existing data first, then new data (new takes precedence)
            merged = {**existing_data, **output_data}
            output_data = merged
        except Exception as e:
            print(f"⚠️ Warning: Failed to read existing batch_targets_output.json: {e}")
    
    # Write atomically
    _atomic_write_json(output_file, output_data)
    
    print(f"✅ Wrote batch_targets_output.json to {batch_context.batch_name} "
          f"({len(batch_context.batch_samples)} samples)")
    
    return output_file


def ensure_composite_keys(
    curator_outputs: Dict[str, Any],
    sample_type: str,
) -> Dict[str, Any]:
    """
    Ensure all curator output keys use composite format (sample_type::field).
    
    Parameters
    ----------
    curator_outputs : Dict[str, Any]
        Dictionary of field_name or composite_key -> curator_output
    sample_type : str
        Sample type to use for composite key prefix
        
    Returns
    -------
    Dict[str, Any]
        Dictionary with all keys in composite format
    """
    normalized = {}
    
    for key, value in curator_outputs.items():
        if "::" in key:
            # Already composite, use as-is
            normalized[key] = value
        else:
            # Simple key, convert to composite
            composite_key = f"{sample_type}::{key}"
            normalized[composite_key] = value
    
    return normalized


def extract_field_from_composite_key(composite_key: str) -> tuple:
    """
    Extract sample_type and field_name from a composite key.
    
    Parameters
    ----------
    composite_key : str
        Key in format "sample_type::field_name" or just "field_name"
        
    Returns
    -------
    tuple
        (sample_type, field_name) - sample_type is None if not composite
    """
    if "::" in composite_key:
        parts = composite_key.split("::", 1)
        return (parts[0], parts[1]) if len(parts) == 2 else (None, composite_key)
    return (None, composite_key)



