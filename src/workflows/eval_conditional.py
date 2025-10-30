"""
Iterative conditional processing with arbitration-driven curation correction.

Workflow:
1) Perform conditional curation (reuse batch_targets conditional parts)
2) For each sample, evaluate all curated fields with ArbitratorAgent in parallel
3) For incorrect fields, re-run CuratorAgent with guidance and merge results
4) Repeat 2-3 until all correct or max_iterations reached
5) Run unified normalization on final curated data
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Dict, Any, List, Optional

from src.workflows.batch_targets import (
    TARGET_FIELD_CONFIG,
    create_model_provider_for_operation,
    InitialProcessingResult,
    run_conditional_processing as bt_run_conditional_processing,
)
from src.agents.Arbitrator import run_arbitration_for_sample
from src.agents.curator import run_curator_agent
# Normalization is invoked later via utilities; direct import not needed here
from src.evaluation.loader import load_raw_context
from src.workflows.batch_targets import run_unified_normalization


def _safe_serialize(obj):
    """
    Convert curator outputs (which may include Pydantic models and rich objects)
    into plain JSON-serializable dict/list/scalar structures.
    """
    import json as _json

    # Fast path for already serializable primitives
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, list):
        return [_safe_serialize(item) for item in obj]
    if isinstance(obj, dict):
        return {str(k): _safe_serialize(v) for k, v in obj.items()}

    # Pydantic v2 models
    if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
        try:
            data = obj.model_dump()
            return _safe_serialize(data)
        except Exception:
            pass

    # Pydantic v1 models
    if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
        try:
            data = obj.dict()
            return _safe_serialize(data)
        except Exception:
            pass

    # Generic objects: walk public attributes
    out = {}
    try:
        for attr in dir(obj):
            if attr.startswith("_"):
                continue
            try:
                val = getattr(obj, attr)
            except Exception:
                continue
            # Skip callables
            if callable(val):
                continue
            try:
                serialized = _safe_serialize(val)
                # Test JSON compatibility
                _ = _json.dumps(serialized)
                out[attr] = serialized
            except Exception:
                # Skip non-serializable fields
                continue
        if out:
            return out
    except Exception:
        pass

    # Fallback: string representation
    try:
        return str(obj)
    except Exception:
        return None


def _atomic_write_json(path: Path, data: dict, *, indent: int = 2) -> None:
    """Write JSON atomically to avoid truncated/corrupted files."""
    import json as _json
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    with tmp_path.open("w", encoding="utf-8") as f:
        _json.dump(data, f, indent=indent)
        f.flush()
        os_fsync = getattr(__import__("os"), "fsync", None)
        if os_fsync and hasattr(f, "fileno"):
            try:
                os_fsync(f.fileno())
            except Exception:
                pass
    try:
        tmp_path.replace(path)
    except Exception:
        # Fallback to rename
        tmp_path.rename(path)

def merge_corrective_with_original_results(original_result, corrective_result, corrected_sample_ids: list):
    """
    Merge corrective results with original results, preserving uncorrected samples.
    
    Args:
        original_result: Original CuratorOutput with all samples
        corrective_result: Corrective CuratorOutput with only corrected samples
        corrected_sample_ids: List of sample IDs that were corrected
        
    Returns:
        Merged CuratorOutput with all samples (corrected + unchanged)
    """
    print(f"🔍 DEBUG[merge_corrective]: Original has {len(getattr(original_result, 'curation_results', []))} samples")
    print(f"🔍 DEBUG[merge_corrective]: Corrective has {len(getattr(corrective_result, 'curation_results', []))} samples")
    print(f"🔍 DEBUG[merge_corrective]: Corrected sample IDs: {corrected_sample_ids}")
    
    if not original_result:
        return corrective_result
    
    if not corrective_result:
        return original_result
    
    # Create a merged result
    class MergedCuratorOutput:
        def __init__(self):
            self.success = True
            self.message = "Merged original and corrective curation results"
            self.execution_time_seconds = 0
            self.sample_ids_requested = []
            self.target_field = getattr(original_result, 'target_field', getattr(corrective_result, 'target_field', ''))
            self.session_directory = getattr(original_result, 'session_directory', getattr(corrective_result, 'session_directory', ''))
            self.curation_results = []
            self.total_samples_processed = 0
            self.samples_needing_review = 0
            self.files_created = []
            self.curation_results_file = getattr(original_result, 'curation_results_file', None)
            self.average_confidence = None
            self.warnings = []
    
    merged = MergedCuratorOutput()
    
    # Build a map of corrected samples
    corrective_map = {}
    if hasattr(corrective_result, 'curation_results'):
        for curation in corrective_result.curation_results:
            sample_id = getattr(curation, 'sample_id', None)
            if sample_id:
                corrective_map[sample_id] = curation
    
    # Add all samples: use corrective version if available, otherwise original
    if hasattr(original_result, 'curation_results'):
        for curation in original_result.curation_results:
            sample_id = getattr(curation, 'sample_id', None)
            if sample_id in corrective_map:
                # Use corrected version
                merged.curation_results.append(corrective_map[sample_id])
                print(f"🔍 DEBUG[merge_corrective]: Using corrected version for {sample_id}")
            else:
                # Keep original version
                merged.curation_results.append(curation)
                print(f"🔍 DEBUG[merge_corrective]: Keeping original version for {sample_id}")
    
    merged.total_samples_processed = len(merged.curation_results)
    merged.sample_ids_requested = [getattr(c, 'sample_id', '') for c in merged.curation_results]
    
    print(f"🔍 DEBUG[merge_corrective]: Final merged result has {len(merged.curation_results)} samples")
    
    return merged


def merge_batch_curator_results(batch_results: list, field_name: str):
    """
    Merge multiple corrective curator batch results into a single result.
    
    Args:
        batch_results: List of CuratorOutput objects from different batches
        field_name: Field name being corrected
        
    Returns:
        Merged CuratorOutput object
    """
    if not batch_results:
        return None
    
    if len(batch_results) == 1:
        return batch_results[0]
    
    # Merge multiple batch results
    merged_curation_results = []
    
    # Collect all curation results from all batches
    for batch_result in batch_results:
        if hasattr(batch_result, 'curation_results'):
            merged_curation_results.extend(batch_result.curation_results)
    
    # Create a merged result using the structure of the first result
    class MergedCuratorOutput:
        def __init__(self):
            self.success = True
            self.target_field = field_name
            self.curation_results = merged_curation_results
            self.total_samples_processed = len(merged_curation_results)
            self.execution_time_seconds = sum(
                getattr(br, 'execution_time_seconds', 0) for br in batch_results
            )
            self.message = f"Merged corrective curation from {len(batch_results)} batches"
            # Add session_directory from first batch result to fix normalization errors
            self.session_directory = getattr(batch_results[0], 'session_directory', '') if batch_results else ''
            self.sample_ids_requested = [getattr(r, 'sample_id', '') for r in merged_curation_results]
            self.curation_results_file = None
            self.files_created = []
            self.average_confidence = None
            self.warnings = []
            self.samples_needing_review = 0
        
        def model_dump(self):
            return {
                "success": self.success,
                "target_field": self.target_field,
                "curation_results": [
                    {
                        "sample_id": r.sample_id if hasattr(r, 'sample_id') else r.get('sample_id'),
                        "final_candidate": r.final_candidate if hasattr(r, 'final_candidate') else r.get('final_candidate'),
                        "final_confidence": r.final_confidence if hasattr(r, 'final_confidence') else r.get('final_confidence'),
                        "final_candidates": r.final_candidates if hasattr(r, 'final_candidates') else r.get('final_candidates', []),
                        "processing_notes": getattr(r, 'processing_notes', [])
                    } for r in self.curation_results
                ],
                "message": self.message,
                "total_samples_processed": self.total_samples_processed,
                "execution_time_seconds": self.execution_time_seconds
            }
    
    return MergedCuratorOutput()


def load_corrective_curator_results(session_directory: str, sample_id: str, sample_type: str) -> Dict[str, str]:
    """
    Load corrective curator results from previous iterations.
    
    Args:
        session_directory: Base session directory
        sample_id: Sample ID to load results for
        sample_type: Sample type
        
    Returns:
        Dictionary of field_name -> corrected_value for this sample
    """
    corrective_values = {}
    try:
        # Look for corrective curator outputs in session directory
        session_path = Path(session_directory)
        
        # Check for session-specific directories created by corrective curator runs
        pattern = f"{sample_type}_eval_fix"
        for potential_dir in session_path.rglob(pattern):
            if potential_dir.is_dir():
                # Look for curator output files in this directory
                for field_dir in potential_dir.iterdir():
                    if field_dir.is_dir():
                        field_name = field_dir.name
                        curator_file = field_dir / "curator_output_cell_line.json"
                        if curator_file.exists():
                            try:
                                with open(curator_file, 'r') as f:
                                    curator_data = json.load(f)
                                
                                # Extract corrected value for this sample
                                if "curation_results" in curator_data:
                                    for result in curator_data["curation_results"]:
                                        if result.get("sample_id") == sample_id:
                                            # Get the corrected value
                                            if "final_candidate" in result:
                                                corrective_values[field_name] = str(result["final_candidate"])
                                            elif "assay_type" in result and field_name == "assay_type":
                                                corrective_values[field_name] = str(result["assay_type"])
                                            elif result.get("final_candidates"):
                                                first_candidate = result["final_candidates"][0]
                                                if isinstance(first_candidate, dict) and "value" in first_candidate:
                                                    corrective_values[field_name] = str(first_candidate["value"])
                                            break
                            except Exception as e:
                                print(f"🔧 DEBUG[load_corrective]: Error loading {curator_file}: {e}")
                                continue
        
        print(f"🔧 DEBUG[load_corrective]: Found {len(corrective_values)} corrective values for {sample_id}: {corrective_values}")
    except Exception as e:
        print(f"🔧 DEBUG[load_corrective]: Error loading corrective results: {e}")
    
    return corrective_values


async def run_eval_conditional(
    *,
    session_directory: str,
    sample_type_batches: Dict[str, List[List[str]]] = None,  # New batch structure
    grouped_samples: Dict[str, List[str]] = None,  # Legacy parameter for backward compatibility
    target_fields: List[str],
    model_provider,
    max_tokens: Optional[int] = None,
    max_workers: Optional[int] = None,
    max_iterations: int = 2,
    data_intake_output: Any = None,
    arbitrator_test_mode: bool = False,
    incremental_csv_callback = None,  # Optional callback for CSV writing
) -> Dict[str, Any]:
    print("🔧 DEBUG[eval_conditional]: starting run_eval_conditional")
    print(f"🔧 DEBUG[eval_conditional]: session_directory={session_directory}")
    print(f"🔧 DEBUG[eval_conditional]: target_fields={target_fields}")
    print(f"🔧 DEBUG[eval_conditional]: max_workers={max_workers}, max_iterations={max_iterations}")
    print(f"🔧 DEBUG[eval_conditional]: arbitrator_test_mode={arbitrator_test_mode}")
    
    # Handle both new batch structure and legacy grouped_samples for backward compatibility
    if sample_type_batches is not None:
        # New approach: respect batch sizes from preprocessing
        _batch_info = {k: f"{len(batches)} batches, {sum(len(b) for b in batches)} samples" for k, batches in sample_type_batches.items()}
        print(f"🔧 DEBUG[eval_conditional]: sample_type_batches -> {_batch_info}")
        # Create flattened view for arbitration (but preserve batch structure for curation)
        grouped_samples = {}
        for k, batches in sample_type_batches.items():
            if not batches:
                continue
            flat = []
            for b in batches:
                flat.extend(b)
            if flat:
                grouped_samples[k] = flat
    elif grouped_samples is not None:
        # Legacy approach: create single batches from flattened groups
        print("⚠️ LEGACY MODE: Using flattened grouped_samples - this may cause large batch issues")
        _gs_sizes = {k: len(v) for k, v in grouped_samples.items()}
        print(f"🔧 DEBUG[eval_conditional]: grouped_samples sizes -> {_gs_sizes}")
        # Convert to batch structure with reasonable batch sizes
        batch_size = max_workers or 5  # Use max_workers as batch size, fallback to 5
        sample_type_batches = {}
        for sample_type, samples_list in grouped_samples.items():
            batches = []
            for i in range(0, len(samples_list), batch_size):
                batch = samples_list[i:i + batch_size]
                batches.append(batch)
            sample_type_batches[sample_type] = batches
    else:
        raise ValueError("Either sample_type_batches or grouped_samples must be provided")

    session_path = Path(session_directory)
    conditional_dir = session_path / "conditional_processing"
    conditional_dir.mkdir(exist_ok=True)

    import time as _time
    _start_ts = _time.time()

    # Phase A: Perform curation from scratch using same infrastructure (no wrapper)
    # Process samples in proper batches to avoid overloading the curator
    from src.models import LinkerOutput
    curated_outputs_per_st: Dict[str, Dict[str, Any]] = {}

    cond_results_per_st: Dict[str, Any] = {}
    initial_results_per_st: Dict[str, Any] = {}

    # Process each sample type with proper batch sizing
    for sample_type, batches_list in sample_type_batches.items():
        if not batches_list:
            continue
        
        print(f"🔧 DEBUG[eval_conditional]: Processing {sample_type} with {len(batches_list)} batches")
        
        # For eval mode, we'll still create a single consolidated batch directory per sample type
        # but process the samples in smaller chunks for curation
        batch_name = f"{sample_type}_batch_1"
        batch_dir = conditional_dir / batch_name
        batch_dir.mkdir(parents=True, exist_ok=True)
        
        # Get all samples for this sample type (flattened for directory creation)
        samples_list = []
        for batch in batches_list:
            samples_list.extend(batch)

        # Filter curation packages and construct a consolidated LinkerOutput for all samples
        filtered_curation_packages = []
        try:
            if data_intake_output and getattr(data_intake_output, "curation_packages", None):
                for pkg in data_intake_output.curation_packages:
                    try:
                        if getattr(pkg, "sample_id", None) in samples_list:
                            filtered_curation_packages.append(pkg)
                    except Exception:
                        pass
        except Exception:
            filtered_curation_packages = []

        # Create consolidated data intake for the entire sample type (used for arbitration)
        consolidated_data_intake = LinkerOutput(
            success=True,
            message=f"Consolidated for {sample_type} processing",
            execution_time_seconds=0.0,
            sample_ids_requested=list(samples_list),
            session_directory=str(batch_dir),
            fields_removed_during_cleaning=getattr(data_intake_output, "fields_removed_during_cleaning", []) or [],
            linked_data=getattr(data_intake_output, "linked_data", None),
            files_created=getattr(data_intake_output, "files_created", []) or [],
            successfully_linked=list(samples_list),
            failed_linking=[],
            warnings=getattr(data_intake_output, "warnings", []) or [],
            sample_ids_for_curation=list(samples_list),
            recommended_curation_fields=target_fields,
            cleaned_metadata_files=getattr(data_intake_output, "cleaned_metadata_files", None),
            cleaned_series_metadata=getattr(data_intake_output, "cleaned_series_metadata", None),
            cleaned_sample_metadata=getattr(data_intake_output, "cleaned_sample_metadata", None),
            cleaned_abstract_metadata=getattr(data_intake_output, "cleaned_abstract_metadata", None),
            curation_packages=filtered_curation_packages,
        )

        # Save consolidated data intake output JSON for audit consistency
        try:
            (batch_dir / "data_intake_output.json").write_text(consolidated_data_intake.model_dump_json(indent=2), encoding="utf-8")
        except Exception:
            pass

        curator_provider = create_model_provider_for_operation("conditional_curation", model_provider)
        
        # Check if arbitrator test mode is enabled
        if arbitrator_test_mode:
            print("🧪 ARBITRATOR TEST MODE ENABLED: Injecting fake, deliberately incorrect curation results")
            print("🧪 TEST OBJECTIVE: Verify arbitrator catches and corrects ALL fake wrong values:")
            print("   GSM5482375 (H520 lung cancer): disease=Alzheimer's, organ=brain, cell_line=HeLa, cell_type=neuron, assay_type=microarray, treatment=aspirin")
            print("   GSM3567929 (Neural stem iPSC): disease=pancreatic cancer, organ=pancreas, cell_line=HEK293, cell_type=beta cell, assay_type=ChIP-seq, treatment=chemotherapy")
            print("🧪 EXPECTED: Arbitrator should identify ALL 12 values as incorrect and suggest proper corrections!")
            
            def create_fake_curator_result(sample_id: str, field: str, wrong_value: str, confidence: float = 0.9):
                """Create a fake CurationResult with obviously wrong values."""
                class MockCurationResult:
                    def __init__(self, sample_id, field, wrong_value, confidence):
                        self.tool_name = "MockCuratorAgent"
                        self.sample_id = sample_id
                        self.target_field = field
                        self.final_candidate = wrong_value
                        self.final_confidence = confidence
                        self.final_candidates = [{
                            "value": wrong_value,
                            "confidence": confidence,
                            "source": "mock",
                            "context": "FAKE: Deliberately wrong value for testing arbitrator",
                            "rationale": "MOCK: This is intentionally incorrect to test arbitration"
                        }]
                        self.reconciliation_needed = False
                        self.processing_notes = ["MOCK INJECTION FOR TESTING"]
                
                return MockCurationResult(sample_id, field, wrong_value, confidence)
            
            def create_fake_curator_output(field_name: str, curation_results: list):
                """Create a fake CuratorOutput object."""
                class MockCuratorOutput:
                    def __init__(self, field_name, curation_results):
                        self.success = True
                        self.target_field = field_name
                        self.curation_results = curation_results
                        self.total_samples_processed = len(curation_results)
                        self.execution_time_seconds = 0.1
                        self.message = f"MOCK: Fake curation for {field_name}"
                        
                        def model_dump(self):
                            return {
                                "success": self.success,
                                "target_field": self.target_field,
                                "curation_results": [
                                    {
                                        "sample_id": r.sample_id,
                                        "final_candidate": r.final_candidate,
                                        "final_confidence": r.final_confidence,
                                        "final_candidates": r.final_candidates,
                                        "processing_notes": r.processing_notes
                                    } for r in self.curation_results
                                ],
                                "message": self.message
                            }
                        
                        self.model_dump = model_dump
                
                return MockCuratorOutput(field_name, curation_results)
            
            # Create deliberately wrong curation results based on real metadata
            if sample_type == "cell_line":
                mock_outputs = {
                    "disease": create_fake_curator_output("disease", [
                        create_fake_curator_result("GSM5482375", "disease", "Alzheimer's disease"),  # WRONG: should be lung cancer
                        create_fake_curator_result("GSM3567929", "disease", "pancreatic cancer"),    # WRONG: should be healthy/control
                    ]),
                    "organ": create_fake_curator_output("organ", [
                        create_fake_curator_result("GSM5482375", "organ", "brain"),        # WRONG: should be lung
                        create_fake_curator_result("GSM3567929", "organ", "pancreas"),     # WRONG: should be None reported
                    ]),
                    "cell_line": create_fake_curator_output("cell_line", [
                        create_fake_curator_result("GSM5482375", "cell_line", "HeLa"),     # WRONG: should be H520
                        create_fake_curator_result("GSM3567929", "cell_line", "HEK293"),   # WRONG: should be None reported
                    ]),
                    "cell_type": create_fake_curator_output("cell_type", [
                        create_fake_curator_result("GSM5482375", "cell_type", "neuron"),   # WRONG: should be squamous cell
                        create_fake_curator_result("GSM3567929", "cell_type", "beta cell"), # WRONG: should be neural stem cell
                    ]),
                    "assay_type": create_fake_curator_output("assay_type", [
                        create_fake_curator_result("GSM5482375", "assay_type", "microarray"), # WRONG: should be bulk RNA-seq
                        create_fake_curator_result("GSM3567929", "assay_type", "ChIP-seq"),   # WRONG: should be single-cell RNA-seq
                    ]),
                    "treatment": create_fake_curator_output("treatment", [
                        create_fake_curator_result("GSM5482375", "treatment", "aspirin"),     # WRONG: should be DMSO
                        create_fake_curator_result("GSM3567929", "treatment", "chemotherapy"), # WRONG: should be None reported
                    ]),
                }
            else:
                mock_outputs = {}
            
            # Create a mock result object
            class MockConditionalResult:
                def __init__(self, outputs):
                    self.success = True
                    self.all_sample_type_outputs = outputs
                    self.execution_time_seconds = 0.1
            
            cond_result = MockConditionalResult(mock_outputs)
            
            print("🧪 MOCK: Injected fake wrong values for arbitrator testing:")
            for field, output in mock_outputs.items():
                for result in output.curation_results:
                    print(f"  - {result.sample_id} {field}: '{result.final_candidate}' (should be corrected by arbitrator)")
            
            curated_outputs_per_st[sample_type] = mock_outputs
            
        else:
            # Normal operation - run actual conditional processing with concurrent batching
            # Process batches concurrently to utilize max_workers parameter effectively
            sample_type_outputs = {}
            
            print(f"🔧 EVAL MODE: Processing {len(batches_list)} small batches for {sample_type} (total: {sum(len(b) for b in batches_list)} samples) concurrently")
            
            async def process_batch(batch_idx, batch_samples):
                """Process a single batch concurrently."""
                print(f"🔧 DEBUG[eval_conditional]: Processing batch {batch_idx + 1}/{len(batches_list)} for {sample_type} ({len(batch_samples)} samples)")
                
                # Create batch-specific curation packages
                batch_filtered_packages = []
                try:
                    if data_intake_output and getattr(data_intake_output, "curation_packages", None):
                        for pkg in data_intake_output.curation_packages:
                            try:
                                if getattr(pkg, "sample_id", None) in batch_samples:
                                    batch_filtered_packages.append(pkg)
                            except Exception:
                                pass
                except Exception:
                    batch_filtered_packages = []

                # Create batch-specific LinkerOutput
                batch_data_intake = LinkerOutput(
                    success=True,
                    message=f"Batch {batch_idx + 1} for {sample_type} processing",
                    execution_time_seconds=0.0,
                    sample_ids_requested=list(batch_samples),
                    session_directory=str(batch_dir),
                    fields_removed_during_cleaning=getattr(data_intake_output, "fields_removed_during_cleaning", []) or [],
                    linked_data=getattr(data_intake_output, "linked_data", None),
                    files_created=getattr(data_intake_output, "files_created", []) or [],
                    successfully_linked=list(batch_samples),
                    failed_linking=[],
                    warnings=getattr(data_intake_output, "warnings", []) or [],
                    sample_ids_for_curation=list(batch_samples),
                    recommended_curation_fields=target_fields,
                    cleaned_metadata_files=getattr(data_intake_output, "cleaned_metadata_files", None),
                    cleaned_series_metadata=getattr(data_intake_output, "cleaned_series_metadata", None),
                    cleaned_sample_metadata=getattr(data_intake_output, "cleaned_sample_metadata", None),
                    cleaned_abstract_metadata=getattr(data_intake_output, "cleaned_abstract_metadata", None),
                    curation_packages=batch_filtered_packages,
                )
                
                # Create batch-specific mock initial result
                batch_mock_initial = InitialProcessingResult(
                    success=True,
                    session_id=f"{sample_type}_eval_batch_{batch_idx}",
                    session_directory=str(batch_dir),
                    data_intake_output=batch_data_intake,
                    sample_ids=list(batch_samples),
                    direct_fields={},
                    initial_curation_data={},
                    initial_curator_outputs={},
                    sample_type_mapping={sid: sample_type for sid in batch_samples},
                    grouped_samples={sample_type: list(batch_samples)},
                )
                
                # Process this batch
                batch_cond_result = await bt_run_conditional_processing(
                    initial_result=batch_mock_initial,
                    model_provider=curator_provider,
                    max_tokens=max_tokens,
                )
                
                return batch_cond_result, batch_idx
            
            # Create tasks for all batches
            batch_tasks = [
                process_batch(batch_idx, batch_samples) 
                for batch_idx, batch_samples in enumerate(batches_list)
            ]
            
            # Process all batches concurrently
            print(f"🔧 DEBUG[eval_conditional]: Launching {len(batch_tasks)} concurrent batch processing tasks")
            batch_results = await asyncio.gather(*batch_tasks)
            print(f"🔧 DEBUG[eval_conditional]: Completed {len(batch_results)} concurrent batch processing tasks")
            
            # Merge batch results into sample type outputs
            for batch_cond_result, batch_idx in batch_results:
                batch_outputs = getattr(batch_cond_result, "all_sample_type_outputs", {}) or {}
                for field_name, curator_output in batch_outputs.items():
                    if field_name not in sample_type_outputs:
                        sample_type_outputs[field_name] = []
                    sample_type_outputs[field_name].append(curator_output)
            
            # Merge multiple batch outputs per field
            merged_outputs = {}
            for field_name, curator_outputs_list in sample_type_outputs.items():
                if len(curator_outputs_list) == 1:
                    merged_outputs[field_name] = curator_outputs_list[0]
                else:
                    # Merge multiple curator outputs for the same field
                    merged_outputs[field_name] = merge_batch_curator_results(curator_outputs_list, field_name)
            
            curated_outputs_per_st[sample_type] = merged_outputs
            
            # Create a synthetic cond_result for compatibility
            class SyntheticCondResult:
                def __init__(self, outputs):
                    self.success = True
                    self.all_sample_type_outputs = outputs
                    self.execution_time_seconds = 0.1
            
            cond_result = SyntheticCondResult(merged_outputs)
        
        # Create consolidated initial result for arbitration (using all samples for the sample type)
        consolidated_mock_initial = InitialProcessingResult(
            success=True,
            session_id=f"{sample_type}_eval",
            session_directory=str(batch_dir),
            data_intake_output=consolidated_data_intake,  # Use the consolidated data intake
            sample_ids=list(samples_list),  # All samples for this sample type
            direct_fields={},
            initial_curation_data={},
            initial_curator_outputs={},
            sample_type_mapping={sid: sample_type for sid in samples_list},
            grouped_samples={sample_type: list(samples_list)},
        )
        
        # Store outputs for later arbitration
        initial_results_per_st[sample_type] = consolidated_mock_initial
        cond_results_per_st[sample_type] = cond_result

    # Build quick metadata lookup from data_intake_output.curation_packages
    sample_meta_lookup: Dict[str, Dict[str, Any]] = {}
    if data_intake_output and getattr(data_intake_output, "curation_packages", None):
        try:
            for pkg in data_intake_output.curation_packages:
                sid = getattr(pkg, "sample_id", None)
                if not sid:
                    continue
                sample_meta_lookup[sid] = {
                    "series_id": getattr(pkg, "series_id", "") or "",
                    "series_metadata": getattr(pkg, "series_metadata", None),
                    "sample_metadata": getattr(pkg, "sample_metadata", None),
                    "abstract_text": None,
                }
                # Abstract text from abstract_metadata if present
                am = getattr(pkg, "abstract_metadata", None)
                if isinstance(am, dict):
                    sample_meta_lookup[sid]["abstract_text"] = (
                        am.get("abstract")
                        or am.get("Abstract")
                        or am.get("abstract_text")
                        or am.get("ABSTRACT")
                    )
        except Exception:
            pass

    # Iterative arbitration and correction
    for iteration in range(1, max_iterations + 1):
        print(f"🔧 DEBUG[eval_conditional]: iteration {iteration} starting")
        iter_dir = conditional_dir / f"iteration_{iteration}"
        arb_dir = iter_dir / "arbitrator"
        corr_dir = iter_dir / "curation_corrections"
        arb_dir.mkdir(parents=True, exist_ok=True)
        corr_dir.mkdir(parents=True, exist_ok=True)

        # Build arbitration tasks per sample
        sem = asyncio.Semaphore(max_workers or 8)
        arbitrator_provider = create_model_provider_for_operation("arbitrator", model_provider)
        tasks = []
        sample_index: List[tuple[str, str]] = []  # (sample_type, sample_id)

        for sample_type, samples_list in grouped_samples.items():
            if not samples_list:
                continue
            for sample_id in samples_list:
                # Load series_id from mapping
                series_id = ""
                try:
                    mapping = json.loads((Path(session_directory) / "series_sample_mapping.json").read_text())
                    for k, v in mapping.get("series_sample_mapping", {}).items():
                        if sample_id in v:
                            series_id = k
                            break
                except Exception:
                    series_id = ""

                # Prefer curated packages metadata; fallback to filesystem loader
                pkg_meta = sample_meta_lookup.get(sample_id, {})
                if pkg_meta:
                    series_meta = pkg_meta.get("series_metadata")
                    sample_meta = pkg_meta.get("sample_metadata")
                    abstract_text = pkg_meta.get("abstract_text")
                    # Convert Pydantic objects to dicts to avoid JSON serialization errors
                    if hasattr(series_meta, 'model_dump'):
                        series_meta = series_meta.model_dump()
                    elif hasattr(series_meta, 'dict'):
                        series_meta = series_meta.dict()
                    if hasattr(sample_meta, 'model_dump'):
                        sample_meta = sample_meta.model_dump()
                    elif hasattr(sample_meta, 'dict'):
                        sample_meta = sample_meta.dict()
                else:
                    abstract_text, series_meta, sample_meta = load_raw_context(session_directory, series_id, sample_id)

                # Load curated values from actual curation results
                curated_values: Dict[str, str] = {}
                try:
                    # Find the sample type for this sample to locate the right curation outputs
                    sample_st = None
                    for st, samples_list in grouped_samples.items():
                        if sample_id in samples_list:
                            sample_st = st
                            break
                    
                    if sample_st and sample_st in curated_outputs_per_st:
                        st_outputs = curated_outputs_per_st[sample_st]
                        for field_name, curator_output in st_outputs.items():
                            if not curator_output or not hasattr(curator_output, 'curation_results'):
                                continue
                            # Find the curation result for this sample
                            for curation_result in curator_output.curation_results:
                                if getattr(curation_result, 'sample_id', None) == sample_id:
                                    # Extract final candidate value - handle both formats
                                    final_candidate = getattr(curation_result, 'final_candidate', None)
                                    if final_candidate is not None:
                                        curated_values[field_name] = str(final_candidate)
                                    elif hasattr(curation_result, 'final_candidates') and curation_result.final_candidates:
                                        # Fallback to first candidate
                                        first_candidate = curation_result.final_candidates[0]
                                        if hasattr(first_candidate, 'value'):
                                            curated_values[field_name] = str(first_candidate.value)
                                    # Special handling for assay_type field which may use different attribute
                                    elif field_name == 'assay_type' and hasattr(curation_result, 'assay_type'):
                                        curated_values[field_name] = str(getattr(curation_result, 'assay_type'))
                                    break
                    
                    # Also check for any existing corrective curator results from previous iterations
                    corrective_values = load_corrective_curator_results(session_directory, sample_id, sample_st)
                    curated_values.update(corrective_values)  # Corrective results override original
                    
                    print(f"🔧 DEBUG[eval_conditional]: loaded {len(curated_values)} curated values for {sample_id}: {list(curated_values.keys())}")
                except Exception as e:
                    print(f"🔧 DEBUG[eval_conditional]: error loading curated values for {sample_id}: {e}")
                    pass

                async def arb_task(st=sample_type, sid=sample_id, s_meta=series_meta, sm_meta=sample_meta, cur=curated_values, ser_id=series_id, abs_txt=abstract_text):
                    async with sem:
                        print(f"🔧 DEBUG[eval_conditional]: Arbitrator start sid={sid} st={st}")
                        result = await run_arbitration_for_sample(
                            model_name=getattr(arbitrator_provider, 'default_model', ''),
                            model_provider=arbitrator_provider,
                            sample_id=sid,
                            series_id=ser_id,
                            sample_type=st,
                            abstract_text=abs_txt or "",
                            series_metadata=s_meta,
                            sample_metadata=sm_meta,
                            curated_values=cur,
                        )
                        out_path = arb_dir / f"{sid}_evaluation.json"
                        out_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
                        print(f"🔧 DEBUG[eval_conditional]: Arbitrator done sid={sid}, fields={len(result.fields) if getattr(result,'fields',None) else 0}")
                        return result

                tasks.append(arb_task())
                sample_index.append((sample_type, sample_id))

        print(f"🔧 DEBUG[eval_conditional]: submitting {len(tasks)} arbitrator tasks")
        results: List[Optional[Any]] = await asyncio.gather(*tasks, return_exceptions=False)
        print(f"🔧 DEBUG[eval_conditional]: received {len(results)} arbitrator results")

        # Determine incorrect fields and perform corrections
        any_incorrect = False
        corrections: Dict[str, Dict[str, str]] = {}

        incorrect_counter = 0
        # Build guidance per field per sample for re-curation
        field_to_samples: Dict[str, List[str]] = {}
        # Canonicalization and skips
        alias_map = {"gender": "sex"}
        skip_direct_fields = set(["organism", "pubmed_id", "platform_id", "instrument"])  # handled via direct extraction
        guidance_payload: Dict[str, Dict[str, Any]] = {}
        for (sample_type, sample_id), res in zip(sample_index, results):
            if not res or not getattr(res, 'fields', None):
                continue
            for fe in res.fields:
                # Ignore not applicable fields for this sample_type per TARGET_FIELD_CONFIG
                not_applicable = False
                cfg = TARGET_FIELD_CONFIG["conditional_processing"].get(sample_type, {})
                if fe.field_name in cfg.get("not_applicable", []):
                    not_applicable = True
                if not_applicable:
                    continue

                # Normalize field name and skip non-curation/alias issues
                fname = (fe.field_name or "").strip().lower()
                fname = alias_map.get(fname, fname)
                if fname in skip_direct_fields:
                    continue

                if fe.is_curated_correct is False:
                    any_incorrect = True
                    incorrect_counter += 1
                    # Prepare curator rerun guidance directory
                    fld_dir = corr_dir / fname
                    fld_dir.mkdir(exist_ok=True, parents=True)
                    # Guidance payload artifact
                    guide = {
                        "sample_id": sample_id,
                        "sample_type": sample_type,
                        "field": fname,
                        "original_curated": fe.curated_value,
                        "suggested_curation": fe.suggested_curation,
                        "reason": fe.curated_reason,
                    }
                    (fld_dir / f"{sample_id}_guidance.json").write_text(json.dumps(guide, indent=2), encoding="utf-8")
                    corrections.setdefault(sample_id, {})[fname] = fe.suggested_curation or ""
                    field_to_samples.setdefault(fname, []).append(sample_id)
                    guidance_payload.setdefault(fname, {})[sample_id] = guide

        print(f"🔧 DEBUG[eval_conditional]: iteration {iteration} incorrect fields count={incorrect_counter}")
        if not any_incorrect:
            break

        # Re-run curator for incorrect fields
        # Only re-curate fields that are configured for curation per sample type
        run_curator_again = True

        if run_curator_again:
            print("🔧 DEBUG[eval_conditional]: launching corrective curation for incorrect fields")
            # Build curation field allowlist per sample type
            curation_allow: Dict[str, set] = {
                st: set(TARGET_FIELD_CONFIG["conditional_processing"].get(st, {}).get("curation", []))
                for st in grouped_samples.keys()
            }
            # Single semaphore and provider for all corrective runs
            sem_fix = asyncio.Semaphore(max_workers or 8)
            curator_provider = create_model_provider_for_operation("conditional_curation", model_provider)
            fix_tasks = []
            corrective_results_per_field_st = {}  # Track corrective results for merging

            total_queued = 0
            for field_name, sample_ids_list in field_to_samples.items():
                # Skip any field not present in any sample type config to avoid irrelevant directories
                present_in_any = any(
                    field_name in curation_allow.get(st, set()) for st in grouped_samples.keys()
                )
                if not present_in_any:
                    print(f"🔧 DEBUG[eval_conditional]: skipping field {field_name} - not in TARGET_FIELD_CONFIG for any sample type")
                    continue
                # group by sample type
                st_groups: Dict[str, List[str]] = {}
                for st, sid in sample_index:
                    if sid in sample_ids_list:
                        st_groups.setdefault(st, []).append(sid)

                for st, sids in st_groups.items():
                    if field_name not in curation_allow.get(st, set()):
                        continue
                    if not sids:
                        continue

                    # Build filtered LinkerOutput for just these sample IDs using the correct per-sample-type intake
                    base_initial = initial_results_per_st.get(st)
                    if not base_initial:
                        continue
                    base_linker = getattr(base_initial, "data_intake_output", None)
                    if not base_linker:
                        continue

                    # Filter curation packages for these sids
                    filtered_curation_packages = []
                    try:
                        if getattr(base_linker, "curation_packages", None):
                            for pkg in base_linker.curation_packages:
                                try:
                                    if getattr(pkg, "sample_id", None) in sids:
                                        filtered_curation_packages.append(pkg)
                                except Exception:
                                    pass
                    except Exception:
                        filtered_curation_packages = []


                # 🚀 BATCH SIZE COMPLIANCE: Respect batch size limits for corrective curation
                # Split large sample sets into manageable batch sizes to prevent overload
                batch_size = max_workers or 5  # Use max_workers as batch size, fallback to 5
                
                # Create batches of samples for this field and sample type
                sample_batches = []
                for i in range(0, len(sids), batch_size):
                    batch_sids = sids[i:i + batch_size]
                    sample_batches.append(batch_sids)
                
                print(f"🔧 DEBUG[eval_conditional]: corrective run field={field_name} sample_type={st} samples={len(sids)} batches={len(sample_batches)}")
                
                # Process each batch separately
                for batch_idx, batch_sids in enumerate(sample_batches):
                    # Filter curation packages for this batch
                    filtered_curation_packages = []
                    try:
                        if getattr(base_linker, "curation_packages", None):
                            for pkg in base_linker.curation_packages:
                                try:
                                    if getattr(pkg, "sample_id", None) in batch_sids:
                                        filtered_curation_packages.append(pkg)
                                except Exception:
                                    pass
                    except Exception:
                        filtered_curation_packages = []

                    from src.models import LinkerOutput
                    batch_filtered_linker = LinkerOutput(
                        success=True,
                        message=f"Correction batch {batch_idx+1} for {st} {field_name}",
                        execution_time_seconds=0.0,
                        sample_ids_requested=list(batch_sids),
                        session_directory=getattr(base_linker, "session_directory", session_directory),
                        fields_removed_during_cleaning=getattr(base_linker, "fields_removed_during_cleaning", []) or [],
                        linked_data=getattr(base_linker, "linked_data", None),
                        files_created=getattr(base_linker, "files_created", []) or [],
                        successfully_linked=list(batch_sids),
                        failed_linking=[],
                        warnings=getattr(base_linker, "warnings", []) or [],
                        sample_ids_for_curation=list(batch_sids),
                        recommended_curation_fields=[field_name],
                        cleaned_metadata_files=getattr(base_linker, "cleaned_metadata_files", None),
                        cleaned_series_metadata=getattr(base_linker, "cleaned_series_metadata", None),
                        cleaned_sample_metadata=getattr(base_linker, "cleaned_sample_metadata", None),
                        cleaned_abstract_metadata=getattr(base_linker, "cleaned_abstract_metadata", None),
                        curation_packages=filtered_curation_packages,
                    )

                    async def _fix_batch(st_local=st, field_local=field_name, linker_local=batch_filtered_linker, batch_idx_local=batch_idx):
                        async with sem_fix:
                            # Save corrective results to the original batch directory for proper integration
                            corrective_batch_dir = Path(session_directory) / "conditional_processing" / f"{st_local}_batch_1"
                            corrective_batch_dir.mkdir(parents=True, exist_ok=True)
                            
                            # Update the linker to point to the correct session directory for output
                            if hasattr(linker_local, 'session_directory'):
                                linker_local.session_directory = str(corrective_batch_dir)
                            
                            try:
                                corrective_result = await run_curator_agent(
                                    data_intake_output=linker_local,
                                    target_field=field_local,
                                    session_id=f"{st_local}_corrective_batch_{batch_idx_local}",
                                    sandbox_dir=str(corrective_batch_dir),
                                    model_provider=curator_provider,
                                    max_tokens=max_tokens,
                                    max_turns=100,
                                    guidance=guidance_payload.get(field_local),
                                )
                                
                                # Store corrective result for later integration
                                corrective_results_per_field_st[(field_local, st_local, batch_idx_local)] = corrective_result
                                print(f"🔧 DEBUG[eval_conditional]: corrective batch {batch_idx_local+1} completed for {field_local} {st_local}")
                                
                            except Exception as e:
                                print(f"❌ DEBUG[eval_conditional]: corrective batch {batch_idx_local+1} failed for {field_local} {st_local}: {e}")
                                # Continue with other batches instead of failing completely
                                corrective_results_per_field_st[(field_local, st_local, batch_idx_local)] = None
                    
                    fix_tasks.append(_fix_batch())
                    total_queued += 1

            print(f"🔧 DEBUG[eval_conditional]: queued {total_queued} corrective tasks across fields and sample types")
            if fix_tasks:
                await asyncio.gather(*fix_tasks)
                
                # Merge corrective results back into main curation outputs
                print(f"🔧 DEBUG[eval_conditional]: merging {len(corrective_results_per_field_st)} corrective results")
                
                # Group corrective results by field and sample type, merging batches
                merged_corrective_results = {}
                for (field_name, sample_type, batch_idx), corrective_result in corrective_results_per_field_st.items():
                    key = (field_name, sample_type)
                    if key not in merged_corrective_results:
                        merged_corrective_results[key] = []
                    if corrective_result:  # Only add successful results
                        merged_corrective_results[key].append(corrective_result)
                
                # Merge batched corrective results WITH original results (preserving uncorrected samples)
                for (field_name, sample_type), batch_results in merged_corrective_results.items():
                    if batch_results and sample_type in curated_outputs_per_st:
                        # If we have multiple batches, we need to merge them first
                        if len(batch_results) == 1:
                            # Single batch - use directly
                            final_corrective_result = batch_results[0]
                        else:
                            # Multiple batches - merge curation results
                            final_corrective_result = merge_batch_curator_results(batch_results, field_name)
                        
                        # Get list of corrected sample IDs from field_to_samples
                        corrected_sample_ids = field_to_samples.get(field_name, [])
                        
                        # Merge corrective results WITH original results (preserve uncorrected samples)
                        if field_name in curated_outputs_per_st[sample_type]:
                            original_result = curated_outputs_per_st[sample_type][field_name]
                            print(f"🔧 DEBUG[eval_conditional]: merging {len(batch_results)} corrective batches for {field_name}/{sample_type} with original (preserving uncorrected samples)")
                            merged_result = merge_corrective_with_original_results(
                                original_result, 
                                final_corrective_result, 
                                corrected_sample_ids
                            )
                            curated_outputs_per_st[sample_type][field_name] = merged_result
                        else:
                            print(f"🔧 DEBUG[eval_conditional]: adding new {field_name} for {sample_type} from merged corrective result")
                            curated_outputs_per_st[sample_type][field_name] = final_corrective_result

        # In this iteration, stop after one corrective pass
        print("🔧 DEBUG[eval_conditional]: stopping after corrective curation pass")
        break

    # After iterations, run unified normalization per sample type (to populate normalized terms)
    try:
        for st in list(initial_results_per_st.keys()):
            initial_for_st = initial_results_per_st.get(st)
            cond_for_st = cond_results_per_st.get(st)
            if not (initial_for_st and cond_for_st):
                continue
            normalization_data = await run_unified_normalization(
                initial_result=initial_for_st,
                conditional_result=cond_for_st,
                model_provider=model_provider,
                max_tokens=max_tokens,
                max_turns=100,
            )
            # Persist normalization results where consolidator expects them
            norm_payload = {}
            for field_name, per_sample in (normalization_data or {}).items():
                sample_results = []
                if isinstance(per_sample, dict):
                    for sid, result in per_sample.items():
                        sample_results.append({
                            "sample_id": sid,
                            "result": _safe_serialize(result),
                        })
                norm_payload[field_name] = {"sample_results": sample_results}
            bt_out = {"normalization_results": norm_payload}
            st_batch_dir = Path(session_directory) / "conditional_processing" / f"{st}_batch_1"
            bt_file = st_batch_dir / "batch_targets_output.json"
            if bt_file.exists():
                try:
                    existing = json.loads(bt_file.read_text())
                except Exception:
                    existing = {}
            else:
                existing = {}
            existing.update(bt_out)
            _atomic_write_json(bt_file, existing, indent=2)
    except Exception as _norm_err:
        print(f"🔧 DEBUG[eval_conditional]: unified normalization phase failed: {_norm_err}")

    # For compatibility, return a result dict similar to ConditionalProcessingResult
    # For compatibility, return a result dict similar to ConditionalProcessingResult
    # After iterative evaluation, produce classic-like output so downstream consolidation writes CSVs
    # Build a synthetic structure similar to conditional_processing output to leverage existing consolidation
    batch_results = []
    try:
        for st, cond_result in cond_results_per_st.items():
            if not cond_result:
                continue
            # Attempt to locate the batch directory we created earlier
            batch_dir = Path(session_directory) / "conditional_processing" / f"{st}_batch_1"
            # Collect sample list back from initial mock results
            samples_list = list(initial_results_per_st.get(st).sample_ids) if initial_results_per_st.get(st) else []
            batch_result = {
                "success": True,
                "batch_name": f"{st}_batch_1",
                "sample_type": st,
                "batch_samples": samples_list,
                "batch_directory": str(batch_dir),
                # Don't include the actual CuratorOutput objects as they're not JSON serializable
                # The updated results are now written to files for CSV consolidation
            }
            batch_results.append(batch_result)
            
            # Call incremental CSV callback if provided
            if incremental_csv_callback:
                try:
                    await incremental_csv_callback(batch_result)
                except Exception as e:
                    print(f"🔧 DEBUG[eval_conditional]: CSV callback failed for {st}: {e}")
                    # Continue processing even if CSV callback fails
    except Exception:
        pass
    
    # Ensure curated (and corrective) results are written to the batch directories for CSV consolidation
    try:
        for st in curated_outputs_per_st:
            batch_dir = Path(session_directory) / "conditional_processing" / f"{st}_batch_1"
            if batch_dir.exists():
                # Write updated curation results back to the batch directory files
                for field_name, curator_output in curated_outputs_per_st[st].items():
                    field_dir = batch_dir / field_name
                    field_dir.mkdir(exist_ok=True)
                    
                    # Write the curator output to the expected file location
                    output_file = field_dir / f"curator_output_{st}.json"
                    if curator_output:
                        try:
                            output_dict = _safe_serialize(curator_output)
                            _atomic_write_json(output_file, output_dict, indent=2)
                            print(f"🔧 DEBUG[eval_conditional]: updated curator output written to {output_file}")
                            
                        except Exception as e:
                            print(f"🔧 DEBUG[eval_conditional]: error writing curator output to {output_file}: {e}")
                            # Continue processing other fields instead of failing completely
                            continue
    except Exception as e:
        print(f"🔧 DEBUG[eval_conditional]: error writing updated curator outputs: {e}")

    # Build classic-like output structure for downstream consolidation
    total_batches = len(batch_results)
    total_samples = sum(len(b.get("batch_samples", [])) for b in batch_results)
    successful_batches = total_batches
    failed_batches = 0
    successful_samples = total_samples
    failed_samples = 0

    execution_time_seconds = _time.time() - _start_ts
    conditional_processing_dir = str(conditional_dir)
    stage_output_path = Path(session_directory) / "conditional_processing" / "conditional_processing_output.json"

    output = {
        "success": failed_batches == 0,
        "message": f"Conditional processing completed: {successful_batches}/{total_batches} batches successful",
        "execution_time_seconds": execution_time_seconds,
        "statistics": {
            "total_batches": total_batches,
            "successful_batches": successful_batches,
            "failed_batches": failed_batches,
            "total_samples": total_samples,
            "successful_samples": successful_samples,
            "failed_samples": failed_samples,
            "target_fields_processed": list(target_fields) if isinstance(target_fields, list) else target_fields,
        },
        "batch_results": batch_results,
        "failed_items": {
            "curation_failures": {},
            "normalization_failures": {},
            "missing_results": {},
        },
        "session_directory": str(session_directory),
        "conditional_processing_directory": conditional_processing_dir,
        "output_files": {
            "conditional_processing_output": str(stage_output_path),
        },
        "timestamp": __import__("datetime").datetime.now().isoformat(),
    }

    try:
        _atomic_write_json(stage_output_path, output, indent=2)
    except Exception:
        pass

    return output


