"""
Preprocessing workflow for sample type curation and batch organization.

This workflow handles the second stage of the batch processing pipeline:
1. Takes raw metadata from data intake workflow
2. Performs sample type curation using CuratorAgent with Gemini Flash
3. Groups samples by sample type 
4. Creates batches within each sample type group
5. Outputs organized sample batches ready for conditional processing

The workflow produces:
- sample_type_mapping.json: Maps sample_id -> {sample_type, batch_id}
- Sample type directories with curation results
- Batch organization files for downstream processing
"""

import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import logging

from src.agents.curator import run_curator_agent
from agents import ModelProvider
from src.models import LinkerOutput

logger = logging.getLogger(__name__)


class PreprocessingWorkflow:
    """
    Preprocessing workflow for sample type curation and batch organization.
    
    This workflow bridges the data intake stage and conditional processing stage
    by performing sample type curation and organizing samples into batches by type.
    """
    
    def __init__(
        self,
        data_intake_output: LinkerOutput,
        session_directory: str,
        batch_size: int = 5,
        sample_type_filter: str = None,
        model_provider=None,
        max_tokens: int = None,
    ):
        """
        Initialize the preprocessing workflow.
        
        Parameters
        ----------
        session_directory : str
            Path to the session directory containing data intake outputs
        batch_size : int, optional
            Number of samples per batch (default: 5)
        sample_type_filter : str, optional
            Filter to process only specific sample type. If None, processes all sample types.
            Available types: primary_sample, cell_line, unknown
        model_provider : ModelProvider, optional
            Model provider for sample type curation (should be Gemini Flash)
        max_tokens : int, optional
            Maximum tokens for LLM responses
        """
        self.data_intake_output = data_intake_output
        self.session_directory = Path(session_directory)
        self.batch_size = batch_size
        self.sample_type_filter = sample_type_filter
        self.model_provider = model_provider
        self.max_tokens = max_tokens
        
        # Create discovery directory structure (matching original workflow)
        self.discovery_dir = self.session_directory / "discovery"
        self.discovery_dir.mkdir(exist_ok=True)
        
        # Create subdirectories within discovery (matching original structure)
        self.discovery_outputs_dir = self.discovery_dir / "outputs"
        self.discovery_outputs_dir.mkdir(exist_ok=True)
        
        self.discovery_raw_data_dir = self.discovery_dir / "raw_data"
        self.discovery_raw_data_dir.mkdir(exist_ok=True)
        
        # Create preprocessing directory for workflow outputs
        self.preprocessing_dir = self.session_directory / "preprocessing"
        self.preprocessing_dir.mkdir(exist_ok=True)
        
        # Output files
        self.sample_type_mapping_file = self.discovery_dir / "sample_type_mapping.json"  # Put in discovery dir like original
        self.series_sample_mapping_file = self.discovery_dir / "series_sample_mapping.json"  # Copy from data intake
        self.preprocessing_output_file = self.preprocessing_dir / "preprocessing_output.json"
        
        # Validate sample_type_filter if provided
        if self.sample_type_filter and self.sample_type_filter not in ["primary_sample", "cell_line", "unknown"]:
            raise ValueError(f"Invalid sample_type_filter: {self.sample_type_filter}. Must be one of: primary_sample, cell_line, unknown")

    async def discover_sample_types(
        self, samples: List[str], discovery_batch_size: int = None
    ) -> Dict[str, str]:
        """
        Discover sample types for the given samples using sample type curation.
        
        Parameters
        ----------
        samples : List[str]
            List of sample IDs to process
        discovery_batch_size : int, optional
            Batch size for sample type discovery (defaults to self.batch_size)
            
        Returns
        -------
        Dict[str, str]
            Dictionary mapping sample_id -> sample_type
        """
        # Use instance batch_size if not specified
        if discovery_batch_size is None:
            discovery_batch_size = self.batch_size
            
        logger.info(f"🔍 Starting sample type discovery for {len(samples)} samples")
        logger.info(f"📋 Using discovery batch size: {discovery_batch_size}")
        
        # Create batches for discovery
        discovery_batches = []
        for i in range(0, len(samples), discovery_batch_size):
            batch = samples[i:i + discovery_batch_size]
            discovery_batches.append(batch)
        
        logger.info(f"📋 Created {len(discovery_batches)} discovery batches")
        
        # Store all sample type mappings and cached results
        all_sample_type_mapping = {}
        cached_initial_results = {}
        
        # Process discovery batches
        for batch_num, batch_samples in enumerate(discovery_batches, 1):
            try:
                logger.info(f"🔄 Processing discovery batch {batch_num}/{len(discovery_batches)} with {len(batch_samples)} samples")
                
                # Run sample type curation using existing data intake output
                logger.info(f"🔍 DEBUG: Original data_intake_output has {len(self.data_intake_output.curation_packages) if self.data_intake_output.curation_packages else 0} curation packages")
                
                # Filter data intake output for this batch
                batch_data_intake = self._filter_data_intake_for_batch(self.data_intake_output, batch_samples)
                logger.info(f"🔍 DEBUG: Filtered data_intake_output has {len(batch_data_intake.curation_packages) if batch_data_intake.curation_packages else 0} curation packages")
                
                # Create sample type curation-specific model provider (using faster model for sample types)
                curation_model_provider = self._create_sample_type_model_provider(self.model_provider)
                
                # Run curator agent for sample type using discovery directory
                curator_result = await run_curator_agent(
                    data_intake_output=batch_data_intake,
                    target_field="sample_type",
                    session_id="discovery",  # Use unified discovery session like original workflow
                    sandbox_dir=str(self.discovery_dir),  # Use discovery directory
                    model_provider=curation_model_provider,
                    max_tokens=self.max_tokens,
                )
                
                if curator_result.success:
                    # Extract sample type mapping from curator results
                    batch_sample_types = self._extract_sample_types_from_curator_result(curator_result)
                    all_sample_type_mapping.update(batch_sample_types)
                    
                    # Store cached results per individual sample (matching original workflow structure)
                    for sample_id in batch_samples:
                        sample_type = batch_sample_types.get(sample_id, "failed")
                        cached_initial_results[sample_id] = {
                            "discovery_session_id": "discovery",  # Use unified discovery session
                            "session_directory": str(self.discovery_dir),  # Use discovery directory
                            "sample_type": sample_type,
                            "batch_id": batch_num,
                            "initial_curator_outputs": {}  # Not needed since we're only doing sample type curation
                        }
                    
                    # Log sample type distribution for this batch
                    batch_distribution = {}
                    for sample_id in batch_samples:
                        sample_type = batch_sample_types.get(sample_id, "failed")
                        batch_distribution[sample_type] = batch_distribution.get(sample_type, 0) + 1
                    
                    logger.info(f"✅ Discovery batch {batch_num} completed. Sample types: {batch_distribution}")
                    
                else:
                    logger.error(f"❌ Discovery batch {batch_num} failed: {curator_result.message}")
                    
                    # Mark all samples in this batch as failed
                    for sample_id in batch_samples:
                        all_sample_type_mapping[sample_id] = "failed"
                        
            except Exception as e:
                logger.error(f"❌ Discovery batch {batch_num} failed with exception: {str(e)}")
                
                # Mark all samples in this batch as failed
                for sample_id in batch_samples:
                    all_sample_type_mapping[sample_id] = "failed"
                    cached_initial_results[sample_id] = {
                        "discovery_session_id": "discovery",
                        "session_directory": str(self.session_directory / "discovery"),
                        "sample_type": "failed",
                        "batch_id": batch_num
                    }
        
        # Create unified sample type mapping with batch IDs
        unified_sample_type_mapping = {}
        for sample_id, cache_info in cached_initial_results.items():
            unified_sample_type_mapping[sample_id] = {
                "sample_type": cache_info["sample_type"],
                "batch_id": cache_info["batch_id"]
            }
        
        # Save unified sample_type_mapping.json
        with open(self.sample_type_mapping_file, "w") as f:
            json.dump(unified_sample_type_mapping, f, indent=2)
        
        logger.info(f"💾 Saved sample type mapping to {self.sample_type_mapping_file}")
        
        # Log final sample type distribution
        final_distribution = {}
        for sample_type in all_sample_type_mapping.values():
            final_distribution[sample_type] = final_distribution.get(sample_type, 0) + 1
        
        logger.info(f"🎯 Final sample type distribution: {final_distribution}")
        
        # Report failed samples
        failed_samples = [sid for sid, stype in all_sample_type_mapping.items() if stype == "failed"]
        if failed_samples:
            logger.warning(f"⚠️ {len(failed_samples)} samples failed sample type determination: {failed_samples}")
            
            # Save failed samples for reporting
            try:
                with open(self.session_directory / "failed_sample_type_discovery.json", "w") as f:
                    json.dump({
                        "failed_samples": failed_samples,
                        "reason": "Sample type determination failed during discovery phase",
                        "timestamp": datetime.now().isoformat()
                    }, f, indent=2)
            except Exception as e:
                logger.error(f"Failed to save failed samples report: {e}")
        
        return all_sample_type_mapping

    def create_sample_type_batches(
        self, samples: List[str], sample_type_mapping: Dict[str, str]
    ) -> Dict[str, List[List[str]]]:
        """
        Create batches grouped by sample type with fixed batch size.
        
        Parameters
        ----------
        samples : List[str]
            List of GSM samples
        sample_type_mapping : Dict[str, str]
            Dictionary mapping sample_id -> sample_type
            
        Returns
        -------
        Dict[str, List[List[str]]]
            Dictionary mapping sample_type -> list of batches
        """
        
        # Group samples by sample type
        sample_type_groups = {
            "primary_sample": [],
            "cell_line": [],
            "unknown": [],
            "failed": []
        }
        
        for sample_id in samples:
            sample_type = sample_type_mapping.get(sample_id, "failed")
            sample_type_groups[sample_type].append(sample_id)
        
        # Create batches for each sample type
        sample_type_batches = {}
        
        for sample_type, type_samples in sample_type_groups.items():
            if not type_samples:
                continue  # Skip empty groups
                
            # Apply sample type filter if specified
            if self.sample_type_filter and sample_type != self.sample_type_filter:
                logger.info(f"⏭️ Skipping {sample_type} samples due to filter (filter: {self.sample_type_filter})")
                continue
                
            type_batches = []
            for i in range(0, len(type_samples), self.batch_size):
                batch = type_samples[i : i + self.batch_size]
                type_batches.append(batch)
            
            sample_type_batches[sample_type] = type_batches
            logger.info(f"📦 Created {len(type_batches)} batches for {sample_type} with {len(type_samples)} samples")
       
        return sample_type_batches

    async def run_preprocessing(self, samples: List[str]) -> Dict[str, Any]:
        """
        Run the complete preprocessing workflow.
        
        Parameters
        ----------
        samples : List[str]
            List of sample IDs to process
            
        Returns
        -------
        Dict[str, Any]
            Preprocessing workflow results including sample type batches
        """
        start_time = time.time()
        
        logger.info(f"🚀 Starting preprocessing workflow for {len(samples)} samples")
        
        try:
            # Copy GSE directories and metadata from data intake to discovery directory
            await self._copy_data_intake_to_discovery()
            
            # Discover sample types
            sample_type_mapping = await self.discover_sample_types(samples)
            
            # Create sample type batches
            sample_type_batches = self.create_sample_type_batches(samples, sample_type_mapping)
            
            # Calculate statistics
            total_batches = sum(len(batches) for batches in sample_type_batches.values())
            processed_samples = sum(len(samples) for batch_list in sample_type_batches.values() for samples in batch_list)
            
            execution_time = time.time() - start_time
            
            # Create output structure
            output = {
                "success": True,
                "message": f"Preprocessing completed successfully for {processed_samples} samples in {total_batches} batches",
                "execution_time_seconds": execution_time,
                "sample_type_mapping": sample_type_mapping,
                "sample_type_batches": sample_type_batches,
                "statistics": {
                    "total_samples": len(samples),
                    "processed_samples": processed_samples,
                    "failed_samples": len(samples) - processed_samples,
                    "total_batches": total_batches,
                    "sample_type_distribution": {
                        sample_type: len([s for s in sample_type_mapping.values() if s == sample_type])
                        for sample_type in ["primary_sample", "cell_line", "unknown", "failed"]
                    }
                },
                "session_directory": str(self.session_directory),
                "preprocessing_directory": str(self.preprocessing_dir),
                "discovery_directory": str(self.discovery_dir),
                "output_files": {
                    "sample_type_mapping": str(self.sample_type_mapping_file),
                    "series_sample_mapping": str(self.series_sample_mapping_file),
                    "preprocessing_output": str(self.preprocessing_output_file),
                    "discovery_outputs": str(self.discovery_outputs_dir),
                    "discovery_raw_data": str(self.discovery_raw_data_dir)
                },
                "timestamp": datetime.now().isoformat()
            }
            
            # Save preprocessing output
            with open(self.preprocessing_output_file, "w") as f:
                json.dump(output, f, indent=2)
            
            logger.info(f"✅ Preprocessing workflow completed in {execution_time:.2f} seconds")
            logger.info(f"📊 Statistics: {output['statistics']}")
            
            return output
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"❌ Preprocessing workflow failed: {str(e)}")
            
            error_output = {
                "success": False,
                "message": f"Preprocessing workflow failed: {str(e)}",
                "execution_time_seconds": execution_time,
                "error": str(e),
                "session_directory": str(self.session_directory),
                "timestamp": datetime.now().isoformat()
            }
            
            # Save error output
            with open(self.preprocessing_output_file, "w") as f:
                json.dump(error_output, f, indent=2)
            
            return error_output

    async def _copy_data_intake_to_discovery(self):
        """
        Copy GSE directories and metadata files from data intake stage to discovery directory.
        This replicates the directory structure from the original workflow.
        """
        import shutil
        
        logger.info("📁 Copying data intake outputs to discovery directory structure")
        
        # Get the data intake session directory
        data_intake_session_dir = Path(self.data_intake_output.session_directory)
        
        # Copy all GSE directories from data intake to discovery
        if data_intake_session_dir.exists():
            for item in data_intake_session_dir.iterdir():
                if item.is_dir() and item.name.startswith("GSE"):
                    target_dir = self.discovery_dir / item.name
                    if target_dir.exists():
                        shutil.rmtree(target_dir)
                    shutil.copytree(item, target_dir)
                    logger.info(f"📂 Copied {item.name} to discovery directory")
                
                # Copy important files like series_sample_mapping.json
                elif item.is_file() and item.name in ["series_sample_mapping.json"]:
                    target_file = self.discovery_dir / item.name
                    shutil.copy2(item, target_file)
                    logger.info(f"📄 Copied {item.name} to discovery directory")
        
        # Copy outputs and raw_data directories if they exist
        for dir_name in ["outputs", "raw_data"]:
            source_dir = data_intake_session_dir / dir_name
            target_dir = self.discovery_dir / dir_name
            if source_dir.exists():
                if target_dir.exists():
                    shutil.rmtree(target_dir)
                shutil.copytree(source_dir, target_dir)
                logger.info(f"📂 Copied {dir_name} directory to discovery")
        
        logger.info("✅ Data intake to discovery copy completed")

    def _filter_data_intake_for_batch(self, data_intake_output: LinkerOutput, batch_samples: List[str]) -> LinkerOutput:
        """
        Filter the data intake output to only include samples in the current batch.
        
        Parameters
        ----------
        data_intake_output : LinkerOutput
            Original data intake output
        batch_samples : List[str]
            Sample IDs in the current batch
            
        Returns
        -------
        LinkerOutput
            Filtered data intake output
        """
        # Filter curation packages to only include samples in this batch
        filtered_packages = []
        for package in data_intake_output.curation_packages:
            if package.sample_id in batch_samples:
                filtered_packages.append(package)
        
        # Create new LinkerOutput with filtered packages
        filtered_output = LinkerOutput(
            success=data_intake_output.success,
            message=data_intake_output.message,
            execution_time_seconds=data_intake_output.execution_time_seconds,
            sample_ids_requested=batch_samples,
            session_directory=data_intake_output.session_directory,
            curation_packages=filtered_packages,
            sample_ids_for_curation=batch_samples,  # This is what the curator agent needs
        )
        
        return filtered_output

    def _extract_sample_types_from_curator_result(self, curator_result) -> Dict[str, str]:
        """
        Extract sample type mapping from curator agent result.
        
        Parameters
        ----------
        curator_result
            Result from curator agent containing SampleTypeCurationResult objects
            
        Returns
        -------
        Dict[str, str]
            Mapping of sample_id to sample_type
        """
        sample_type_mapping = {}
        
        try:
            if hasattr(curator_result, 'curation_results') and curator_result.curation_results:
                for result in curator_result.curation_results:
                    # Extract from SampleTypeCurationResult object
                    sample_id = result.sample_id
                    sample_type = str(result.sample_type.value) if hasattr(result.sample_type, 'value') else str(result.sample_type)
                    if sample_id and sample_type:
                        sample_type_mapping[sample_id] = sample_type
                        logger.info(f"🔍 Extracted sample type: {sample_id} -> {sample_type}")
                        
        except Exception as e:
            logger.warning(f"Error extracting sample types from curator result: {e}")
            logger.info(f"🔍 Curator result structure: {type(curator_result)}")
            if hasattr(curator_result, 'curation_results'):
                logger.info(f"🔍 Curation results count: {len(curator_result.curation_results) if curator_result.curation_results else 0}")
            
        return sample_type_mapping

    def _create_sample_type_model_provider(self, base_model_provider: ModelProvider) -> ModelProvider:
        """
        Create a model provider optimized for sample type curation (using faster model).
        
        Parameters
        ----------
        base_model_provider : ModelProvider
            Base model provider
            
        Returns
        -------
        ModelProvider
            Model provider optimized for sample type curation
        """
        # Use Gemini Flash for sample type curation (faster and cost-effective)
        if hasattr(base_model_provider, 'model_name'):
            # Create a copy with Gemini Flash for sample type curation
            sample_type_provider = ModelProvider(
                model_name="google/gemini-2.5-flash",
                max_tokens=base_model_provider.max_tokens if hasattr(base_model_provider, 'max_tokens') else None
            )
            return sample_type_provider
        
        return base_model_provider


async def run_preprocessing_workflow(
    data_intake_output: LinkerOutput,
    samples: List[str],
    session_directory: str,
    batch_size: int = 5,
    sample_type_filter: str = None,
    model_provider=None,
    max_tokens: int = None,
) -> Dict[str, Any]:
    """
    Run the preprocessing workflow.
    
    Parameters
    ----------
    data_intake_output : LinkerOutput
        Output from the data intake workflow
    samples : List[str]
        List of sample IDs to process
    session_directory : str
        Path to the session directory
    batch_size : int, optional
        Number of samples per batch (default: 5)
    sample_type_filter : str, optional
        Filter to process only specific sample type
    model_provider : ModelProvider, optional
        Model provider for sample type curation (should be Gemini Flash)
    max_tokens : int, optional
        Maximum tokens for LLM responses
        
    Returns
    -------
    Dict[str, Any]
        Preprocessing workflow results
    """
    workflow = PreprocessingWorkflow(
        data_intake_output=data_intake_output,
        session_directory=session_directory,
        batch_size=batch_size,
        sample_type_filter=sample_type_filter,
        model_provider=model_provider,
        max_tokens=max_tokens,
    )
    
    return await workflow.run_preprocessing(samples)


if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="Preprocessing workflow for sample type curation and batch organization")
    
    parser.add_argument("--samples", "-s", required=True, help="Comma-separated list of sample IDs")
    parser.add_argument("--session-dir", "-d", required=True, help="Session directory path")
    parser.add_argument("--batch-size", "-b", type=int, default=5, help="Batch size (default: 5)")
    parser.add_argument("--sample-type-filter", "-f", choices=["primary_sample", "cell_line", "unknown"], help="Filter by sample type")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Set up logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Parse samples
    samples = [s.strip() for s in args.samples.split(",")]
    
    # Mock data intake output for standalone testing
    from src.models import LinkerOutput
    mock_data_intake = LinkerOutput(
        success=True,
        message="Mock data intake output",
        execution_time_seconds=0.0,
        sample_ids_requested=samples,
        session_directory=args.session_dir,
        files_created=[],
        successfully_linked=samples,
        failed_linking=[],
        warnings=[],
        sample_ids_for_curation=samples,
        recommended_curation_fields=["Disease", "Tissue", "Age", "Organ"],
        fields_removed_during_cleaning=[],
        linked_data=None,
        cleaned_metadata_files=None,
        cleaned_series_metadata=None,
        cleaned_sample_metadata=None,
        cleaned_abstract_metadata=None,
    )
    
    # Run workflow
    async def main():
        result = await run_preprocessing_workflow(
            data_intake_output=mock_data_intake,
            samples=samples,
            session_directory=args.session_dir,
            batch_size=args.batch_size,
            sample_type_filter=args.sample_type_filter,
        )
        
        print(f"Preprocessing Result: {'✅ Success' if result['success'] else '❌ Failed'}")
        print(f"Message: {result['message']}")
        if result['success']:
            print(f"Statistics: {result['statistics']}")
        
        return 0 if result['success'] else 1
    
    # Run the async main function
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
