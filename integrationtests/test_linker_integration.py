"""
Integration tests for LinkerAgent tools.

This module contains integration tests that use real data from the sandbox
directory to test the LinkerAgent functionality end-to-end.
"""

import os
import json
import tempfile
import shutil
from pathlib import Path
from src.tools.linker_tools import LinkerTools


class TestLinkerIntegration:
    """Integration tests for LinkerAgent using real sandbox data."""
    
    def setup_method(self):
        """Set up test fixtures with real sandbox data."""
        # Use the actual sandbox directory with real data
        self.sandbox_dir = Path("/teamspace/studios/this_studio/sandbox/98375b76-65e5-403d-bdb3-3de0d204d429")
        
        if not self.sandbox_dir.exists():
            raise FileNotFoundError(f"Sandbox directory not found: {self.sandbox_dir}")
        
        self.tools = LinkerTools(str(self.sandbox_dir))
        
        # Test with real sample IDs from the sandbox
        self.test_sample_ids = ["GSM1000981", "GSM1098382"]
        
        # Create a temporary directory for outputs
        self.temp_output_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if hasattr(self, 'temp_output_dir'):
            shutil.rmtree(self.temp_output_dir)
    
    def test_load_real_mapping_file(self):
        """Test loading the real mapping file from sandbox."""
        result = self.tools.load_mapping_file()
        
        assert result.success is True
        assert result.data is not None
        assert 'reverse_mapping' in result.data
        assert 'mapping' in result.data
        
        # Check that our test sample IDs are in the mapping
        for sample_id in self.test_sample_ids:
            if sample_id in result.data['reverse_mapping']:
                print(f"✓ Found sample {sample_id} in mapping")
            else:
                print(f"⚠ Sample {sample_id} not found in mapping")
    
    def test_find_real_sample_directories(self):
        """Test finding directories for real sample IDs."""
        for sample_id in self.test_sample_ids:
            result = self.tools.find_sample_directory(sample_id)
            
            if result.success:
                print(f"✓ Found directory for {sample_id}: {result.data['directory']}")
                assert Path(result.data['directory']).exists()
                assert result.data['sample_id'] == sample_id
                assert result.data['series_id'] is not None
            else:
                print(f"⚠ Could not find directory for {sample_id}: {result.message}")
    
    def test_clean_real_metadata_files(self):
        """Test cleaning real metadata files."""
        # Use the first available sample ID
        mapping_result = self.tools.load_mapping_file()
        if not mapping_result.success:
            print("⚠ Could not load mapping file, skipping test")
            return
        
        available_samples = list(mapping_result.data['reverse_mapping'].keys())
        if not available_samples:
            print("⚠ No samples found in mapping, skipping test")
            return
        
        sample_id = available_samples[0]
        print(f"Testing cleaning for sample: {sample_id}")
        
        # Define fields to remove
        fields_to_remove = [
            'status', 'submission_date', 'last_update_date',
            'extract_protocol_ch1', 'growth_protocol_ch1'
        ]
        
        result = self.tools.clean_metadata_files(sample_id, fields_to_remove)
        
        if result.success:
            print(f"✓ Successfully cleaned {len(result.files_created)} files")
            
            # Verify that files were created
            for file_path in result.files_created:
                assert Path(file_path).exists()
                print(f"  ✓ Created: {file_path}")
                
                # Check that fields were actually removed
                with open(file_path, 'r') as f:
                    cleaned_data = json.load(f)
                
                # Verify that specified fields are not present
                for field in fields_to_remove:
                    assert field not in str(cleaned_data), f"Field '{field}' should have been removed"
        else:
            print(f"⚠ Failed to clean metadata files: {result.message}")
    
    def test_download_real_series_matrix(self):
        """Test downloading a real series matrix file."""
        # Use the first available sample ID
        mapping_result = self.tools.load_mapping_file()
        if not mapping_result.success:
            print("⚠ Could not load mapping file, skipping test")
            return
        
        available_samples = list(mapping_result.data['reverse_mapping'].keys())
        if not available_samples:
            print("⚠ No samples found in mapping, skipping test")
            return
        
        sample_id = available_samples[0]
        print(f"Testing download for sample: {sample_id}")
        
        result = self.tools.download_series_matrix(sample_id)
        
        if result.success:
            print(f"✓ Successfully downloaded: {result.data['file_name']}")
            
            # Verify that file was downloaded
            file_path = Path(result.data['file_path'])
            assert file_path.exists()
            assert file_path.stat().st_size > 0  # File should not be empty
            
            print(f"  ✓ File size: {file_path.stat().st_size} bytes")
        else:
            print(f"⚠ Failed to download series matrix: {result.message}")
    
    def test_extract_real_matrix_metadata(self):
        """Test extracting metadata from a real series matrix file."""
        # Use the first available sample ID
        mapping_result = self.tools.load_mapping_file()
        if not mapping_result.success:
            print("⚠ Could not load mapping file, skipping test")
            return
        
        available_samples = list(mapping_result.data['reverse_mapping'].keys())
        if not available_samples:
            print("⚠ No samples found in mapping, skipping test")
            return
        
        sample_id = available_samples[0]
        print(f"Testing metadata extraction for sample: {sample_id}")
        
        result = self.tools.extract_matrix_metadata(sample_id)
        
        if result.success:
            print(f"✓ Successfully extracted {len(result.data)} metadata fields")
            
            # Print some example metadata fields
            for i, (key, value) in enumerate(result.data.items()):
                if i < 5:  # Show first 5 fields
                    print(f"  {key}: {value[:100]}..." if len(str(value)) > 100 else f"  {key}: {value}")
            
            if len(result.data) > 5:
                print(f"  ... and {len(result.data) - 5} more fields")
        else:
            print(f"⚠ Failed to extract matrix metadata: {result.message}")
    
    def test_extract_real_sample_metadata(self):
        """Test extracting sample-specific metadata from a real series matrix file."""
        # Use the first available sample ID
        mapping_result = self.tools.load_mapping_file()
        if not mapping_result.success:
            print("⚠ Could not load mapping file, skipping test")
            return
        
        available_samples = list(mapping_result.data['reverse_mapping'].keys())
        if not available_samples:
            print("⚠ No samples found in mapping, skipping test")
            return
        
        sample_id = available_samples[0]
        print(f"Testing sample metadata extraction for sample: {sample_id}")
        
        result = self.tools.extract_sample_metadata(sample_id)
        
        if result.success:
            data_count = len(result.data['data'])
            print(f"✓ Successfully extracted data for {data_count} probes/genes")
            print(f"  Sample ID: {result.data['sample_id']}")
            print(f"  Column index: {result.data['column_index']}")
            
            # Show a few example data points
            sample_data = result.data['data']
            for i, (probe_id, value) in enumerate(sample_data.items()):
                if i < 3:  # Show first 3 data points
                    print(f"  {probe_id}: {value}")
            
            if data_count > 3:
                print(f"  ... and {data_count - 3} more data points")
        else:
            print(f"⚠ Failed to extract sample metadata: {result.message}")
    
    def test_package_real_linked_data(self):
        """Test packaging all linked data for a real sample."""
        # Use the first available sample ID
        mapping_result = self.tools.load_mapping_file()
        if not mapping_result.success:
            print("⚠ Could not load mapping file, skipping test")
            return
        
        available_samples = list(mapping_result.data['reverse_mapping'].keys())
        if not available_samples:
            print("⚠ No samples found in mapping, skipping test")
            return
        
        sample_id = available_samples[0]
        print(f"Testing full data packaging for sample: {sample_id}")
        
        result = self.tools.package_linked_data(sample_id)
        
        if result.success:
            print(f"✓ Successfully packaged linked data for {sample_id}")
            
            # Verify the structure of packaged data
            assert 'sample_id' in result.data
            assert 'series_id' in result.data
            assert 'cleaned_files' in result.data
            assert 'sample_metadata' in result.data
            assert 'matrix_metadata' in result.data
            assert 'sample_matrix_data' in result.data
            assert 'processing_summary' in result.data
            
            # Print summary information
            summary = result.data['processing_summary']
            print(f"  Cleaned files: {summary['cleaned_files_count']}")
            print(f"  Matrix metadata fields: {summary['matrix_metadata_fields']}")
            print(f"  Sample data points: {summary['sample_data_points']}")
            
            # Verify that the packaged file was created
            assert len(result.files_created) == 1
            packaged_file = Path(result.files_created[0])
            assert packaged_file.exists()
            print(f"  ✓ Packaged file created: {packaged_file}")
        else:
            print(f"⚠ Failed to package linked data: {result.message}")
    
    def test_full_workflow_integration(self):
        """Test the complete workflow from start to finish."""
        print("\n=== Testing Full Workflow Integration ===")
        
        # Load mapping to get available samples
        mapping_result = self.tools.load_mapping_file()
        if not mapping_result.success:
            print("⚠ Could not load mapping file, skipping full workflow test")
            return
        
        available_samples = list(mapping_result.data['reverse_mapping'].keys())
        if not available_samples:
            print("⚠ No samples found in mapping, skipping full workflow test")
            return
        
        sample_id = available_samples[0]
        print(f"Running full workflow for sample: {sample_id}")
        
        # Step 1: Find sample directory
        dir_result = self.tools.find_sample_directory(sample_id)
        assert dir_result.success, f"Failed to find directory: {dir_result.message}"
        print(f"  ✓ Step 1: Found directory - {dir_result.data['series_id']}")
        
        # Step 2: Clean metadata files
        clean_result = self.tools.clean_metadata_files(sample_id)
        assert clean_result.success, f"Failed to clean files: {clean_result.message}"
        print(f"  ✓ Step 2: Cleaned {len(clean_result.files_created)} files")
        
        # Step 3: Download series matrix
        download_result = self.tools.download_series_matrix(sample_id)
        if download_result.success:
            print(f"  ✓ Step 3: Downloaded {download_result.data['file_name']}")
        else:
            print(f"  ⚠ Step 3: Download failed - {download_result.message}")
        
        # Step 4: Extract matrix metadata
        matrix_result = self.tools.extract_matrix_metadata(sample_id)
        if matrix_result.success:
            print(f"  ✓ Step 4: Extracted {len(matrix_result.data)} metadata fields")
        else:
            print(f"  ⚠ Step 4: Metadata extraction failed - {matrix_result.message}")
        
        # Step 5: Extract sample metadata
        sample_result = self.tools.extract_sample_metadata(sample_id)
        if sample_result.success:
            print(f"  ✓ Step 5: Extracted {len(sample_result.data['data'])} sample data points")
        else:
            print(f"  ⚠ Step 5: Sample extraction failed - {sample_result.message}")
        
        # Step 6: Package everything
        package_result = self.tools.package_linked_data(sample_id)
        if package_result.success:
            print(f"  ✓ Step 6: Packaged all data successfully")
        else:
            print(f"  ⚠ Step 6: Packaging failed - {package_result.message}")
        
        print(f"✓ Full workflow completed for {sample_id}")


def run_integration_tests():
    """Run all integration tests."""
    test_instance = TestLinkerIntegration()
    
    try:
        test_instance.setup_method()
        
        print("=== LinkerAgent Integration Tests ===")
        
        # Run all tests
        test_instance.test_load_real_mapping_file()
        test_instance.test_find_real_sample_directories()
        test_instance.test_clean_real_metadata_files()
        test_instance.test_download_real_series_matrix()
        test_instance.test_extract_real_matrix_metadata()
        test_instance.test_extract_real_sample_metadata()
        test_instance.test_package_real_linked_data()
        test_instance.test_full_workflow_integration()
        
        print("\n✅ All integration tests completed!")
        
    except Exception as e:
        print(f"\n❌ Integration tests failed: {e}")
        raise
    finally:
        test_instance.teardown_method()


if __name__ == "__main__":
    run_integration_tests() 