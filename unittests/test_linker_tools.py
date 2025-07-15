"""
Unit tests for LinkerAgent tools.

This module contains unit tests for the LinkerAgent tools that process
and link metadata files from IngestionAgent output.
"""

import pytest
import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from src.tools.linker_tools import (
    LinkerTools,
    LinkerResult,
    load_mapping_file_impl,
    find_sample_directory_impl,
    clean_metadata_files_impl,
    download_series_matrix_impl,
    extract_matrix_metadata_impl,
    extract_sample_metadata_impl,
    package_linked_data_impl
)


class TestLinkerTools:
    """Test class for LinkerTools functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.session_dir = Path(self.temp_dir)
        self.tools = LinkerTools(str(self.session_dir))
        
        # Create test mapping file
        self.mapping_data = {
            "mapping": {
                "GSE29282": {
                    "sample_ids": ["GSM1000981"],
                    "sample_count": 1,
                    "series_directory": "GSE29282"
                }
            },
            "reverse_mapping": {
                "GSM1000981": "GSE29282"
            },
            "total_series": 1,
            "total_samples": 1
        }
        
        # Create test directory structure
        self.series_dir = self.session_dir / "GSE29282"
        self.series_dir.mkdir(parents=True)
        
        # Create mapping file
        with open(self.session_dir / "series_sample_mapping.json", 'w') as f:
            json.dump(self.mapping_data, f)
        
        # Create test metadata files
        self.sample_metadata = {
            "gsm_id": "GSM1000981",
            "status": "retrieved",
            "submission_date": "2023-01-01",
            "attributes": {
                "title": "Test Sample",
                "organism": "Homo sapiens"
            }
        }
        
        self.series_metadata = {
            "gse_id": "GSE29282",
            "status": "retrieved",
            "submission_date": "2023-01-01",
            "attributes": {
                "title": "Test Series",
                "summary": "Test summary"
            }
        }
        
        self.series_matrix_metadata = {
            "gse_id": "GSE29282",
            "type": "series_matrix_metadata",
            "available_files": ["GSE29282_series_matrix.txt.gz"],
            "file_links": ["https://example.com/GSE29282_series_matrix.txt.gz"]
        }
        
        # Write test files
        with open(self.series_dir / "GSM1000981_metadata.json", 'w') as f:
            json.dump(self.sample_metadata, f)
        
        with open(self.series_dir / "GSE29282_metadata.json", 'w') as f:
            json.dump(self.series_metadata, f)
        
        with open(self.series_dir / "GSE29282_series_matrix.json", 'w') as f:
            json.dump(self.series_matrix_metadata, f)
        
        with open(self.series_dir / "PMID_12345_metadata.json", 'w') as f:
            json.dump({"pmid": 12345, "title": "Test Paper"}, f)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    def test_load_mapping_file_success(self):
        """Test successful loading of mapping file."""
        result = self.tools.load_mapping_file()
        
        assert result.success is True
        assert result.message == "Mapping file loaded successfully"
        assert result.data == self.mapping_data
    
    def test_load_mapping_file_not_found(self):
        """Test loading mapping file when file doesn't exist."""
        # Remove mapping file
        (self.session_dir / "series_sample_mapping.json").unlink()
        
        result = self.tools.load_mapping_file()
        
        assert result.success is False
        assert "not found" in result.message
    
    def test_find_sample_directory_success(self):
        """Test successful finding of sample directory."""
        result = self.tools.find_sample_directory("GSM1000981")
        
        assert result.success is True
        assert result.data['sample_id'] == "GSM1000981"
        assert result.data['series_id'] == "GSE29282"
        assert Path(result.data['directory']).exists()
    
    def test_find_sample_directory_not_found(self):
        """Test finding sample directory when sample doesn't exist."""
        result = self.tools.find_sample_directory("GSM9999999")
        
        assert result.success is False
        assert "not found in mapping" in result.message
    
    def test_clean_metadata_files_success(self):
        """Test successful cleaning of metadata files."""
        fields_to_remove = ["status", "submission_date"]
        result = self.tools.clean_metadata_files("GSM1000981", fields_to_remove)
        
        assert result.success is True
        assert len(result.files_created) == 3  # series, abstract, matrix files
        
        # Check that cleaned files exist
        cleaned_dir = self.series_dir / "cleaned"
        assert cleaned_dir.exists()
        
        # Check that fields were removed
        cleaned_series_file = cleaned_dir / "GSE29282_metadata_cleaned.json"
        with open(cleaned_series_file, 'r') as f:
            cleaned_data = json.load(f)
        
        assert "status" not in cleaned_data
        assert "submission_date" not in cleaned_data
        assert "attributes" in cleaned_data  # Should still have other fields
    
    def test_clean_metadata_files_default_fields(self):
        """Test cleaning metadata files with default fields to remove."""
        result = self.tools.clean_metadata_files("GSM1000981")
        
        assert result.success is True
        assert len(result.files_created) == 3
    
    @patch('urllib.request.urlretrieve')
    def test_download_series_matrix_success(self, mock_urlretrieve):
        """Test successful download of series matrix file."""
        mock_urlretrieve.return_value = None
        
        result = self.tools.download_series_matrix("GSM1000981")
        
        assert result.success is True
        assert result.data['file_name'] == "GSE29282_series_matrix.txt.gz"
        assert result.data['file_url'] == "https://example.com/GSE29282_series_matrix.txt.gz"
        mock_urlretrieve.assert_called_once()
    
    @patch('urllib.request.urlretrieve')
    def test_download_series_matrix_failure(self, mock_urlretrieve):
        """Test failed download of series matrix file."""
        mock_urlretrieve.side_effect = Exception("Download failed")
        
        result = self.tools.download_series_matrix("GSM1000981")
        
        assert result.success is False
        assert "Download failed" in result.message
    
    @patch('urllib.request.urlretrieve')
    @patch('builtins.open', new_callable=mock_open)
    def test_extract_matrix_metadata_success(self, mock_file, mock_urlretrieve):
        """Test successful extraction of matrix metadata."""
        mock_urlretrieve.return_value = None
        
        # Mock file content with metadata lines
        mock_content = """!Series_title="Test Series"
!Series_summary="Test summary"
!Sample_geo_accession="GSM1000981"
ID_REF	VALUE
probe1	100
probe2	200
"""
        mock_file.return_value.read.return_value = mock_content
        
        result = self.tools.extract_matrix_metadata("GSM1000981")
        
        assert result.success is True
        assert "Series_title" in result.data
        assert result.data["Series_title"] == "Test Series"
    
    @patch('urllib.request.urlretrieve')
    @patch('gzip.open', new_callable=mock_open)
    def test_extract_matrix_metadata_gzipped(self, mock_gzip, mock_urlretrieve):
        """Test extraction of metadata from gzipped file."""
        mock_urlretrieve.return_value = None
        
        # Mock gzipped file content
        mock_content = """!Series_title="Test Series"
!Series_summary="Test summary"
ID_REF	VALUE
probe1	100
"""
        mock_gzip.return_value.read.return_value = mock_content
        
        result = self.tools.extract_matrix_metadata("GSM1000981")
        
        assert result.success is True
        assert "Series_title" in result.data
    
    @patch('urllib.request.urlretrieve')
    @patch('builtins.open', new_callable=mock_open)
    def test_extract_sample_metadata_success(self, mock_file, mock_urlretrieve):
        """Test successful extraction of sample metadata."""
        mock_urlretrieve.return_value = None
        
        # Mock file content with sample data
        mock_content = """!Series_title="Test Series"
ID_REF	GSM1000981	GSM1000982
probe1	100	150
probe2	200	250
probe3	300	350
"""
        mock_file.return_value.read.return_value = mock_content
        
        result = self.tools.extract_sample_metadata("GSM1000981")
        
        assert result.success is True
        assert result.data['sample_id'] == "GSM1000981"
        assert result.data['column_index'] == 1
        assert "probe1" in result.data['data']
        assert result.data['data']['probe1'] == "100"
    
    @patch('urllib.request.urlretrieve')
    @patch('builtins.open', new_callable=mock_open)
    def test_extract_sample_metadata_sample_not_found(self, mock_file, mock_urlretrieve):
        """Test extraction when sample is not found in matrix."""
        mock_urlretrieve.return_value = None
        
        mock_content = """!Series_title="Test Series"
ID_REF	GSM1000982	GSM1000983
probe1	100	150
"""
        mock_file.return_value.read.return_value = mock_content
        
        result = self.tools.extract_sample_metadata("GSM1000981")
        
        assert result.success is False
        assert "not found in series matrix columns" in result.message
    
    @patch('urllib.request.urlretrieve')
    @patch('builtins.open', new_callable=mock_open)
    def test_package_linked_data_success(self, mock_file, mock_urlretrieve):
        """Test successful packaging of linked data."""
        mock_urlretrieve.return_value = None
        
        # Mock matrix file content
        mock_content = """!Series_title="Test Series"
ID_REF	GSM1000981
probe1	100
probe2	200
"""
        mock_file.return_value.read.return_value = mock_content
        
        result = self.tools.package_linked_data("GSM1000981")
        
        assert result.success is True
        assert result.data['sample_id'] == "GSM1000981"
        assert result.data['series_id'] == "GSE29282"
        assert 'cleaned_files' in result.data
        assert 'sample_metadata' in result.data
        assert 'matrix_metadata' in result.data
        assert 'sample_matrix_data' in result.data
        assert len(result.files_created) == 1  # packaged file


class TestLinkerToolsImplementations:
    """Test class for implementation functions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.session_dir = str(self.temp_dir)
        
        # Create test mapping file
        mapping_data = {
            "reverse_mapping": {
                "GSM1000981": "GSE29282"
            }
        }
        
        with open(Path(self.temp_dir) / "series_sample_mapping.json", 'w') as f:
            json.dump(mapping_data, f)
        
        # Create series directory
        series_dir = Path(self.temp_dir) / "GSE29282"
        series_dir.mkdir()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    def test_load_mapping_file_impl(self):
        """Test load_mapping_file_impl function."""
        result = load_mapping_file_impl(self.session_dir)
        
        assert result['success'] is True
        assert 'reverse_mapping' in result['data']
    
    def test_find_sample_directory_impl(self):
        """Test find_sample_directory_impl function."""
        result = find_sample_directory_impl("GSM1000981", self.session_dir)
        
        assert result['success'] is True
        assert result['data']['sample_id'] == "GSM1000981"
        assert result['data']['series_id'] == "GSE29282"
    
    def test_find_sample_directory_impl_not_found(self):
        """Test find_sample_directory_impl when sample not found."""
        result = find_sample_directory_impl("GSM9999999", self.session_dir)
        
        assert result['success'] is False
        assert "not found in mapping" in result['message']


def test_get_gsm_metadata_with_linker():
    """Integration test combining ingestion and linker functionality."""
    # This would test the full workflow but requires actual data
    # For now, we'll test that the functions can be imported and called
    try:
        from src.tools.linker_tools import LinkerTools
        from src.tools.ingestion_tools import NCBIClient
        
        # Test that classes can be instantiated
        tools = LinkerTools("/tmp/test")
        client = NCBIClient()
        
        assert tools is not None
        assert client is not None
        print("Integration test passed - classes can be instantiated")
    except Exception as e:
        print(f"Integration test failed: {e}")
        raise


if __name__ == "__main__":
    # Run basic tests
    test_get_gsm_metadata_with_linker()
    print("Basic integration test passed!") 