import os
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, mock_open

from src.tools.file_ops import read_file, write_file, update_file, list_dir, delete_file


class TestReadFile:
    """Test read_file function."""
    
    def test_read_file_success(self):
        """Test successful file reading."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
            tmp.write("Hello, World!")
            tmp_path = tmp.name
        
        try:
            result = read_file(tmp_path)
            assert result == "Hello, World!"
        finally:
            os.unlink(tmp_path)
    
    def test_read_file_not_found(self):
        """Test reading non-existent file."""
        result = read_file("nonexistent_file.txt")
        assert "Error reading file:" in result
    
    def test_read_file_permission_error(self):
        """Test reading file with permission error."""
        with patch("builtins.open", mock_open()) as mock_file:
            mock_file.side_effect = PermissionError("Permission denied")
            result = read_file("test.txt")
            assert "Error reading file:" in result
            assert "Permission denied" in result


class TestWriteFile:
    """Test write_file function."""
    
    def test_write_file_success(self):
        """Test successful file writing."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            content = "Hello, World!"
            result = write_file(tmp_path, content)
            assert "written successfully" in result
            
            # Verify content was written
            with open(tmp_path, 'r') as f:
                assert f.read() == content
        finally:
            os.unlink(tmp_path)
    
    def test_write_file_new_file(self):
        """Test writing to new file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            file_path = os.path.join(tmp_dir, "new_file.txt")
            content = "New content"
            
            result = write_file(file_path, content)
            assert "written successfully" in result
            
            # Verify file was created and content written
            assert os.path.exists(file_path)
            with open(file_path, 'r') as f:
                assert f.read() == content
    
    def test_write_file_error(self):
        """Test writing to invalid path."""
        result = write_file("/invalid/path/file.txt", "content")
        assert "Error writing to file:" in result


class TestUpdateFile:
    """Test update_file function."""
    
    def test_update_file_success(self):
        """Test successful file update."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
            tmp.write("Hello, World!")
            tmp_path = tmp.name
        
        try:
            result = update_file(tmp_path, "World", "Universe")
            assert "updated successfully" in result
            
            # Verify content was updated
            with open(tmp_path, 'r') as f:
                assert f.read() == "Hello, Universe!"
        finally:
            os.unlink(tmp_path)
    
    def test_update_file_no_match(self):
        """Test updating file with no matching content."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
            tmp.write("Hello, World!")
            tmp_path = tmp.name
        
        try:
            result = update_file(tmp_path, "NotFound", "Replacement")
            assert "updated successfully" in result
            
            # Verify content unchanged
            with open(tmp_path, 'r') as f:
                assert f.read() == "Hello, World!"
        finally:
            os.unlink(tmp_path)
    
    def test_update_file_not_found(self):
        """Test updating non-existent file."""
        result = update_file("nonexistent_file.txt", "find", "replace")
        assert "Error updating file:" in result


class TestListDir:
    """Test list_dir function."""
    
    def test_list_dir_success(self):
        """Test successful directory listing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create test files and directories
            Path(tmp_dir, "file1.txt").touch()
            Path(tmp_dir, "file2.txt").touch()
            Path(tmp_dir, "subdir").mkdir()
            Path(tmp_dir, "subdir", "file3.txt").touch()
            
            result = list_dir(tmp_dir)
            assert "Contents of" in result
            assert "file1.txt" in result
            assert "file2.txt" in result
            assert "subdir/" in result
            assert "file3.txt" in result
    
    def test_list_dir_single_file(self):
        """Test listing single file."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            result = list_dir(tmp_path)
            assert os.path.basename(tmp_path) in result
        finally:
            os.unlink(tmp_path)
    
    def test_list_dir_hidden_files_ignored(self):
        """Test that hidden files are ignored."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create normal and hidden files
            Path(tmp_dir, "normal.txt").touch()
            Path(tmp_dir, ".hidden.txt").touch()
            Path(tmp_dir, ".hidden_dir").mkdir()
            
            result = list_dir(tmp_dir)
            assert "normal.txt" in result
            assert ".hidden.txt" not in result
            assert ".hidden_dir" not in result
    
    def test_list_dir_not_found(self):
        """Test listing non-existent directory."""
        result = list_dir("nonexistent_directory")
        assert "Error: Path" in result
        assert "does not exist" in result


class TestDeleteFile:
    """Test delete_file function."""
    
    def test_delete_file_temp_file(self):
        """Test deleting temporary file (allowed)."""
        with tempfile.NamedTemporaryFile(suffix='.tmp', delete=False) as tmp:
            tmp_path = tmp.name
        
        result = delete_file(tmp_path)
        assert "deleted successfully" in result
        assert not os.path.exists(tmp_path)
    
    def test_delete_file_meaningful_extension(self):
        """Test deleting file with meaningful extension (should be blocked)."""
        with tempfile.NamedTemporaryFile(suffix='.py', delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            result = delete_file(tmp_path)
            assert "Cannot delete file" in result
            assert "meaningful extension" in result
            assert os.path.exists(tmp_path)  # File should still exist
        finally:
            os.unlink(tmp_path)
    
    def test_delete_file_not_found(self):
        """Test deleting non-existent file."""
        result = delete_file("nonexistent_file.txt")
        assert "Error: File" in result
        assert "not found" in result
    
    def test_delete_file_directory(self):
        """Test attempting to delete directory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = delete_file(tmp_dir)
            assert "Error: Path" in result
            assert "is a directory" in result
    
    def test_delete_file_meaningful_extensions(self):
        """Test various meaningful extensions that should be blocked."""
        meaningful_exts = ['.py', '.png', '.jpg', '.pdf', '.md', '.txt', '.csv', '.json']
        
        for ext in meaningful_exts:
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                tmp_path = tmp.name
            
            try:
                result = delete_file(tmp_path)
                assert "Cannot delete file" in result
                assert f"meaningful extension '{ext}'" in result
                assert os.path.exists(tmp_path)
            finally:
                os.unlink(tmp_path) 