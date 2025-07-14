import os
import tempfile
import pytest
from pathlib import Path

from src.tools.move_file import move_file


class TestMoveFile:
    """Test move_file function."""
    
    def test_move_file_within_session(self):
        """Test moving file within session directory."""
        with tempfile.TemporaryDirectory() as session_dir:
            # Create source file within session
            source_path = os.path.join(session_dir, "source.txt")
            with open(source_path, "w") as f:
                f.write("test content")
            
            # Move to different location within session
            dest_path = "moved/file.txt"
            
            result = move_file(source_path, dest_path, session_dir)
            assert "Successfully moved" in result
            
            # Verify source no longer exists
            assert not os.path.exists(source_path)
            
            # Verify destination exists with correct content
            full_dest_path = os.path.join(session_dir, dest_path)
            assert os.path.exists(full_dest_path)
            with open(full_dest_path, "r") as f:
                assert f.read() == "test content"
    
    def test_copy_file_outside_session(self):
        """Test copying file from outside session directory."""
        with tempfile.TemporaryDirectory() as session_dir:
            # Create source file outside session
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
                tmp.write("external content")
                external_source = tmp.name
            
            try:
                dest_path = "copied/file.txt"
                
                result = move_file(external_source, dest_path, session_dir)
                assert "Successfully copied" in result
                
                # Verify source still exists (was copied, not moved)
                assert os.path.exists(external_source)
                
                # Verify destination exists with correct content
                full_dest_path = os.path.join(session_dir, dest_path)
                assert os.path.exists(full_dest_path)
                with open(full_dest_path, "r") as f:
                    assert f.read() == "external content"
            finally:
                os.unlink(external_source)
    
    def test_move_file_destination_outside_session(self):
        """Test attempting to move file to outside session directory (should fail)."""
        with tempfile.TemporaryDirectory() as session_dir:
            # Create source file within session
            source_path = os.path.join(session_dir, "source.txt")
            with open(source_path, "w") as f:
                f.write("test content")
            
            # Try to move to outside session
            dest_path = "../../outside/file.txt"
            
            result = move_file(source_path, dest_path, session_dir)
            assert "Error: Destination" in result
            assert "outside the session directory" in result
            
            # Verify source still exists
            assert os.path.exists(source_path)
    
    def test_move_file_nonexistent_source(self):
        """Test moving non-existent source file."""
        with tempfile.TemporaryDirectory() as session_dir:
            result = move_file("nonexistent.txt", "dest.txt", session_dir)
            assert "Error: Source" in result
            assert "not a file or does not exist" in result
    
    def test_move_file_directory_source(self):
        """Test attempting to move directory (should fail)."""
        with tempfile.TemporaryDirectory() as session_dir:
            # Create source directory
            source_dir = os.path.join(session_dir, "source_dir")
            os.makedirs(source_dir)
            
            result = move_file(source_dir, "dest_dir", session_dir)
            assert "Error: Source" in result
            assert "not a file or does not exist" in result
    
    def test_move_file_creates_parent_directories(self):
        """Test that parent directories are created if they don't exist."""
        with tempfile.TemporaryDirectory() as session_dir:
            # Create source file
            source_path = os.path.join(session_dir, "source.txt")
            with open(source_path, "w") as f:
                f.write("test content")
            
            # Move to nested destination
            dest_path = "nested/deep/path/file.txt"
            
            result = move_file(source_path, dest_path, session_dir)
            assert "Successfully moved" in result
            
            # Verify destination exists
            full_dest_path = os.path.join(session_dir, dest_path)
            assert os.path.exists(full_dest_path)
            with open(full_dest_path, "r") as f:
                assert f.read() == "test content"
    
    def test_move_file_overwrite_existing(self):
        """Test moving file to existing destination (should overwrite)."""
        with tempfile.TemporaryDirectory() as session_dir:
            # Create source file
            source_path = os.path.join(session_dir, "source.txt")
            with open(source_path, "w") as f:
                f.write("new content")
            
            # Create existing destination file
            dest_path = "existing.txt"
            full_dest_path = os.path.join(session_dir, dest_path)
            with open(full_dest_path, "w") as f:
                f.write("old content")
            
            result = move_file(source_path, dest_path, session_dir)
            assert "Successfully moved" in result
            
            # Verify destination has new content
            with open(full_dest_path, "r") as f:
                assert f.read() == "new content"
    
    def test_move_file_absolute_destination_path(self):
        """Test using absolute destination path within session (should work)."""
        with tempfile.TemporaryDirectory() as session_dir:
            # Create source file
            source_path = os.path.join(session_dir, "source.txt")
            with open(source_path, "w") as f:
                f.write("test content")
            
            # Use absolute destination path within session
            dest_path = os.path.join(session_dir, "absolute_dest.txt")
            
            result = move_file(source_path, dest_path, session_dir)
            assert "Successfully moved" in result
            
            # Verify destination exists
            assert os.path.exists(dest_path)
            with open(dest_path, "r") as f:
                assert f.read() == "test content"
    
    def test_move_file_exception_handling(self):
        """Test that unexpected exceptions are handled gracefully."""
        with tempfile.TemporaryDirectory() as session_dir:
            # Create source file
            source_path = os.path.join(session_dir, "source.txt")
            with open(source_path, "w") as f:
                f.write("test content")
            
            # Test with invalid session directory
            result = move_file(source_path, "dest.txt", "/invalid/session/dir")
            assert "An unexpected error occurred:" in result 