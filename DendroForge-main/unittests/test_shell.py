import os
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.tools.shell import shell_command


class TestShellCommand:
    """Test shell_command function."""
    
    def test_shell_command_success(self):
        """Test successful shell command execution."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = shell_command("echo 'Hello World'", tmp_dir)
            assert "Hello World" in result
            assert "STDOUT:" in result
    
    def test_shell_command_with_stderr(self):
        """Test shell command that produces stderr."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = shell_command("echo 'Error message' >&2", tmp_dir)
            assert "Error message" in result
            assert "STDERR:" in result
    
    def test_shell_command_with_both_stdout_stderr(self):
        """Test shell command that produces both stdout and stderr."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = shell_command("echo 'Output'; echo 'Error' >&2", tmp_dir)
            assert "Output" in result
            assert "Error" in result
            assert "STDOUT:" in result
            assert "STDERR:" in result
    
    def test_shell_command_nonzero_exit_code(self):
        """Test shell command with non-zero exit code."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = shell_command("exit 1", tmp_dir)
            assert "Return code: 1" in result
    
    def test_shell_command_working_directory(self):
        """Test that command runs in specified directory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a test file in the directory
            test_file = os.path.join(tmp_dir, "test.txt")
            with open(test_file, "w") as f:
                f.write("test content")
            
            result = shell_command("ls", tmp_dir)
            assert "test.txt" in result
    
    def test_shell_command_invalid_sandbox_dir(self):
        """Test shell command with invalid sandbox directory."""
        result = shell_command("echo 'test'", "/nonexistent/directory")
        assert "Error: Sandbox directory" in result
        assert "does not exist" in result
    
    def test_shell_command_sandbox_dir_not_directory(self):
        """Test shell command with sandbox path that is not a directory."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file_path = tmp_file.name
        
        try:
            result = shell_command("echo 'test'", tmp_file_path)
            assert "Error: Sandbox directory" in result
            assert "not a directory" in result
        finally:
            os.unlink(tmp_file_path)
    
    def test_shell_command_timeout(self):
        """Test shell command timeout handling."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch('src.tools.shell.subprocess.run') as mock_run:
                mock_run.side_effect = subprocess.TimeoutExpired(
                    cmd="sleep 10", timeout=300
                )
                
                result = shell_command("sleep 10", tmp_dir)
                assert "timed out after 300 seconds" in result
    
    def test_shell_command_no_output(self):
        """Test shell command with no output."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = shell_command("true", tmp_dir)
            assert "Command executed successfully with no output" in result
    
    def test_shell_command_complex_command(self):
        """Test complex shell command with pipes and redirection."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a test file
            test_file = os.path.join(tmp_dir, "numbers.txt")
            with open(test_file, "w") as f:
                f.write("3\n1\n2\n")
            
            result = shell_command("sort numbers.txt", tmp_dir)
            assert "1\n2\n3" in result.replace("STDOUT:\n", "")
    
    def test_shell_command_file_operations(self):
        """Test shell command that creates and manipulates files."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Test creating a file
            result = shell_command("echo 'test content' > output.txt", tmp_dir)
            
            # Verify file was created
            output_file = os.path.join(tmp_dir, "output.txt")
            assert os.path.exists(output_file)
            
            # Test reading the file
            result = shell_command("cat output.txt", tmp_dir)
            assert "test content" in result
    
    def test_shell_command_environment_variables(self):
        """Test shell command with environment variables."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = shell_command("echo $HOME", tmp_dir)
            assert "STDOUT:" in result
            # HOME should be set to some value
            assert result.strip() != "STDOUT:"
    
    def test_shell_command_special_characters(self):
        """Test shell command with special characters."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = shell_command("echo 'Special chars: !@#$%^&*()'", tmp_dir)
            assert "Special chars: !@#$%^&*()" in result
    
    def test_shell_command_multiline_output(self):
        """Test shell command with multiline output."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = shell_command("echo -e 'Line 1\\nLine 2\\nLine 3'", tmp_dir)
            assert "Line 1" in result
            assert "Line 2" in result
            assert "Line 3" in result
    
    def test_shell_command_exception_handling(self):
        """Test that unexpected exceptions are handled gracefully."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch('src.tools.shell.subprocess.run') as mock_run:
                mock_run.side_effect = Exception("Unexpected error")
                
                result = shell_command("echo 'test'", tmp_dir)
                assert "An unexpected error occurred:" in result
                assert "Unexpected error" in result


# Import subprocess for the timeout test
import subprocess 