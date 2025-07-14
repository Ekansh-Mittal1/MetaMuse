# Unit Tests for DendroForge Tools

This directory contains comprehensive unit tests for all the tools defined in the `src/tools` directory.

## Test Structure

- `test_file_ops.py` - Tests for file operations (read_file, write_file, update_file, list_dir, delete_file)
- `test_move_file.py` - Tests for the move_file function
- `test_shell.py` - Tests for the shell_command function

## Running Tests

### Run All Tests
```bash
uv run pytest unittests/ -v
```

### Run Tests for a Specific Module
```bash
uv run pytest unittests/test_file_ops.py -v
uv run pytest unittests/test_move_file.py -v
uv run pytest unittests/test_shell.py -v
```

### Run a Specific Test Class
```bash
uv run pytest unittests/test_file_ops.py::TestReadFile -v
```

### Run a Specific Test Case
```bash
uv run pytest unittests/test_file_ops.py::TestReadFile::test_read_file_success -v
```

## Test Coverage

The tests cover the following scenarios:

### File Operations (`test_file_ops.py`)
- **read_file**: Success cases, file not found, permission errors
- **write_file**: Success cases, new file creation, invalid paths
- **update_file**: Success cases, no match scenarios, file not found
- **list_dir**: Directory listing, single files, hidden files (ignored), non-existent paths
- **delete_file**: Temp file deletion (allowed), meaningful extensions (blocked), error cases

### Move File (`test_move_file.py`)
- Moving files within session directory
- Copying files from outside session directory
- Security checks for destinations outside session
- Error handling for non-existent sources
- Parent directory creation
- File overwriting
- Exception handling

### Shell Command (`test_shell.py`)
- Successful command execution
- Commands with stdout/stderr output
- Non-zero exit codes
- Working directory verification
- Invalid sandbox directories
- Timeout handling
- Complex commands with pipes
- File operations through shell
- Environment variables
- Special characters
- Exception handling

## Dependencies

The tests require:
- `pytest` - Testing framework
- `tempfile` - For creating temporary files/directories in tests
- `unittest.mock` - For mocking in certain test scenarios

## Configuration

The tests are configured through `pytest.ini` in the project root:
- Test discovery in `unittests/` directory
- Verbose output enabled
- Short traceback format for cleaner output 