from __future__ import annotations

import os
from pathlib import Path


def read_file(path: str, max_length: int = 20_000) -> str:
    """
    Read the content of a file.

    Parameters
    ----------
    path : str
        The path to the file.
    max_length : int, optional
        The maximum length of the file content to read. Default is 20,000 characters.

    Returns
    -------
    str
        The content of the file or an error message.
    """
    try:
        with open(path, "r") as f:
            return f.read(max_length)
    except Exception as e:
        return f"Error reading file: {e}"


def write_file(path: str, content: str) -> str:
    """
    Write content to a file. This will create the file if it doesn't exist,
    and overwrite it if it does.

    Parameters
    ----------
    path : str
        The path to the file.
    content : str
        The content to write to the file.

    Returns
    -------
    str
        A success or error message.
    """
    try:
        with open(path, "w") as f:
            f.write(content)
        return f"File '{path}' written successfully."
    except Exception as e:
        return f"Error writing to file: {e}"


def update_file(path: str, find: str, replace: str) -> str:
    """
    Find and replace content in a file.

    Parameters
    ----------
    path : str
        The path to the file.
    find : str
        The content to find.
    replace : str
        The content to replace with.

    Returns
    -------
    str
        A success or error message.
    """
    try:
        with open(path, "r") as f:
            content = f.read()
        new_content = content.replace(find, replace)
        with open(path, "w") as f:
            f.write(new_content)
        return f"File '{path}' updated successfully."
    except Exception as e:
        return f"Error updating file: {e}"


def list_dir(path: str, max_length: int = 20_000) -> str:
    """
    List the contents of a directory recursively, ignoring hidden files/folders.

    Parameters
    ----------
    path : str
        The path to the directory or file.
    max_length : int, optional
        The maximum length of the directory listing to return. Default is 20,000 characters.

    Returns
    -------
    str
        A structured string of the directory contents or the file name, truncated if necessary.
    """
    try:
        path_obj = Path(path)
        if not path_obj.exists():
            return f"Error: Path '{path}' does not exist."

        if path_obj.is_file():
            return "" if path_obj.name.startswith(".") else path_obj.name

        if path_obj.is_dir():
            output = f"Contents of '{path}':\n"
            for item in sorted(path_obj.rglob("*")):
                # This check ensures that we ignore files/directories that are hidden
                # or are inside a hidden directory.
                if any(part.startswith('.') for part in item.relative_to(path_obj).parts):
                    continue

                depth = len(item.relative_to(path_obj).parts) - 1
                indent = "    " * depth
                if item.is_dir():
                    output += f"{indent}└── {item.name}/\n"
                else:
                    output += f"{indent}├── {item.name}\n"
                
                # Check if we're approaching the max_length limit
                if len(output) > max_length:
                    output = output[:max_length] + f"\n\n[LISTING TRUNCATED - Directory has more files than can be displayed]"
                    break
            
            return output
        return f"Error: Path '{path}' is not a file or directory."
    except Exception as e:
        return f"Error listing directory: {e}"


def delete_file(path: str) -> str:
    """
    Delete a file.

    Parameters
    ----------
    path : str
        The path to the file to delete.

    Returns
    -------
    str
        A success or error message.
    """
    try:
        path_obj = Path(path)
        if not path_obj.exists():
            return f"Error: File '{path}' not found."
        if not path_obj.is_file():
            return f"Error: Path '{path}' is a directory, not a file."

        # Check if file has meaningful extensions that should not be deleted
        meaningful_extensions = {'.py', '.png', '.svg', '.jpg', '.jpeg', '.pdf', '.md', '.txt', '.csv', '.json', '.h5ad', '.h5', '.hdf5', '.xlsx', '.xls', '.html', '.css', '.js', '.ipynb'}
        if path_obj.suffix.lower() in meaningful_extensions:
            return f"Error: Cannot delete file '{path}' with meaningful extension '{path_obj.suffix}'. This file may contain important data or code."

        os.remove(path_obj)
        return f"File '{path}' deleted successfully."
    except Exception as e:
        return f"Error deleting file: {e}"