import shutil
from pathlib import Path


def move_file(source_path: str, destination_path: str, session_dir: str) -> str:
    """
    Move or copy a file to a destination relative to the session directory.

    If the source is outside the session directory, it will be copied.
    If the source is inside the session directory, it will be moved.

    Parameters
    ----------
    source_path : str
        The path to the source file.
    destination_path : str
        The destination path, relative to the session directory.
    session_dir : str
        The session directory.

    Returns
    -------
    str
        Success or error message.
    """
    try:
        session_path = Path(session_dir).resolve()
        source = Path(source_path).resolve()
        destination = (session_path / destination_path).resolve()

        # Security check: ensure destination is within the session directory
        if session_path not in destination.parents and destination != session_path:
            return f"Error: Destination '{destination_path}' is outside the session directory."

        destination.parent.mkdir(parents=True, exist_ok=True)

        if source.is_file():
            # If source is outside the session, copy it.
            # If it's inside, move it.
            try:
                source.relative_to(session_path)
                # It's inside, so move.
                shutil.move(str(source), str(destination))
                return f"Successfully moved '{source}' to '{destination}'."
            except ValueError:
                # It's outside, so copy.
                shutil.copy(str(source), str(destination))
                return f"Successfully copied '{source}' to '{destination}'."
        else:
            return f"Error: Source '{source_path}' is not a file or does not exist."

    except Exception as e:
        return f"An unexpected error occurred: {e}" 