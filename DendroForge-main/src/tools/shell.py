import subprocess
import os
from pathlib import Path


def shell_command(command: str, sandbox_dir: str, max_length: int = 20_000) -> str:
    """Execute a shell command in a specific directory and return the output.

    Parameters
    ----------
    command : str
        The command to execute.
    sandbox_dir : str
        The directory to execute the command in.
    max_length : int, optional
        The maximum length of the command output to return. Default is 20,000 characters.

    Returns
    -------
    str
        The stdout and stderr of the command, truncated if necessary.
    """
    try:
        sandbox_path = Path(sandbox_dir).resolve()
        if not sandbox_path.exists() or not sandbox_path.is_dir():
            return f"Error: Sandbox directory {sandbox_path} does not exist or is not a directory."

        result = subprocess.run(
            command,
            shell=True,
            check=False,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
            cwd=str(sandbox_path),
        )
        output = ""
        if result.stdout:
            output += f"STDOUT:\n{result.stdout}\n"
        if result.stderr:
            output += f"STDERR:\n{result.stderr}\n"

        if result.returncode != 0:
            output += f"Return code: {result.returncode}\n"

        final_output = output if output else "Command executed successfully with no output."
        
        # Truncate output if it exceeds max_length
        if len(final_output) > max_length:
            final_output = final_output[:max_length] + f"\n\n[OUTPUT TRUNCATED - Original length: {len(final_output)} characters]"
        
        return final_output
    except subprocess.TimeoutExpired as e:
        return f"Command '{e.cmd}' timed out after {e.timeout} seconds."
    except Exception as e:
        return f"An unexpected error occurred: {e}"