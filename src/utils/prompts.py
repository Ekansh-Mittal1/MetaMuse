"""
Prompt utilities for MetaMuse agents.
"""

import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, Template
from typing import Dict, Any


def load_prompt(filename: str, **variables) -> str:
    """
    Load and render a prompt template from the prompts directory.

    Parameters
    ----------
    filename : str
        The name of the prompt file (e.g., 'ingestion_agent.md')
    **variables : dict
        Template variables to render in the prompt

    Returns
    -------
    str
        The rendered prompt with variables substituted
    """
    prompts_dir = Path("src/prompts")
    env = Environment(loader=FileSystemLoader(str(prompts_dir)))

    try:
        template = env.get_template(filename)

        # Load global preamble if it exists
        global_path = prompts_dir / "global.md"
        if global_path.exists():
            with open(global_path, "r") as f:
                global_preamble = f.read()

            # Check if the template content already includes global_preamble placeholder
            template_source = (
                template.source
                if hasattr(template, "source")
                else env.loader.get_source(env, filename)[0]
            )

            if "{{ global_preamble }}" not in template_source:
                # If not, add it to variables but don't inject it automatically
                variables.setdefault("global_preamble", global_preamble)
            else:
                # If the template explicitly includes the placeholder, provide the content
                variables["global_preamble"] = global_preamble
        else:
            variables.setdefault("global_preamble", "")

        return template.render(**variables)

    except Exception as e:
        raise RuntimeError(f"Error loading prompt {filename}: {e}")


def get_default_prompt(prompt_name: str) -> str:
    """
    Get a default prompt based on the prompt name.

    Args:
        prompt_name (str): Name of the prompt

    Returns:
        str: Default prompt content
    """
    # Try to load the prompt from the prompts directory
    try:
        return load_prompt(prompt_name)
    except Exception as e:
        # Fallback to a simple default if the file doesn't exist
        return f"Default prompt for {prompt_name} - Error loading: {e}"


def save_prompt(prompt_name: str, content: str) -> None:
    """
    Save a prompt to the prompts directory.

    Args:
        prompt_name (str): Name of the prompt file
        content (str): The prompt content to save
    """
    current_dir = Path(__file__).parent
    prompts_dir = current_dir / "prompts"
    prompts_dir.mkdir(exist_ok=True)

    prompt_file = prompts_dir / prompt_name
    with open(prompt_file, "w", encoding="utf-8") as f:
        f.write(content)
