from pathlib import Path
from jinja2 import Environment, FileSystemLoader, Template
from typing import Dict, Any


def load_prompt(filename: str, **variables) -> str:
    """
    Load and render a prompt template from the prompts directory.
    
    Parameters
    ----------
    filename : str
        The name of the prompt file (e.g., 'planning_agent.md')
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
            with open(global_path, 'r') as f:
                global_preamble = f.read()
            
            # Check if the template content already includes global_preamble placeholder
            template_source = template.source if hasattr(template, 'source') else env.loader.get_source(env, filename)[0]
            
            if '{{ global_preamble }}' not in template_source:
                # If not, add it to variables but don't inject it automatically
                variables.setdefault('global_preamble', global_preamble)
            else:
                # If the template explicitly includes the placeholder, provide the content
                variables['global_preamble'] = global_preamble
        else:
            variables.setdefault('global_preamble', "")
        
        return template.render(**variables)
        
    except Exception as e:
        raise RuntimeError(f"Error loading prompt {filename}: {e}") 