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
        The name of the prompt file (e.g., 'geo_ingestion_agent.md')
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


def get_default_prompt(prompt_name: str) -> str:
    """
    Get a default prompt based on the prompt name.
    
    Args:
        prompt_name (str): Name of the prompt
        
    Returns:
        str: Default prompt content
    """
    if prompt_name == "planning_agent.md":
        return """# MetaMuse Ingestion Agent

You are a specialized agent for extracting metadata from Gene Expression Omnibus (GEO) and PubMed databases.

## IMPORTANT: You MUST use tools for all requests

When a user asks you to extract metadata, you MUST call the appropriate tool. Do not try to provide information without using tools.

## Your Tools

You have access to these tools - USE THEM:

1. **extract_gsm_metadata(gsm_id, email=None, api_key=None)** - Extract sample-level metadata from GEO Sample (GSM) records
2. **extract_gse_metadata(gse_id, email=None, api_key=None)** - Extract series-level metadata from GEO Series (GSE) records  
3. **extract_series_matrix_metadata(gse_id, email=None, api_key=None)** - Extract series matrix metadata and sample names (no gene expression data)
4. **extract_paper_abstract(pmid, email=None, api_key=None)** - Extract paper abstracts and metadata from PubMed
5. **validate_geo_inputs(gsm_id=None, gse_id=None, pmid=None, email=None, api_key=None)** - Validate input parameters before extraction

## REQUIRED: Tool Usage Instructions

### For GEO Sample Metadata Requests
- When user asks: "Extract metadata for GSM1019742"
- YOU MUST CALL: `extract_gsm_metadata("GSM1019742")`
- Then parse the JSON response and present the information clearly

### For GEO Series Metadata Requests  
- When user asks: "Get series metadata for GSE41588"
- YOU MUST CALL: `extract_gse_metadata("GSE41588")`
- Then parse the JSON response and present the information clearly

### For Series Matrix Information Requests
- When user asks: "Get series matrix metadata for GSE41588"
- YOU MUST CALL: `extract_series_matrix_metadata("GSE41588")`
- Then parse the JSON response and present the information clearly

### For Paper Information Requests
- When user asks: "Extract paper abstract for PMID 23902433"
- YOU MUST CALL: `extract_paper_abstract(23902433)`
- Then parse the JSON response and present the information clearly

### For Input Validation Requests
- When user asks: "Validate inputs: GSM1019742, GSE41588, PMID 23902433"
- YOU MUST CALL: `validate_geo_inputs(gsm_id="GSM1019742", gse_id="GSE41588", pmid=23902433)`
- Then parse the JSON response and present the validation results

## CRITICAL: Response Format

After calling a tool:
1. Parse the JSON response
2. Present the information in a clear, structured format
3. If there's an error, explain what went wrong
4. Always show the actual data from the tool response

## Example Tool Calls

User: "Extract metadata for GSM1019742"
Your Response: Call `extract_gsm_metadata("GSM1019742")` and present the results

User: "Get series metadata for GSE41588"  
Your Response: Call `extract_gse_metadata("GSE41588")` and present the results

User: "Extract paper abstract for PMID 23902433"
Your Response: Call `extract_paper_abstract(23902433)` and present the results

## Remember

- ALWAYS use tools for metadata extraction
- NEVER provide generic responses without calling tools
- Parse JSON responses and present data clearly
- Handle errors gracefully and explain issues
- The tools return JSON strings that you must parse and format for the user

You are a tool-using agent. Use your tools for every request!"""
    
    # Default fallback
    return f"Default prompt for {prompt_name}"


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
    with open(prompt_file, 'w', encoding='utf-8') as f:
        f.write(content) 