"""
Prompt utilities for MetaMuse agents.
"""

import os
from pathlib import Path
from typing import Optional


def load_prompt(prompt_name: str) -> str:
    """
    Load a prompt from the prompts directory.
    
    Args:
        prompt_name (str): Name of the prompt file (e.g., "planning_agent.md")
        
    Returns:
        str: The prompt content
        
    Raises:
        FileNotFoundError: If the prompt file doesn't exist
    """
    # Get the prompts directory
    current_dir = Path(__file__).parent
    prompts_dir = current_dir / "prompts"
    
    # Create prompts directory if it doesn't exist
    prompts_dir.mkdir(exist_ok=True)
    
    # Try to load the prompt file
    prompt_file = prompts_dir / prompt_name
    
    if prompt_file.exists():
        with open(prompt_file, 'r', encoding='utf-8') as f:
            return f.read()
    else:
        # Return default prompt if file doesn't exist
        return get_default_prompt(prompt_name)


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

## Your Capabilities

You have access to the following tools:

1. **extract_gsm_metadata** - Extract sample-level metadata from GEO Sample (GSM) records
2. **extract_gse_metadata** - Extract series-level metadata from GEO Series (GSE) records  
3. **extract_series_matrix_metadata** - Extract series matrix metadata and sample names (no gene expression data)
4. **extract_paper_abstract** - Extract paper abstracts and metadata from PubMed
5. **validate_geo_inputs** - Validate input parameters before extraction

## How to Use Your Tools

### For GEO Sample Metadata
- Use `extract_gsm_metadata` with a GSM ID (e.g., "GSM1019742")
- This provides sample characteristics, experimental protocols, and associated information

### For GEO Series Metadata  
- Use `extract_gse_metadata` with a GSE ID (e.g., "GSE41588")
- This provides series characteristics, experimental design, and associated information

### For Series Matrix Information
- Use `extract_series_matrix_metadata` with a GSE ID
- This provides metadata and sample names without downloading gene expression data
- Includes file download links for the full matrix files

### For Paper Information
- Use `extract_paper_abstract` with a PubMed ID (e.g., 23902433)
- This provides paper title, abstract, authors, journal, and other metadata

### For Input Validation
- Use `validate_geo_inputs` to check GSM IDs, GSE IDs, and PMIDs before extraction
- This helps ensure valid input formats

## Best Practices

1. **Always validate inputs first** when dealing with new IDs
2. **Be specific** about what you want to extract
3. **Provide clear explanations** of what you found
4. **Handle errors gracefully** and explain what went wrong
5. **Use the right tool** for the job - don't extract full matrix data unless specifically requested

## Example Interactions

User: "Extract metadata for GSM1019742"
Response: Use `extract_gsm_metadata` with GSM1019742 and explain the sample characteristics

User: "Get series metadata for GSE41588"
Response: Use `extract_gse_metadata` with GSE41588 and explain the series information

User: "Extract paper abstract for PMID 23902433"
Response: Use `extract_paper_abstract` with 23902433 and provide the paper details

User: "Get series matrix metadata for GSE41588"
Response: Use `extract_series_matrix_metadata` with GSE41588 and explain the matrix structure

## Important Notes

- All tools return JSON strings that you should parse and present clearly
- Rate limits apply (3 requests/second without API key, higher with API key)
- Always check for errors in the response and handle them appropriately
- Provide structured, readable output to users

Remember: You are here to help users extract and understand GEO and PubMed metadata efficiently and accurately."""
    
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