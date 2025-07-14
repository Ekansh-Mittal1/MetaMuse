from typing import Callable
from agents import RunResultStreaming, Runner, RunConfig, ModelSettings, FunctionTool
from openai.types.shared import Reasoning
from pathlib import Path
import os
from dotenv import load_dotenv
from agents import ItemHelpers
import json

# Load environment variables
load_dotenv(override=True)


class SimpleOrchestrator:
    """
    Simple orchestrator for running GEO metadata extraction workflows.
    
    This minimal orchestrator manages session directories and executes
    workflow functions that build agent chains for metadata extraction.
    """
    
    def __init__(self, session_id: str, model_provider=None, provider_max_tokens=None, sandbox_dir: str = None):
        """
        Initialize the orchestrator.
        
        Parameters
        ----------
        session_id : str
            The unique session identifier
        model_provider : optional
            Custom model provider to use for agent runs
        provider_max_tokens : optional
            Max tokens for the model provider
        sandbox_dir : str, optional
            Base sandbox directory. If not provided, uses SANDBOX_DIR environment variable or defaults to "sandbox"
        """
        self.session_id = session_id
        self.model_provider = model_provider
        self.provider_max_tokens = provider_max_tokens
        
        # Determine sandbox directory
        if sandbox_dir is None:
            sandbox_dir = os.getenv("SANDBOX_DIR", "sandbox")
        
        self.sandbox_dir = sandbox_dir
        self.session_dir = Path(sandbox_dir) / session_id
        self.session_dir.mkdir(parents=True, exist_ok=True)
    
    async def run_workflow(
        self, 
        workflow_func: Callable,
        input_data: str, 
        **kwargs
    ) -> RunResultStreaming:
        """
        Execute a workflow function with the given input data.
        
        Parameters
        ----------
        workflow_func : Callable
            The workflow function to execute (e.g., create_geo_extraction_pipeline)
        input_data : str
            The input data/request to process
        **kwargs
            Additional keyword arguments to pass to the workflow function
            
        Returns
        -------
        RunResultStreaming
            The result of the workflow execution
        """
        # Create the workflow (agent chain) using the provided function
        print(f"🔧 Creating workflow with session_id: {self.session_id}")
        workflow = workflow_func(
            session_id=self.session_id,
            sandbox_dir=self.sandbox_dir,
            **kwargs
        )
        print(f"✅ Workflow created: {workflow.name}")
        print(f"   Tools: {len(workflow.tools)}")
        
        # Configure run settings
        run_config = RunConfig()
        
        if self.model_provider:
            run_config.model_provider = self.model_provider
            
        if self.provider_max_tokens:
            run_config.model_settings = ModelSettings(max_tokens=self.provider_max_tokens)
        
        # Execute the workflow
        print(f"🔄 Executing workflow with input: {input_data}")
        result = await Runner.run(
            workflow,
            input_data,
            run_config=run_config
        )
        print(f"✅ Workflow execution completed")
        
        return result
    
    def get_session_files(self) -> list[str]:
        """
        Get a list of files created in the session directory.
        
        Returns
        -------
        list[str]
            List of file paths in the session directory
        """
        if not self.session_dir.exists():
            return []
        
        return [str(f) for f in self.session_dir.iterdir() if f.is_file()]
    
    def get_session_metadata(self) -> dict:
        """
        Get metadata about the session.
        
        Returns
        -------
        dict
            Session metadata including directory, files, and statistics
        """
        files = self.get_session_files()
        
        return {
            "session_id": self.session_id,
            "session_dir": str(self.session_dir),
            "files_created": len(files),
            "file_list": files,
            "sandbox_dir": self.sandbox_dir
        } 