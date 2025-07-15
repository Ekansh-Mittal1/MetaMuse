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
    Simple orchestrator for running agent workflows.
    
    This minimal orchestrator manages session directories and executes
    workflow functions that build agent chains.
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
        Run a workflow function.
        
        Parameters
        ----------
        workflow_func : Callable
            A function that builds and returns the entry point agent
        input_data : str
            The input data for the workflow
        **kwargs : dict
            Additional arguments passed to the workflow function
            
        Returns
        -------
        RunResultStreaming
            The result from Runner.run
        """
        # Add sandbox_dir to kwargs so workflow functions can pass it to agent factories
        kwargs['sandbox_dir'] = self.sandbox_dir
        
        # Build the agent chain using the workflow function
        # Only pass session_id if it's not already in kwargs (for existing session directories)
        if 'session_id' not in kwargs:
            kwargs['session_id'] = self.session_id
        entry_agent = workflow_func(**kwargs)
        
        # Prepare run config if model provider is specified
        run_config = None
        if self.model_provider:
            run_config = RunConfig(
                model_provider=self.model_provider,
                model_settings=ModelSettings(
                    max_tokens=self.provider_max_tokens,
                    reasoning=Reasoning(
                        effort="high",
                    )
                ),
            )
        
        # Run the workflow
        print(f"🔄 Executing workflow with input: {input_data}")
        result = await Runner.run(
            entry_agent,
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
        
        files = []
        for item in self.session_dir.rglob("*"):
            if item.is_file():
                files.append(str(item))
        
        return files
    
    def get_session_metadata(self) -> dict:
        """
        Get metadata about the session.
        
        Returns
        -------
        dict
            Session metadata including directory, files, and statistics
        """
        files = self.get_session_files()
        
        # Analyze directory structure
        series_dirs = []
        root_files = []
        
        if self.session_dir.exists():
            for item in self.session_dir.iterdir():
                if item.is_dir() and item.name.startswith("GSE"):
                    # This is a series directory
                    series_files = [f.name for f in item.iterdir() if f.is_file()]
                    series_dirs.append({
                        "series_id": item.name,
                        "path": str(item),
                        "files": series_files,
                        "file_count": len(series_files)
                    })
                elif item.is_file():
                    # This is a file in the root directory
                    root_files.append(item.name)
        
        return {
            "session_id": self.session_id,
            "session_dir": str(self.session_dir),
            "files_created": len(files),
            "file_list": files,
            "sandbox_dir": self.sandbox_dir,
            "series_directories": series_dirs,
            "root_files": root_files,
            "series_count": len(series_dirs),
            "root_file_count": len(root_files)
        } 