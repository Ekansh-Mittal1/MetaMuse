#!/usr/bin/env python3
"""
Integration tests for workflow compatibility after tool architecture refactoring.

This module tests that:
1. All existing workflows in main.py continue to function
2. The refactored tools integrate properly with the workflow system
3. No functionality has been lost during the refactoring
"""

import pytest
import tempfile
import os
import json
from pathlib import Path
from unittest.mock import patch, Mock

# Import workflow functions
from src.workflows.MetaMuse import (
    create_extraction_pipeline,
    create_multi_agent_pipeline,
    create_linking_pipeline,
    create_full_pipeline,
    create_hybrid_pipeline,
    create_enhanced_hybrid_pipeline,
    create_enhanced_full_pipeline,
    create_curation_pipeline,
)

# Import tool functions
from src.agents.tool_utils import get_session_tools, get_curator_tools, get_normalizer_tools


class TestWorkflowCompatibility:
    """Test that workflows continue to work with refactored tools."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.session_dir = os.path.join(self.temp_dir, "test_session")
        os.makedirs(self.session_dir, exist_ok=True)

    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_tool_creation_for_workflows(self):
        """Test that tools can be created for different workflow types."""
        # Test session tools creation
        session_tools = get_session_tools(self.session_dir)
        assert isinstance(session_tools, list)
        assert len(session_tools) > 0
        
        # Test curator tools creation
        curator_tools = get_curator_tools(self.session_dir)
        assert isinstance(curator_tools, list)
        assert len(curator_tools) > 0
        
        # Test normalizer tools creation
        normalizer_tools = get_normalizer_tools(self.session_dir)
        assert isinstance(normalizer_tools, list)
        assert len(normalizer_tools) > 0

    def test_pipeline_creation_functions_exist(self):
        """Test that all pipeline creation functions are available."""
        # These should all be callable functions
        assert callable(create_extraction_pipeline)
        assert callable(create_multi_agent_pipeline)
        assert callable(create_linking_pipeline)
        assert callable(create_full_pipeline)
        assert callable(create_hybrid_pipeline)
        assert callable(create_enhanced_hybrid_pipeline)
        assert callable(create_enhanced_full_pipeline)
        assert callable(create_curation_pipeline)

    @patch('src.tools.ingestion_tools.extract_gsm_metadata_impl')
    @patch('src.tools.ingestion_tools.extract_gse_metadata_impl')
    def test_extraction_pipeline_tool_integration(self, mock_gse, mock_gsm):
        """Test that extraction pipeline can use refactored tools."""
        mock_gsm.return_value = "gsm_result"
        mock_gse.return_value = "gse_result"
        
        # Get tools that would be used in extraction pipeline
        tools = get_session_tools(self.session_dir)
        
        # Find specific tools
        gsm_tool = None
        gse_tool = None
        for tool in tools:
            if tool.__name__ == 'extract_gsm_metadata':
                gsm_tool = tool
            elif tool.__name__ == 'extract_gse_metadata':
                gse_tool = tool
        
        assert gsm_tool is not None
        assert gse_tool is not None
        
        # Test that tools can be called
        result1 = gsm_tool("GSM123456")
        result2 = gse_tool("GSE123456")
        
        assert result1 == "gsm_result"
        assert result2 == "gse_result"
        
        # Verify delegation occurred
        mock_gsm.assert_called_once()
        mock_gse.assert_called_once()

    @patch('src.tools.curator_tools.get_data_intake_context_impl')
    @patch('src.tools.curator_tools.save_curation_results_impl')
    def test_curation_pipeline_tool_integration(self, mock_save, mock_context):
        """Test that curation pipeline can use refactored tools."""
        mock_context.return_value = {"success": True, "data": "test_data"}
        mock_save.return_value = {"success": True, "saved": True}
        
        # Get curator tools
        tools = get_curator_tools(self.session_dir)
        
        # Find specific tools
        context_tool = None
        save_tool = None
        for tool in tools:
            if tool.__name__ == 'get_data_intake_context':
                context_tool = tool
            elif tool.__name__ == 'save_curation_results':
                save_tool = tool
        
        assert context_tool is not None
        assert save_tool is not None
        
        # Test that tools can be called
        result1 = context_tool()
        result2 = save_tool('{"test": "data"}')
        
        # Verify results are JSON
        parsed1 = json.loads(result1)
        parsed2 = json.loads(result2)
        
        assert parsed1["success"] is True
        assert parsed2["success"] is True
        
        # Verify delegation occurred
        mock_context.assert_called_once()
        mock_save.assert_called_once()

    @patch('src.tools.normalizer_tools.find_candidates_files_impl')
    @patch('src.tools.normalizer_tools.batch_normalize_session_impl')
    def test_normalization_pipeline_tool_integration(self, mock_batch, mock_find):
        """Test that normalization pipeline can use refactored tools."""
        mock_find.return_value = {"success": True, "candidates_files": []}
        mock_batch.return_value = {"success": True, "normalized": True}
        
        # Get normalizer tools
        tools = get_normalizer_tools(self.session_dir)
        
        # Find specific tools
        find_tool = None
        batch_tool = None
        for tool in tools:
            if tool.__name__ == 'find_candidates_files':
                find_tool = tool
            elif tool.__name__ == 'batch_normalize_session':
                batch_tool = tool
        
        assert find_tool is not None
        assert batch_tool is not None
        
        # Test that tools can be called
        result1 = find_tool()
        result2 = batch_tool("Disease")
        
        # Verify results are JSON
        parsed1 = json.loads(result1)
        parsed2 = json.loads(result2)
        
        assert parsed1["success"] is True
        assert parsed2["success"] is True
        
        # Verify delegation occurred
        mock_find.assert_called_once()
        mock_batch.assert_called_once()

    def test_tool_function_names_unchanged(self):
        """Test that tool function names haven't changed (backward compatibility)."""
        session_tools = get_session_tools(self.session_dir)
        curator_tools = get_curator_tools(self.session_dir)
        normalizer_tools = get_normalizer_tools(self.session_dir)
        
        # Expected tool names for session tools
        expected_session_tools = {
            'extract_gsm_metadata',
            'extract_gse_metadata',
            'extract_paper_abstract',
            'validate_geo_inputs',
            'create_series_sample_mapping',
            'load_mapping_file',
            'find_sample_directory',
            'clean_metadata_files',
            'package_linked_data',
            'create_curation_data_package',
            'process_multiple_samples',
            'save_curation_results',
            'load_curation_data_for_samples',
            'set_testing_session',
            'serialize_agent_output'
        }
        
        session_tool_names = {tool.__name__ for tool in session_tools}
        assert expected_session_tools.issubset(session_tool_names)
        
        # Expected tool names for curator tools
        expected_curator_tools = {
            'load_mapping_file',
            'find_sample_directory',
            'clean_metadata_files',
            'save_curation_results',
            'get_data_intake_context'
        }
        
        curator_tool_names = {tool.__name__ for tool in curator_tools}
        assert expected_curator_tools.issubset(curator_tool_names)
        
        # Expected tool names for normalizer tools
        expected_normalizer_tools = {
            'find_candidates_files',
            'normalize_candidates_file',
            'batch_normalize_session',
            'get_available_ontologies'
        }
        
        normalizer_tool_names = {tool.__name__ for tool in normalizer_tools}
        assert expected_normalizer_tools.issubset(normalizer_tool_names)

    def test_tool_parameter_compatibility(self):
        """Test that tools accept the expected parameters."""
        import inspect
        
        session_tools = get_session_tools(self.session_dir)
        
        # Test extract_gsm_metadata parameters
        gsm_tool = None
        for tool in session_tools:
            if tool.__name__ == 'extract_gsm_metadata':
                gsm_tool = tool
                break
        
        assert gsm_tool is not None
        sig = inspect.signature(gsm_tool)
        params = list(sig.parameters.keys())
        
        # Should accept gsm_id, email, api_key
        assert 'gsm_id' in params
        # email and api_key should be optional
        assert sig.parameters.get('email') is not None
        assert sig.parameters.get('api_key') is not None

    def test_tool_return_format_consistency(self):
        """Test that tools return data in consistent formats."""
        curator_tools = get_curator_tools(self.session_dir)
        
        # Test serialize_agent_output
        serialize_tool = None
        for tool in curator_tools:
            if tool.__name__ == 'serialize_agent_output':
                serialize_tool = tool
                break
        
        assert serialize_tool is not None
        
        # Should return JSON string
        result = serialize_tool("json")
        try:
            parsed = json.loads(result)
            assert isinstance(parsed, dict)
            assert "success" in parsed
        except json.JSONDecodeError:
            pytest.fail("Tool should return valid JSON")

    def test_error_handling_in_workflow_context(self):
        """Test that tools handle errors gracefully in workflow contexts."""
        normalizer_tools = get_normalizer_tools(self.session_dir)
        
        # Test find_candidates_files with empty directory
        find_tool = None
        for tool in normalizer_tools:
            if tool.__name__ == 'find_candidates_files':
                find_tool = tool
                break
        
        assert find_tool is not None
        
        # Should handle no files gracefully
        result = find_tool()
        parsed = json.loads(result)
        
        # Should have proper error structure
        assert "success" in parsed
        if not parsed["success"]:
            assert "message" in parsed

    @patch.dict(os.environ, {'NCBI_EMAIL': 'test@example.com'})
    def test_environment_variable_handling(self):
        """Test that tools properly handle environment variables."""
        session_tools = get_session_tools(self.session_dir)
        
        # Should not raise errors even with missing API key
        assert len(session_tools) > 0
        
        # Tools should be created successfully
        for tool in session_tools:
            assert callable(tool)

    def test_session_directory_binding(self):
        """Test that tools are properly bound to session directories."""
        # Create tools for different session directories
        session1_dir = os.path.join(self.temp_dir, "session1")
        session2_dir = os.path.join(self.temp_dir, "session2")
        os.makedirs(session1_dir, exist_ok=True)
        os.makedirs(session2_dir, exist_ok=True)
        
        tools1 = get_session_tools(session1_dir)
        tools2 = get_session_tools(session2_dir)
        
        # Both should create tools successfully
        assert len(tools1) > 0
        assert len(tools2) > 0
        
        # Tools should be different instances (bound to different sessions)
        assert tools1 is not tools2

    def test_workflow_integration_mock_success(self):
        """Test successful workflow integration with mocked dependencies."""
        with patch('src.tools.ingestion_tools.extract_gsm_metadata_impl') as mock_impl:
            mock_impl.return_value = "success"
            
            tools = get_session_tools(self.session_dir)
            
            # Should be able to create and use tools
            assert len(tools) > 0
            
            # Find and test a specific tool
            gsm_tool = None
            for tool in tools:
                if tool.__name__ == 'extract_gsm_metadata':
                    gsm_tool = tool
                    break
            
            if gsm_tool:
                result = gsm_tool("GSM123456")
                assert result == "success"


class TestBackwardCompatibility:
    """Test backward compatibility with existing code."""

    def test_import_compatibility(self):
        """Test that all expected imports still work."""
        # These imports should not raise exceptions
        from src.agents.tool_utils import get_session_tools
        from src.agents.tool_utils import get_curator_tools
        from src.agents.tool_utils import get_normalizer_tools
        
        assert callable(get_session_tools)
        assert callable(get_curator_tools)
        assert callable(get_normalizer_tools)

    def test_function_signature_compatibility(self):
        """Test that function signatures remain compatible."""
        import inspect
        
        # get_session_tools should accept session_dir
        sig = inspect.signature(get_session_tools)
        params = list(sig.parameters.keys())
        assert 'session_dir' in params
        
        # get_curator_tools should accept session_dir
        sig = inspect.signature(get_curator_tools)
        params = list(sig.parameters.keys())
        assert 'session_dir' in params
        
        # get_normalizer_tools should accept session_dir
        sig = inspect.signature(get_normalizer_tools)
        params = list(sig.parameters.keys())
        assert 'session_dir' in params


if __name__ == "__main__":
    pytest.main([__file__]) 