"""
Deterministic workflow for metadata processing.

This module provides a deterministic workflow that runs agents in sequence
without handoffs, using Runner.run_streamed for each agent and serializing
data between steps. The workflow is: data_intake -> curator -> normalizer.
"""

import json
import asyncio
from pathlib import Path
from typing import Optional, Dict, Any
from uuid import uuid4
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from src.workflows.data_intake import run_data_intake_workflow
from src.agents.curator import run_curator_agent
from src.agents.normalizer import run_normalizer_agent
from src.models.agent_outputs import CuratorOutput
from src.models import (
    CuratorOutput,
)


async def run_deterministic_workflow(
    input_text: str,
    target_field: str = "Disease",
    session_id: str = None,
    sandbox_dir: str = "sandbox",
    ontologies: Optional[list[str]] = None,
    min_score: float = 0.5,
    model_provider=None,
    max_tokens: int = 65536,
    max_turns: int = 100,
) -> Dict[str, Any]:
    """
    Run the complete deterministic workflow: data_intake -> curator -> normalizer.

    This function executes the three-stage workflow where each agent runs independently
    and data is serialized between steps. The workflow is modular and can be easily
    extended with additional agents or control flows.

    Parameters
    ----------
    input_text : str
        Input text containing GEO IDs for processing
    target_field : str, optional
        The target metadata field to extract and normalize (e.g., 'Disease', 'Tissue', 'Age')
    session_id : str, optional
        The unique session identifier. If not provided, generates a new one.
    sandbox_dir : str, optional
        Base sandbox directory. Defaults to "sandbox"
    ontologies : Optional[list[str]], optional
        Specific ontologies to search during normalization
    min_score : float, optional
        Minimum similarity score threshold for ontology matches

    Returns
    -------
    Dict[str, Any]
        Complete workflow results containing outputs from all stages and metadata
    """
    try:
        # Generate session ID if not provided
        if session_id is None:
            session_id = f"det_workflow_{str(uuid4())}"

        print(f"🚀 Starting deterministic workflow with session: {session_id}")
        print(f"📋 Target field: {target_field}")
        print(
            f"📝 Input: {input_text[:100]}..."
            if len(input_text) > 100
            else f"📝 Input: {input_text}"
        )

        # Stage 1: Data Intake
        print("\n" + "=" * 60)
        print("🗂️  STAGE 1: DATA INTAKE")
        print("=" * 60)

        data_intake_output = run_data_intake_workflow(
            input_text=input_text,
            session_id=session_id,
            sandbox_dir=sandbox_dir,
            workflow_type="complete",
        )

        if not data_intake_output.success:
            raise RuntimeError(f"Data intake failed: {data_intake_output.message}")

        print("✅ Data intake completed successfully")
        print(f"📁 Session directory: {data_intake_output.session_directory}")
        print(
            f"🔢 Samples for curation: {len(data_intake_output.sample_ids_for_curation)}"
        )

        # Serialize data_intake_output using Pydantic's built-in JSON serialization
        session_dir = Path(data_intake_output.session_directory)

        data_intake_file = session_dir / "data_intake_output.json"
        with open(data_intake_file, "w", encoding="utf-8") as f:
            f.write(data_intake_output.model_dump_json(indent=2))
        print(f"💾 Serialized data intake output to: {data_intake_file}")

        # Stage 2: Curator Agent
        print("\n" + "=" * 60)
        print("🎯 STAGE 2: CURATOR AGENT")
        print("=" * 60)

        curator_output = await run_curator_agent(
            data_intake_output=data_intake_output,
            target_field=target_field,
            session_id=session_id,
            sandbox_dir=sandbox_dir,
            model_provider=model_provider,
            max_tokens=max_tokens,
            max_turns=max_turns,
        )

        print("✅ Curator completed successfully")
        print(f"🎯 Target field: {curator_output.target_field}")
        print(f"🔢 Samples processed: {curator_output.total_samples_processed}")
        print(
            f"🎯 Candidates extracted: {len(curator_output.curation_results) if curator_output.curation_results else 0}"
        )

        # Serialize curator_output using Pydantic's built-in JSON serialization
        curator_file = session_dir / "curator_output.json"
        with open(curator_file, "w", encoding="utf-8") as f:
            f.write(curator_output.model_dump_json(indent=2))
        print(f"💾 Serialized curator output to: {curator_file}")

        # Stage 3: Normalizer Agent
        print("\n" + "=" * 60)
        print("🔬 STAGE 3: NORMALIZER AGENT")
        print("=" * 60)

        normalizer_output = await run_normalizer_agent(
            curator_output=curator_output,
            target_field=target_field,
            session_id=session_id,
            sandbox_dir=sandbox_dir,
            ontologies=ontologies,
            min_score=min_score,
            model_provider=model_provider,
            max_tokens=max_tokens,
            max_turns=max_turns,
        )

        print("✅ Normalizer completed successfully")
        print(f"🔬 Normalization results: {len(normalizer_output.sample_results)}")

        # Serialize normalizer_output using Pydantic's built-in JSON serialization
        normalizer_file = session_dir / "normalizer_output.json"
        with open(normalizer_file, "w", encoding="utf-8") as f:
            f.write(normalizer_output.model_dump_json(indent=2))
        print(f"💾 Serialized normalizer output to: {normalizer_file}")

        # Workflow Summary
        print("\n" + "=" * 60)
        print("🎉 WORKFLOW COMPLETED SUCCESSFULLY")
        print("=" * 60)

        # Create comprehensive workflow result
        workflow_result = {
            "success": True,
            "session_id": session_id,
            "session_directory": str(session_dir),
            "target_field": target_field,
            "input_text": input_text,
            "data_intake_output": data_intake_output.model_dump(),
            "curator_output": curator_output.model_dump(),
            "normalizer_output": normalizer_output.model_dump(),
            "files_created": [
                str(data_intake_file),
                str(curator_file),
                str(normalizer_file),
            ],
            "summary": {
                "samples_processed": len(data_intake_output.sample_ids_for_curation),
                "candidates_extracted": len(curator_output.curation_results)
                if curator_output.curation_results
                else 0,
                "normalization_results": len(normalizer_output.sample_results),
                "target_field": target_field,
            },
        }

        # Save workflow summary
        workflow_summary_file = session_dir / "workflow_summary.json"
        with open(workflow_summary_file, "w", encoding="utf-8") as f:
            json.dump(workflow_result, f, indent=2, ensure_ascii=False)
        print(f"📊 Workflow summary saved to: {workflow_summary_file}")

        return workflow_result

    except Exception as e:
        print(f"❌ Deterministic workflow failed: {str(e)}")
        import traceback

        print("🔍 Full traceback:")
        traceback.print_exc()

        # Return error result
        return {
            "success": False,
            "error": str(e),
            "session_id": session_id,
            "session_directory": str(Path(sandbox_dir) / session_id)
            if session_id
            else None,
            "target_field": target_field,
            "input_text": input_text,
        }


def run_deterministic_workflow_sync(
    input_text: str,
    target_field: str = "Disease",
    session_id: str = None,
    sandbox_dir: str = "sandbox",
    ontologies: Optional[list[str]] = None,
    min_score: float = 0.5,
    model_provider=None,
    max_tokens: int = 65536,
    max_turns: int = 100,
) -> Dict[str, Any]:
    """
    Synchronous wrapper for the deterministic workflow.

    This function provides a synchronous interface to the async deterministic workflow,
    useful for integration with existing synchronous code.

    Parameters are the same as run_deterministic_workflow.

    Returns
    -------
    Dict[str, Any]
        Complete workflow results containing outputs from all stages and metadata
    """
    import asyncio

    try:
        # Try to get existing event loop
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If loop is running, we need to use a different approach
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    asyncio.run,
                    run_deterministic_workflow(
                        input_text,
                        target_field,
                        session_id,
                        sandbox_dir,
                        ontologies,
                        min_score,
                        model_provider,
                        max_tokens,
                        max_turns,
                    ),
                )
                return future.result()
        else:
            # Loop exists but not running, can use it directly
            return loop.run_until_complete(
                run_deterministic_workflow(
                    input_text,
                    target_field,
                    session_id,
                    sandbox_dir,
                    ontologies,
                    min_score,
                    model_provider,
                    max_tokens,
                    max_turns,
                )
            )
    except RuntimeError:
        # No event loop exists, create new one
        return asyncio.run(
            run_deterministic_workflow(
                input_text,
                target_field,
                session_id,
                sandbox_dir,
                ontologies,
                min_score,
                model_provider,
                max_tokens,
                max_turns,
            )
        )


async def test_normalizer_agent(
    test_session_dir: str = "sandbox/test_session",
    model_provider=None,
    max_tokens: int = 65536,
    max_turns: int = 100,
) -> Dict[str, Any]:
    """
    Test the normalizer agent in isolation using curator output from a test session.

    This function loads curator output from a test session directory and passes it
    to the normalizer agent the same way the deterministic workflow would.

    Parameters
    ----------
    test_session_dir : str, optional
        Path to test session directory containing curator_output.json (default: "sandbox/test_session")
    model_provider : optional
        Model provider configuration
    max_tokens : int, optional
        Maximum tokens for model responses (default: 4096)
    max_turns : int, optional
        Maximum conversation turns per agent (default: 100)

    Returns
    -------
    Dict[str, Any]
        Normalizer agent results and metadata
    """
    print("🧪 TESTING NORMALIZER AGENT IN ISOLATION")
    print("=" * 60)

    try:
        # Load curator output from test session
        curator_output_path = Path(test_session_dir) / "curator_output.json"
        if not curator_output_path.exists():
            raise FileNotFoundError(f"Curator output not found: {curator_output_path}")

        print(f"📂 Loading curator output from: {curator_output_path}")

        with open(curator_output_path, "r") as f:
            curator_output_data = json.load(f)

        # Convert to CuratorOutput Pydantic model
        curator_output = CuratorOutput(**curator_output_data)

        print("✅ Loaded curator output:")
        print(f"   - Target Field: {curator_output.target_field}")
        print(f"   - Samples: {curator_output.sample_ids_requested}")
        print(f"   - Total Samples Processed: {curator_output.total_samples_processed}")
        print(f"   - Curation Results: {len(curator_output.curation_results)}")

        # Extract sample IDs and target field from curator output
        sample_ids = list(curator_output.sample_ids_requested)
        target_field = curator_output.target_field

        # Override the session directory to use test_session
        print(
            f"🔄 Overriding session directory from '{curator_output.session_directory}' to '{test_session_dir}'"
        )
        curator_output.session_directory = test_session_dir

        print("\n🔬 STAGE: NORMALIZER AGENT (ISOLATED TEST)")
        print("=" * 60)

        # Run the normalizer agent with the same parameters as deterministic workflow
        normalizer_output = await run_normalizer_agent(
            curator_output=curator_output,
            target_field=target_field,
            session_id=None,  # Will use existing session from curator output
            sandbox_dir=None,
            ontologies=None,  # Let tool determine ontology automatically
            min_score=0.5,
            model_provider=model_provider,
            max_tokens=max_tokens,
            max_turns=max_turns,
        )

        print("✅ Normalizer completed successfully")
        print(f"🔬 Normalization results: {len(normalizer_output.sample_results)}")

        # Serialize normalizer output
        normalizer_output_file = Path(test_session_dir) / "test_normalizer_output.json"
        with open(normalizer_output_file, "w") as f:
            f.write(normalizer_output.model_dump_json(indent=2))

        print(f"💾 Normalizer output saved to: {normalizer_output_file}")

        # Create test result summary
        test_result = {
            "test_type": "normalizer_agent_isolation",
            "test_session_dir": test_session_dir,
            "curator_input": {
                "target_field": curator_output.target_field,
                "samples": list(curator_output.sample_ids_requested),
                "total_candidates": sum(
                    len(result.series_candidates)
                    + len(result.sample_candidates)
                    + len(result.abstract_candidates)
                    for result in curator_output.curation_results
                ),
            },
            "normalizer_output": {
                "sample_results_count": len(normalizer_output.sample_results),
                "total_candidates_normalized": normalizer_output.total_candidates_normalized,
                "successful_normalizations": normalizer_output.successful_normalizations,
                "target_field": normalizer_output.target_field,
                "session_directory": normalizer_output.session_directory,
            },
            "files_created": [
                str(normalizer_output_file.absolute()),
            ],
            "status": "success",
        }

        print("\n🎯 TEST SUMMARY:")
        print("=" * 60)
        print("✅ Test completed successfully")
        print(
            f"📊 Input candidates: {test_result['curator_input']['total_candidates']}"
        )
        print(
            f"🔬 Normalized results: {test_result['normalizer_output']['sample_results_count']}"
        )
        print(
            f"✨ Successful normalizations: {test_result['normalizer_output']['successful_normalizations']}"
        )
        print(f"📁 Output file: {normalizer_output_file}")

        return test_result

    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback

        print("🔍 Test traceback:")
        traceback.print_exc()

        return {
            "test_type": "normalizer_agent_isolation",
            "test_session_dir": test_session_dir,
            "status": "failed",
            "error": str(e),
        }


def test_normalizer_agent_sync(
    test_session_dir: str = "sandbox/test_session",
    model_provider=None,
    max_tokens: int = 65536,
    max_turns: int = 100,
) -> Dict[str, Any]:
    """
    Synchronous wrapper for testing the normalizer agent in isolation.

    Parameters
    ----------
    test_session_dir : str, optional
        Path to test session directory containing curator_output.json
    model_provider : optional
        Model provider configuration
    max_tokens : int, optional
        Maximum tokens for model responses (default: 4096)
    max_turns : int, optional
        Maximum conversation turns per agent (default: 100)

    Returns
    -------
    Dict[str, Any]
        Test results and metadata
    """
    return asyncio.run(
        test_normalizer_agent(
            test_session_dir=test_session_dir,
            model_provider=model_provider,
            max_tokens=max_tokens,
            max_turns=max_turns,
        )
    )
