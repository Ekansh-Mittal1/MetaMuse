from agents import handoff, RunContextWrapper
from pydantic import BaseModel
from src.agents.handoff_base import BaseHandoff

from src.agents import (
    create_planning_agent,
    create_data_discovery_agent,
    create_coding_planning_agent,
    create_report_agent,
    create_qa_agent,
    DataDiscoveryHandoff,
    CodingPlanningHandoff,
    ReportHandoff,
    QAHandoff
)

def on_handoff_callback(ctx: RunContextWrapper[None], input_data: BaseHandoff):
    """Print the original request and any additional handoff fields for debugging."""
    print(f"[Handoff] original_request: {input_data.original_request}")
    for field_name, value in input_data.dict(exclude={"original_request"}).items():
        print(f"[Handoff] {field_name}: {value}")


def create_report_pipeline(session_id: str, model_provider=None, sandbox_dir=None, **kwargs):
    """
    Build the standard bioinformatics analysis pipeline.    
    
    This workflow chains together all agents in the standard order:
    PlanningAgent -> DataDiscoveryAgent -> CodingPlanningAgent -> ReportAgent
    
    Parameters
    ----------
    session_id : str
        The unique session identifier
    model_provider : optional
        Custom model provider for the coding planning agent
    sandbox_dir : str, optional
        Base sandbox directory to pass to agent factories
    **kwargs : dict
        Additional arguments (ignored)
        
    Returns
    -------
    Agent
        The entry point agent (PlanningAgent)
    """
    # Create all agents independently
    report_agent = create_report_agent(session_id, sandbox_dir=sandbox_dir)
    
    coding_planning_agent = create_coding_planning_agent(
        session_id, 
        model_provider, 
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=report_agent,
                input_type=ReportHandoff,
                on_handoff=on_handoff_callback,
            )
        ]
    )
    
    data_discovery_agent = create_data_discovery_agent(
        session_id, 
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=coding_planning_agent,
                input_type=CodingPlanningHandoff,
                on_handoff=on_handoff_callback,
            )
        ]
    )
    
    planning_agent = create_planning_agent(
        session_id, 
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=data_discovery_agent,
                input_type=DataDiscoveryHandoff,
                on_handoff=on_handoff_callback,
            )
        ]
    )
    
    return planning_agent  # Return entry point


def create_qa_pipeline(session_id: str, model_provider=None, sandbox_dir=None, **kwargs):
    """
    Build the bioinformatics analysis pipeline with a focus on answering questions.
    
    This workflow chains together all agents in the standard order:
    PlanningAgent -> DataDiscoveryAgent -> CodingPlanningAgent -> QAAgent
    
    Parameters
    ----------
    session_id : str
        The unique session identifier
    model_provider : optional
        Custom model provider for the coding planning agent
    sandbox_dir : str, optional
        Base sandbox directory to pass to agent factories
    **kwargs : dict
        Additional arguments (ignored)
        
    Returns
    -------
    Agent
        The entry point agent (PlanningAgent)
    """
    # Create all agents independently
    qa_agent = create_qa_agent(session_id, sandbox_dir=sandbox_dir)
    
    coding_planning_agent = create_coding_planning_agent(
        session_id, 
        model_provider, 
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=qa_agent,
                input_type=QAHandoff,
                on_handoff=on_handoff_callback,
            )
        ]
    )
    
    data_discovery_agent = create_data_discovery_agent(
        session_id, 
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=coding_planning_agent,
                input_type=CodingPlanningHandoff,
                on_handoff=on_handoff_callback,
            )
        ]
    )
    
    planning_agent = create_planning_agent(
        session_id, 
        sandbox_dir=sandbox_dir,
        handoffs=[
            handoff(
                agent=data_discovery_agent,
                input_type=DataDiscoveryHandoff,
                on_handoff=on_handoff_callback,
            )
        ]
    )
    
    return planning_agent  # Return entry point

