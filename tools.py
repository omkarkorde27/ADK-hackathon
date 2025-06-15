# Copyright 2025 Google LLC
# Licensed under the Apache License, Version 2.0

"""Tools for orchestrating the InsightSynergy Council debate process."""

from typing import Dict, Any, List
from google.adk.tools import ToolContext
from google.adk.tools.agent_tool import AgentTool

from .sub_agents import (
    db_agent,
    optimist_analyst_agent,
    pessimist_critic_agent, 
    ethical_auditor_agent,
    synthesis_moderator_agent
)
from .debate_engine.moderator import DebateModerator
from .debate_engine.consensus import BordaConsensusBuilder
from .utils.bias_detection import BiasDetector
from .utils.debate_logger import DebateLogger

async def call_db_agent(
    question: str,
    tool_context: ToolContext,
):
    """Tool to call database (nl2sql) agent."""
    print(
        "\n call_db_agent.use_database:"
        f' {tool_context.state["all_db_settings"]["use_database"]}'
    )

    agent_tool = AgentTool(agent=db_agent)

    db_agent_output = await agent_tool.run_async(
        args={"request": question}, tool_context=tool_context
    )
    tool_context.state["db_agent_output"] = db_agent_output
    return db_agent_output

async def initiate_council_debate(
    question: str,
    tool_context: ToolContext,
) -> Dict[str, Any]:
    """Initiate a structured debate among council agents."""
    
    debate_settings = tool_context.state["debate_settings"]
    moderator = DebateModerator(
        max_rounds=debate_settings["max_rounds"],
        conflict_threshold=debate_settings["conflict_threshold"]
    )
    
    # Initialize agents with their assigned models
    agents = {
        "data_detective": db_agent,
        "optimist_analyst": optimist_analyst_agent,
        "pessimist_critic": pessimist_critic_agent,
        "ethical_auditor": ethical_auditor_agent,
        "synthesis_moderator": synthesis_moderator_agent
    }
    
    debate_context = moderator.initiate_debate(question, agents)
    tool_context.state["current_debate"] = debate_context
    
    return {
        "status": "debate_initiated",
        "debate_id": debate_context["debate_id"],
        "question": question,
        "agents_count": len(agents)
    }

async def retrieve_data_insights(
    question: str,
    tool_context: ToolContext,
) -> Dict[str, Any]:
    """Use Data Detective to retrieve and analyze relevant data."""
    
    agent_tool = AgentTool(agent=db_agent)
    
    detective_output = await agent_tool.run_async(
        args={"request": question}, 
        tool_context=tool_context
    )
    
    tool_context.state["data_insights"] = detective_output
    
    return {
        "status": "data_retrieved",
        "insights": detective_output,
        "agent": "data_detective"
    }

async def calculate_bias_score(
    debate_arguments: List[Any],
    tool_context: ToolContext,
) -> Dict[str, Any]:
    """Calculate bias scores for the current debate."""
    
    bias_detector = BiasDetector(
        fairness_threshold=tool_context.state["debate_settings"]["fairness_threshold"]
    )
    
    bias_analysis = bias_detector.analyze_debate(debate_arguments)
    tool_context.state["bias_analysis"] = bias_analysis
    
    return {
        "bias_score": bias_analysis["overall_bias_score"],
        "fairness_violations": bias_analysis["violations"],
        "recommendations": bias_analysis["recommendations"]
    }

async def generate_consensus_report(
    tool_context: ToolContext,
) -> Dict[str, Any]:
    """Generate final consensus report from debate results."""
    
    if "current_debate" not in tool_context.state:
        return {"error": "No active debate found"}
    
    debate_context = tool_context.state["current_debate"]
    
    # Build consensus using Borda count voting
    consensus_builder = BordaConsensusBuilder()
    
    # Get all debate arguments from moderator
    moderator = DebateModerator()
    all_arguments = moderator.debate_log
    
    consensus_report = consensus_builder.build_consensus(all_arguments)
    
    # Add bias analysis if available
    if "bias_analysis" in tool_context.state:
        consensus_report["bias_analysis"] = tool_context.state["bias_analysis"]
    
    # Log the complete debate
    logger = DebateLogger()
    transcript_id = logger.save_debate_transcript(debate_context, all_arguments, consensus_report)
    
    tool_context.state["final_consensus"] = consensus_report
    tool_context.state["transcript_id"] = transcript_id
    
    return consensus_report