# Copyright 2025 Google LLC
# Licensed under the Apache License, Version 2.0

"""InsightSynergy Council: Multi-Agent Debate-Driven Data Analysis

Main orchestrator agent that coordinates the debate-driven analysis process.
"""

import os
from datetime import date
from google.genai import types
from google.adk.agents import Agent
from google.adk.agents.callback_context import CallbackContext
from google.adk.tools import load_artifacts

from .sub_agents import db_agent
from .prompts import return_instructions_root
from .tools import (
    call_ds_agent,
    call_db_agent,
    #initiate_council_debate,
    #retrieve_data_insights,
    #calculate_bias_score,
    #generate_consensus_report
)
from .sub_agents.bigquery.tools import (
    get_database_settings as get_bq_database_settings,
)
date_today = date.today()

def setup_before_agent_call(callback_context: CallbackContext):
    """Setup the root agent with database and debate settings."""
    
        # setting up database settings in session.state
    if "database_settings" not in callback_context.state:
        db_settings = dict()
        db_settings["use_database"] = "BigQuery"
        callback_context.state["all_db_settings"] = db_settings
    
    if "debate_settings" not in callback_context.state:
        callback_context.state["debate_settings"] = {
            "max_rounds": 3,
            "conflict_threshold": 7,
            "fairness_threshold": float(os.getenv("FAIRNESS_THRESHOLD", "0.85")),
            "models": {
                "data_detective": "gemini-pro",
                "optimist": "claude-3-sonnet",
                "pessimist": "grok-1", 
                "ethical": "gpt-4",
                "synthesis": "gemini-1.5-pro"
            }
        }
    
        # setting up schema in instruction
    if callback_context.state["all_db_settings"]["use_database"] == "BigQuery":
        callback_context.state["database_settings"] = get_bq_database_settings()
        schema = callback_context.state["database_settings"]["bq_ddl_schema"]

        callback_context._invocation_context.agent.instruction = (
            return_instructions_root()
            + f"""

    --------- The BigQuery schema of the relevant data with a few sample rows. ---------
    {schema}

    """
    )

root_agent = Agent(
    model=os.getenv("ROOT_AGENT_MODEL"),
    name="insight_synergy_orchestrator",
    instruction=return_instructions_root(),
    global_instruction=f"""
    You are the InsightSynergy Council Orchestrator.
    Today's date: {date_today}
    
    Your role is to coordinate multi-agent debates that expose hidden biases 
    and generate consensus-driven insights through adversarial reasoning.
    """,
    sub_agents=[db_agent],
    tools=[
        call_db_agent,
        call_ds_agent,
        load_artifacts
    ],
    before_agent_callback=setup_before_agent_call,
    generate_content_config=types.GenerateContentConfig(temperature=0.01)
)