"""Optimist Analyst Agent: Solution-focused opportunity identification."""

import os
from google.adk.agents import Agent
from google.adk.agents.callback_context import CallbackContext
from google.genai import types

from .tools import identify_opportunities, analyze_success_patterns, generate_solutions
from .prompts import return_instructions_optimist_analyst

def setup_optimist_analyst(callback_context: CallbackContext):
    """Setup optimist analyst with debate context."""
    if "debate_context" not in callback_context.state:
        callback_context.state["debate_context"] = {
            "persona": "optimist",
            "focus": "opportunities_and_solutions"
        }

optimist_analyst_agent = Agent(
    model=os.getenv("OPTIMIST_MODEL", "claude-3-sonnet"),
    name="optimist_analyst",
    instruction=return_instructions_optimist_analyst(),
    tools=[
        identify_opportunities,
        analyze_success_patterns,
        generate_solutions
    ],
    before_agent_callback=setup_optimist_analyst,
    generate_content_config=types.GenerateContentConfig(temperature=0.4)
)