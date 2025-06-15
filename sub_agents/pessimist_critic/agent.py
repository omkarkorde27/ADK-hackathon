"""Pessimist Critic Agent: Devil's advocate and assumption challenger."""

import os
from google.adk.agents import Agent
from google.adk.agents.callback_context import CallbackContext
from google.genai import types

from .tools import identify_risks, challenge_assumptions, analyze_failure_patterns
from .prompts import return_instructions_pessimist_critic

def setup_pessimist_critic(callback_context: CallbackContext):
    """Setup pessimist critic with debate context."""
    if "debate_context" not in callback_context.state:
        callback_context.state["debate_context"] = {
            "persona": "pessimist",
            "focus": "risks_and_challenges"
        }

pessimist_critic_agent = Agent(
    model=os.getenv("PESSIMIST_MODEL", "grok-1"),
    name="pessimist_critic",
    instruction=return_instructions_pessimist_critic(),
    tools=[
        identify_risks,
        challenge_assumptions,
        analyze_failure_patterns
    ],
    before_agent_callback=setup_pessimist_critic,
    generate_content_config=types.GenerateContentConfig(temperature=0.5)
)