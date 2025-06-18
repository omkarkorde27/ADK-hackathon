"""Data Science Agent V2: generate nl2py and use code interpreter to run the code."""
import os
from google.adk.code_executors import VertexAiCodeExecutor
from google.adk.agents import Agent
from .prompts import return_instructions_ds


root_agent = Agent(
    model=os.getenv("ANALYTICS_AGENT_MODEL"),
    name="data_science_agent",
    instruction=return_instructions_ds(),
    code_executor=VertexAiCodeExecutor(
        optimize_data_file=True,
        stateful=True,
    ),
)