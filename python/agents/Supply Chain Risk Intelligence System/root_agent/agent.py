import os
from datetime import datetime
import logging

from google.genai import types

from google.adk.agents import Agent
from google.adk.agents.callback_context import CallbackContext
from google.adk.tools import load_artifacts
from .prompts import return_instructions_root
from .tools import trigger_data_collection
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
COLLECTION_FREQUENCY = int(os.getenv("COLLECTION_FREQUENCY", "300")) 

def setup_before_agent_call(callback_context: CallbackContext):
    """Setup the root agent before each call"""
    
    # Initialize system state
    if "supply_chain_system" not in callback_context.state:
        callback_context.state["supply_chain_system"] = {
            "initialized": True,
            "start_time": datetime.utcnow().isoformat(),
            "collection_cycles": 0,
            "total_events_processed": 0,
            "last_collection_timestamp": None,
            "emergency_active": False,
            "system_health": "healthy"
        }
    
    # Update system context in instructions
    system_status = callback_context.state["supply_chain_system"]
    
    callback_context._invocation_context.agent.instruction = (
        return_instructions_root() + f"""
        
    **Current System Status:**
    - System uptime: {system_status.get('start_time')}
    - Collection cycles completed: {system_status.get('collection_cycles', 0)}
    - Total events processed: {system_status.get('total_events_processed', 0)}
    - Last collection: {system_status.get('last_collection_timestamp', 'Never')}
    - Emergency mode: {'ACTIVE' if system_status.get('emergency_active') else 'Inactive'}
    - System health: {system_status.get('system_health', 'Unknown')}
    
    **Available Sub-Agents:**
    - DataCollectorAgent: Real-time data ingestion from external APIs
    - AnalysisAgent: Risk analysis and pattern recognition (placeholder)
    - AlertAgent: Alert generation and management (placeholder)
    """
    )
    
    logger.info("Root agent setup completed")

# Main Root Agent Definition
root_agent = Agent(
    model=os.getenv("ROOT_AGENT_MODEL", "gemini-2.0-flash-001"),
    name="supply_chain_orchestrator",
    instruction=return_instructions_root(),
    global_instruction=(
        f"""
    You are the central orchestrator for a Supply Chain Risk Intelligence System.
    Current date and time: {datetime.utcnow().isoformat()}
    Project ID: {PROJECT_ID}
    Collection frequency: {COLLECTION_FREQUENCY} seconds
    """),
    sub_agents=[data_collector_agent],  # Add other sub-agents as they're implemented
    tools=[
        trigger_data_collection,
        load_artifacts,
    ],
    before_agent_callback=setup_before_agent_call,
    generate_content_config=types.GenerateContentConfig(temperature=0.1),
)