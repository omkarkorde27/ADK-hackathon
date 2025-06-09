#!/usr/bin/env python3
"""
Fixed Root Agent with Proper Import Structure for ADK Compatibility
"""
import os
import sys
from datetime import datetime
import logging

# Add current directory to Python path for proper module resolution
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from google.genai import types
from google.adk.agents import Agent
from google.adk.agents.callback_context import CallbackContext
from google.adk.tools import load_artifacts
from dotenv import load_dotenv

# Import local modules with absolute paths
from root_agent.prompts import return_instructions_root
from root_agent.tools import trigger_data_collection

# Import sub-agent with corrected path
try:
    from sub_agents.data_collector.agent import root_agent as data_collector_agent
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("Could not import data_collector_agent - checking alternative paths")
    
    # Alternative import paths for ADK compatibility
    try:
        import sub_agents.data_collector.agent as dc_module
        data_collector_agent = dc_module.root_agent
    except ImportError:
        try:
            # Direct import as fallback
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "data_collector_agent", 
                os.path.join(project_root, "sub_agents", "data_collector", "agent.py")
            )
            dc_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(dc_module)
            data_collector_agent = dc_module.root_agent
        except Exception as e:
            logger.error(f"Failed to import data_collector_agent: {e}")
            # Create a dummy agent to prevent crashes
            data_collector_agent = None

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    - DataCollectorAgent: {'CONNECTED' if data_collector_agent else 'IMPORT ERROR - CHECK PATHS'}
    
    **Project Configuration:**
    - Project ID: {PROJECT_ID}
    - Collection Frequency: {COLLECTION_FREQUENCY}s
    """
    )
    
    logger.info("Root agent setup completed")

# Prepare sub-agents list
sub_agents_list = []
if data_collector_agent is not None:
    sub_agents_list.append(data_collector_agent)
    logger.info("DataCollectorAgent loaded successfully")
else:
    logger.warning("DataCollectorAgent not available - running in limited mode")

# Main Root Agent Definition
root_agent = Agent(
    model=os.getenv("ROOT_AGENT_MODEL", "gemini-2.0-flash-001"),
    name="supply_chain_orchestrator",
    instruction=return_instructions_root(),
    global_instruction=(
        f"""
    You are the central orchestrator for a Supply Chain Risk Intelligence System.
    Current date and time: {datetime.utcnow().isoformat()}
    Project ID: {PROJECT_ID or 'NOT CONFIGURED'}
    Collection frequency: {COLLECTION_FREQUENCY} seconds
    
    **System Capabilities:**
    - Real-time data collection from 5 external APIs (NOAA, GDELT, MarineTraffic, FRED, Twitter)
    - Data normalization and GeoJSON conversion
    - Google Cloud Pub/Sub publishing
    - Emergency collection modes
    - Document AI processing (optional)
    
    **Current Mode:** {'Production' if PROJECT_ID else 'Development/Testing'}
    """),
    sub_agents=sub_agents_list,
    tools=[
        trigger_data_collection,
        load_artifacts,
    ],
    before_agent_callback=setup_before_agent_call,
    generate_content_config=types.GenerateContentConfig(temperature=0.1),
)