# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Data Collector Sub-Agent for Supply Chain Risk Intelligence System

This agent specializes in real-time data ingestion from multiple external APIs,
data normalization, and publishing to Pub/Sub for downstream processing.
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from google.adk.agents import Agent
from google.adk.agents.callback_context import CallbackContext
from google.genai import types
from dotenv import load_dotenv

from .tools import (
    fetch_from_noaa,
    fetch_from_gdelt, 
    fetch_from_marinetraffic,
    fetch_from_fred,
    fetch_from_twitter,
    normalize_to_geojson,
    publish_to_pubsub,
    process_documents,
    collect_all_sources,
    emergency_collect
)
from .prompts import return_instructions_data_collector

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "raw_events")

def setup_before_agent_call(callback_context: CallbackContext):
    """Setup the DataCollectorAgent before each call"""
    
    # Initialize data collector state
    if "data_collector" not in callback_context.state:
        callback_context.state["data_collector"] = {
            "initialized": True,
            "total_collections": 0,
            "successful_collections": 0,
            "failed_collections": 0,
            "total_events_published": 0,
            "last_collection_time": None,
            "active_sources": [],
            "error_count": 0
        }
    
    # Initialize API connection status
    if "api_status" not in callback_context.state:
        callback_context.state["api_status"] = {
            "NOAA": "unknown",
            "GDELT": "unknown", 
            "MarineTraffic": "unknown",
            "FRED": "unknown",
            "Twitter": "unknown"
        }
    
    # Update agent instruction with current status
    collector_stats = callback_context.state["data_collector"]
    api_status = callback_context.state["api_status"]
    
    callback_context._invocation_context.agent.instruction = (
        return_instructions_data_collector() + f"""
        
    **Current Collection Status:**
    - Total collections: {collector_stats.get('total_collections', 0)}
    - Success rate: {collector_stats.get('successful_collections', 0)}/{collector_stats.get('total_collections', 0) or 1}
    - Events published: {collector_stats.get('total_events_published', 0)}
    - Last collection: {collector_stats.get('last_collection_time', 'Never')}
    - Error count: {collector_stats.get('error_count', 0)}
    
    **API Connection Status:**
    - NOAA: {api_status.get('NOAA', 'unknown')}
    - GDELT: {api_status.get('GDELT', 'unknown')}
    - MarineTraffic: {api_status.get('MarineTraffic', 'unknown')}
    - FRED: {api_status.get('FRED', 'unknown')}
    - Twitter: {api_status.get('Twitter', 'unknown')}
    
    **Target Pub/Sub Topic:** {PUBSUB_TOPIC}
    **Project ID:** {PROJECT_ID}
    """
    )
    
    logger.info("DataCollectorAgent setup completed")

# Main DataCollectorAgent Definition
root_agent = Agent(
    model=os.getenv("DATA_COLLECTOR_MODEL", "gemini-2.0-flash-001"),
    name="supply_chain_data_collector",
    instruction=return_instructions_data_collector(),
    tools=[
        fetch_from_noaa,
        fetch_from_gdelt,
        fetch_from_marinetraffic, 
        fetch_from_fred,
        fetch_from_twitter,
        normalize_to_geojson,
        publish_to_pubsub,
        process_documents,
        collect_all_sources,
        emergency_collect
    ],
    before_agent_callback=setup_before_agent_call,
    generate_content_config=types.GenerateContentConfig(temperature=0.1),
)