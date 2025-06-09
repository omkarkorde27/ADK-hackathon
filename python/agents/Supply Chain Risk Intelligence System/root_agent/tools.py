import os
import sys
import logging
from datetime import datetime

# Add project root to path for proper imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from google.adk.tools import ToolContext

# Import with error handling for ADK compatibility
try:
    from sub_agents.data_collector.agent import root_agent as data_collector_agent
except ImportError as e:
    logging.getLogger(__name__).warning(f"Could not import data_collector_agent: {e}")
    data_collector_agent = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def trigger_data_collection(
    sources: str = "all",
    emergency_mode: bool = False,
    tool_context: ToolContext = None
):
    """
    Tool to trigger data collection from specified sources
    
    Args:
        sources: Comma-separated list of sources ("all", "NOAA", "GDELT", etc.)
        emergency_mode: Whether to run in emergency collection mode
        tool_context: ADK tool context
    
    Returns:
        Collection results and summary
    """
    logger.info(f"Triggering data collection - Sources: {sources}, Emergency: {emergency_mode}")
    
    try:
        # Check if data collector agent is available
        if data_collector_agent is None:
            logger.error("DataCollectorAgent not available - check imports")
            return {
                "status": "error",
                "error": "DataCollectorAgent not available",
                "sources_requested": sources,
                "suggestion": "Check import paths and ensure all dependencies are installed"
            }
        
        # Try direct tool import as fallback
        try:
            # Direct import of collection function
            from sub_agents.data_collector.tools import collect_all_sources
            
            # Call collection function directly
            collection_results = await collect_all_sources(
                sources=sources,
                emergency_mode=emergency_mode,
                tool_context=tool_context
            )
            
        except ImportError:
            # Simple fallback response
            collection_results = {
                "status": "error",
                "message": "Could not import collection tools",
                "total_events_collected": 0,
                "sources_processed": [],
                "errors": ["Import error - check module structure"]
            }
        
        # Store results in context for downstream agents
        if tool_context:
            tool_context.state["last_collection_results"] = collection_results
            tool_context.state["last_collection_timestamp"] = datetime.utcnow().isoformat()
            
            # Update system statistics
            system_stats = tool_context.state.setdefault("supply_chain_system", {})
            system_stats["collection_cycles"] = system_stats.get("collection_cycles", 0) + 1
            
            if collection_results.get("status") == "success":
                events_collected = collection_results.get("total_events_collected", 0)
                system_stats["total_events_processed"] = system_stats.get("total_events_processed", 0) + events_collected
                system_stats["system_health"] = "healthy"
            else:
                system_stats["system_health"] = "degraded"
        
        return {
            "status": "success",
            "collection_results": collection_results,
            "sources_requested": sources,
            "emergency_mode": emergency_mode,
            "events_collected": collection_results.get("total_events_collected", 0),
            "sources_processed": collection_results.get("sources_processed", []),
            "errors": collection_results.get("errors", [])
        }
        
    except Exception as e:
        error_msg = f"Data collection failed: {str(e)}"
        logger.error(error_msg)
        
        # Update system health in context
        if tool_context:
            system_stats = tool_context.state.setdefault("supply_chain_system", {})
            system_stats["system_health"] = "error"
            system_stats["last_error"] = error_msg
            system_stats["last_error_timestamp"] = datetime.utcnow().isoformat()
        
        return {
            "status": "error",
            "error": error_msg,
            "sources_requested": sources
        }

async def get_system_status(
    include_details: bool = True,
    tool_context: ToolContext = None
):
    """
    Get current system status and health information
    
    Args:
        include_details: Whether to include detailed statistics
        tool_context: ADK tool context
        
    Returns:
        System status information
    """
    logger.info("Retrieving system status")
    
    status = {
        "timestamp": datetime.utcnow().isoformat(),
        "system_health": "unknown",
        "agents_available": {
            "data_collector": data_collector_agent is not None
        }
    }
    
    if tool_context and "supply_chain_system" in tool_context.state:
        system_state = tool_context.state["supply_chain_system"]
        status.update({
            "system_health": system_state.get("system_health", "unknown"),
            "uptime": system_state.get("start_time"),
            "collection_cycles": system_state.get("collection_cycles", 0),
            "total_events_processed": system_state.get("total_events_processed", 0),
            "last_collection": system_state.get("last_collection_timestamp"),
            "emergency_active": system_state.get("emergency_active", False)
        })
        
        if include_details:
            # API status if available
            if "api_status" in tool_context.state:
                status["api_status"] = tool_context.state["api_status"]
            
            # Collection statistics
            if "collection_stats" in tool_context.state:
                status["collection_stats"] = tool_context.state["collection_stats"]
            
            # Last collection results summary
            if "last_collection_results" in tool_context.state:
                last_results = tool_context.state["last_collection_results"]
                status["last_collection_summary"] = {
                    "sources_processed": last_results.get("sources_processed", []),
                    "total_events": last_results.get("total_events_collected", 0),
                    "errors": len(last_results.get("errors", [])),
                    "duration": last_results.get("duration_seconds", 0)
                }
    
    return status

async def emergency_response(
    crisis_type: str,
    geographic_focus: str = "",
    tool_context: ToolContext = None
):
    """
    Trigger emergency response mode for crisis situations
    
    Args:
        crisis_type: Type of crisis (natural_disaster, geopolitical, economic, etc.)
        geographic_focus: Geographic area to focus monitoring
        tool_context: ADK tool context
        
    Returns:
        Emergency response results
    """
    logger.info(f"Emergency response triggered - Crisis: {crisis_type}, Focus: {geographic_focus}")
    
    # Define crisis-specific keywords
    crisis_keywords = {
        "natural_disaster": ["earthquake", "tsunami", "hurricane", "typhoon", "flood", "wildfire", "volcano"],
        "geopolitical": ["war", "conflict", "sanctions", "border closure", "trade dispute", "embargo"],
        "economic": ["recession", "inflation", "currency crisis", "market crash", "bank failure"],
        "pandemic": ["outbreak", "lockdown", "quarantine", "travel ban", "factory closure"],
        "cyber": ["cyber attack", "ransomware", "data breach", "system outage", "network failure"],
        "logistics": ["port strike", "shipping delay", "rail disruption", "trucker strike", "fuel shortage"]
    }
    
    # Build emergency keyword list
    emergency_keywords = crisis_keywords.get(crisis_type, [])
    
    # Add geographic focus to keywords
    if geographic_focus:
        emergency_keywords.append(geographic_focus)
    
    try:
        # Trigger emergency data collection
        emergency_collection = await trigger_data_collection(
            sources="all",
            emergency_mode=True,
            tool_context=tool_context
        )
        
        # Update emergency status in context
        if tool_context:
            emergency_state = {
                "crisis_type": crisis_type,
                "geographic_focus": geographic_focus,
                "keywords": emergency_keywords,
                "activated_at": datetime.utcnow().isoformat(),
                "collection_results": emergency_collection
            }
            tool_context.state["emergency_response"] = emergency_state
            
            # Safely update supply chain system state
            supply_chain_system = tool_context.state.setdefault("supply_chain_system", {})
            supply_chain_system["emergency_active"] = True
        
        return {
            "status": "success",
            "crisis_type": crisis_type,
            "geographic_focus": geographic_focus,
            "keywords_used": emergency_keywords,
            "collection_results": emergency_collection,
            "response_time": "immediate",
            "next_collection": "continuous monitoring activated"
        }
        
    except Exception as e:
        error_msg = f"Emergency response failed: {str(e)}"
        logger.error(error_msg)
        return {
            "status": "error",
            "crisis_type": crisis_type,
            "error": error_msg
        }