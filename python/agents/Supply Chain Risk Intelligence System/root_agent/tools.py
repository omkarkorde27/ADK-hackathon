import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from google.adk.tools import ToolContext
from google.adk.tools.agent_tool import AgentTool
from ..sub_agents import data_collector_agent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def trigger_data_collection(
    sources: str = "all",
    emergency_mode: bool = False,
    keywords: Optional[List[str]] = None,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Tool to trigger data collection from specified sources
    
    Args:
        sources: Comma-separated list of sources ("all", "NOAA", "GDELT", etc.)
        emergency_mode: Whether to run in emergency collection mode
        keywords: Additional keywords for emergency collection
        tool_context: ADK tool context
    
    Returns:
        Collection results and summary
    """
    logger.info(f"Triggering data collection - Sources: {sources}, Emergency: {emergency_mode}")
    
    try:
        # Prepare collection request
        collection_request = {
            "action": "emergency_collect" if emergency_mode else "collect_data",
            "sources": sources,
            "keywords": keywords or [],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Call DataCollectorAgent
        data_collector_tool = AgentTool(agent=data_collector_agent)
        collection_results = await data_collector_tool.run_async(
            args={"request": str(collection_request)},
            tool_context=tool_context
        )
        
        # Store results in context for downstream agents
        if tool_context:
            tool_context.state["last_collection_results"] = collection_results
            tool_context.state["last_collection_timestamp"] = datetime.utcnow().isoformat()
        
        return {
            "status": "success",
            "collection_results": collection_results,
            "sources_requested": sources,
            "emergency_mode": emergency_mode
        }
        
    except Exception as e:
        error_msg = f"Data collection failed: {str(e)}"
        logger.error(error_msg)
        return {
            "status": "error",
            "error": error_msg,
            "sources_requested": sources
        }