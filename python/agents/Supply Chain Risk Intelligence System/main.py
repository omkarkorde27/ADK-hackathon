#!/usr/bin/env python3
"""
Main entry point for testing the DataCollectorAgent
"""

import asyncio
import logging
import os
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import the data collector agent directly for testing
from sub_agents.data_collector.agent import root_agent as data_collector_agent
from sub_agents.data_collector.tools import collect_all_sources


class MockToolContext:
    """Mock tool context for testing"""
    def __init__(self):
        self.state = {}


async def test_data_collector():
    """Test the DataCollectorAgent functionality"""
    logger.info("Starting DataCollectorAgent test...")
    
    # Check required environment variables
    required_env_vars = ["GOOGLE_CLOUD_PROJECT", "PUBSUB_TOPIC"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        logger.info("Please copy .env.template to .env and fill in your values")
        return
    
    # Create mock tool context
    tool_context = MockToolContext()
    
    try:
        # Test data collection from all sources
        logger.info("Testing data collection from all sources...")
        
        results = await collect_all_sources(
            sources="all",
            emergency_mode=False,
            tool_context=tool_context
        )
        
        logger.info("Collection Results:")
        logger.info(f"  - Sources processed: {results.get('sources_processed', [])}")
        logger.info(f"  - Total events collected: {results.get('total_events_collected', 0)}")
        logger.info(f"  - Duration: {results.get('duration_seconds', 0):.2f} seconds")
        
        if results.get('errors'):
            logger.warning(f"  - Errors encountered: {len(results['errors'])}")
            for error in results['errors']:
                logger.warning(f"    • {error}")
        
        # Print API status
        api_status = tool_context.state.get('api_status', {})
        logger.info("API Connection Status:")
        for api, status in api_status.items():
            status_emoji = "✅" if status == "connected" else "❌" if status == "error" else "❓"
            logger.info(f"  {status_emoji} {api}: {status}")
        
        return results
        
    except Exception as e:
        logger.error(f"Test failed with error: {str(e)}")
        raise


async def test_individual_sources():
    """Test individual data sources"""
    from sub_agents.data_collector.tools import (
        fetch_from_noaa, fetch_from_gdelt, fetch_from_marinetraffic,
        fetch_from_fred, fetch_from_twitter
    )
    
    tool_context = MockToolContext()
    
    sources_to_test = [
        ("NOAA", fetch_from_noaa),
        ("GDELT", fetch_from_gdelt),
        ("MarineTraffic", fetch_from_marinetraffic),
        ("FRED", fetch_from_fred),
        ("Twitter", fetch_from_twitter)
    ]
    
    results = {}
    
    for source_name, fetch_func in sources_to_test:
        logger.info(f"Testing {source_name}...")
        try:
            result = await fetch_func(tool_context=tool_context)
            results[source_name] = result
            
            if result.get("status") == "success":
                logger.info(f"  ✅ {source_name}: {result.get('events_collected', 0)} events")
            else:
                logger.warning(f"  ❌ {source_name}: {result.get('message', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"  💥 {source_name}: Exception - {str(e)}")
            results[source_name] = {"status": "exception", "error": str(e)}
    
    return results


async def main():
    """Main test function"""
    print("="*60)
    print("Supply Chain Risk Intelligence - DataCollectorAgent Test")
    print("="*60)
    print(f"Start time: {datetime.utcnow().isoformat()}")
    print(f"Project ID: {os.getenv('GOOGLE_CLOUD_PROJECT', 'NOT SET')}")
    print(f"Pub/Sub Topic: {os.getenv('PUBSUB_TOPIC', 'NOT SET')}")
    print()
    
    # Test 1: Individual sources
    print("🔍 Testing individual data sources...")
    individual_results = await test_individual_sources()
    print()
    
    # Test 2: Complete collection
    print("🚀 Testing complete data collection...")
    collection_results = await test_data_collector()
    print()
    
    # Summary
    print("="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    working_sources = [name for name, result in individual_results.items() 
                      if result.get("status") == "success"]
    
    print(f"Working APIs: {len(working_sources)}/5")
    for source in working_sources:
        print(f"  ✅ {source}")
    
    failed_sources = [name for name, result in individual_results.items() 
                     if result.get("status") != "success"]
    
    if failed_sources:
        print(f"Failed APIs: {len(failed_sources)}/5")
        for source in failed_sources:
            print(f"  ❌ {source}")
    
    total_events = collection_results.get('total_events_collected', 0)
    print(f"Total events collected: {total_events}")
    
    if total_events > 0:
        print("\n🎉 DataCollectorAgent is working! Events are being collected and processed.")
    else:
        print("\n⚠️  No events collected. Check your API keys and network connection.")


if __name__ == "__main__":
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Run the test
    asyncio.run(main())