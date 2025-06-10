#!/usr/bin/env python3
"""
ADK Import Diagnostic Script

This script helps diagnose import issues with the Supply Chain Risk Intelligence System
"""

import os
import sys
import traceback
from pathlib import Path

def test_basic_imports():
    """Test basic Python and Google Cloud imports"""
    print("🔍 Testing basic imports...")
    
    try:
        import datetime
        print("  ✅ datetime")
    except ImportError as e:
        print(f"  ❌ datetime: {e}")
    
    try:
        from google.adk.agents import Agent
        print("  ✅ google.adk.agents.Agent")
    except ImportError as e:
        print(f"  ❌ google.adk.agents.Agent: {e}")
    
    try:
        from google.adk.tools import ToolContext
        print("  ✅ google.adk.tools.ToolContext")
    except ImportError as e:
        print(f"  ❌ google.adk.tools.ToolContext: {e}")
    
    try:
        from google.cloud import pubsub_v1
        print("  ✅ google.cloud.pubsub_v1")
    except ImportError as e:
        print(f"  ❌ google.cloud.pubsub_v1: {e}")

def test_project_structure():
    """Test project file structure"""
    print("\n📁 Testing project structure...")
    
    current_dir = Path.cwd()
    required_files = [
        "root_agent/agent.py",
        "root_agent/tools.py",
        "root_agent/prompts.py",
        "sub_agents/__init__.py",
        "sub_agents/data_collector/__init__.py",
        "sub_agents/data_collector/agent.py",
        "sub_agents/data_collector/tools.py",
        "sub_agents/data_collector/prompts.py",
        ".env"
    ]
    
    for file_path in required_files:
        full_path = current_dir / file_path
        if full_path.exists():
            print(f"  ✅ {file_path}")
        else:
            print(f"  ❌ {file_path} (missing)")

def test_module_imports():
    """Test module imports step by step"""
    print("\n🧩 Testing module imports...")
    
    # Add current directory to Python path
    current_dir = Path.cwd()
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
        print(f"  📝 Added to sys.path: {current_dir}")
    
    # Test root_agent imports
    try:
        from root_agent.prompts import return_instructions_root
        print("  ✅ root_agent.prompts.return_instructions_root")
    except ImportError as e:
        print(f"  ❌ root_agent.prompts: {e}")
        traceback.print_exc()
    
    try:
        from root_agent.tools import trigger_data_collection
        print("  ✅ root_agent.tools.trigger_data_collection")
    except ImportError as e:
        print(f"  ❌ root_agent.tools: {e}")
        traceback.print_exc()
    
    # Test sub_agents imports
    try:
        from sub_agents.data_collector.prompts import return_instructions_data_collector
        print("  ✅ sub_agents.data_collector.prompts.return_instructions_data_collector")
    except ImportError as e:
        print(f"  ❌ sub_agents.data_collector.prompts: {e}")
        traceback.print_exc()
    
    try:
        from sub_agents.data_collector.tools import collect_all_sources
        print("  ✅ sub_agents.data_collector.tools.collect_all_sources")
    except ImportError as e:
        print(f"  ❌ sub_agents.data_collector.tools: {e}")
        traceback.print_exc()
    
    try:
        from sub_agents.data_collector.agent import root_agent as data_collector_agent
        print("  ✅ sub_agents.data_collector.agent.root_agent")
        print(f"       Agent type: {type(data_collector_agent)}")
        print(f"       Agent name: {getattr(data_collector_agent, 'name', 'unknown')}")
    except ImportError as e:
        print(f"  ❌ sub_agents.data_collector.agent: {e}")
        traceback.print_exc()
    
    try:
        from sub_agents import data_collector_agent
        print("  ✅ sub_agents.data_collector_agent")
        print(f"       Agent type: {type(data_collector_agent)}")
    except ImportError as e:
        print(f"  ❌ sub_agents.__init__: {e}")
        traceback.print_exc()

def test_root_agent():
    """Test root agent creation"""
    print("\n🏗️ Testing root agent creation...")
    
    try:
        from root_agent.agent import root_agent
        print("  ✅ root_agent.agent.root_agent imported")
        print(f"       Agent type: {type(root_agent)}")
        print(f"       Agent name: {getattr(root_agent, 'name', 'unknown')}")
        print(f"       Tools count: {len(getattr(root_agent, 'tools', []))}")
        print(f"       Sub-agents count: {len(getattr(root_agent, 'sub_agents', []))}")
    except ImportError as e:
        print(f"  ❌ root_agent.agent: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"  ⚠️  root_agent loaded but with error: {e}")
        traceback.print_exc()

def test_environment():
    """Test environment variables"""
    print("\n🌍 Testing environment variables...")
    
    required_vars = ["GOOGLE_CLOUD_PROJECT", "PUBSUB_TOPIC"]
    optional_vars = ["NOAA_API_KEY", "FRED_API_KEY", "TWITTER_BEARER_TOKEN", 
                    "MARINETRAFFIC_API_KEY", "GDELT_API_KEY"]
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"  ✅ {var}: {value}")
        else:
            print(f"  ❌ {var}: NOT SET")
    
    configured_apis = 0
    for var in optional_vars:
        value = os.getenv(var)
        if value and not value.startswith("your-"):
            print(f"  ✅ {var}: CONFIGURED")
            configured_apis += 1
        else:
            print(f"  ⚠️  {var}: NOT CONFIGURED")
    
    print(f"  📊 API Keys configured: {configured_apis}/{len(optional_vars)}")

def test_function_signatures():
    """Test function signatures for ADK compatibility"""
    print("\n🔧 Testing function signatures...")
    
    try:
        from root_agent.tools import trigger_data_collection
        import inspect
        
        sig = inspect.signature(trigger_data_collection)
        print(f"  ✅ trigger_data_collection signature: {sig}")
        
        # Check for problematic type hints
        for param_name, param in sig.parameters.items():
            if param.annotation != inspect.Parameter.empty:
                print(f"       {param_name}: {param.annotation}")
                
                # Check for problematic Optional types
                if hasattr(param.annotation, '__origin__'):
                    if param.annotation.__origin__ is Union:
                        print(f"         ⚠️  Union type detected: {param.annotation}")
                
    except Exception as e:
        print(f"  ❌ Function signature test failed: {e}")

def main():
    """Main diagnostic function"""
    print("="*70)
    print("🔍 ADK IMPORT DIAGNOSTIC TOOL")
    print("="*70)
    print(f"Working directory: {Path.cwd()}")
    print(f"Python version: {sys.version}")
    print(f"Python path: {sys.path[:3]}...")  # Show first 3 entries
    print()
    
    test_basic_imports()
    test_project_structure()
    test_environment()
    test_module_imports()
    test_function_signatures()
    test_root_agent()
    
    print("\n" + "="*70)
    print("DIAGNOSTIC COMPLETE")
    print("="*70)
    print("\n💡 Next Steps:")
    print("1. Fix any missing files or import errors shown above")
    print("2. Replace problematic type hints with simpler ones")
    print("3. Ensure all modules can be imported successfully")
    print("4. Run: poetry run adk web")
    print("5. Select root_agent and test")

if __name__ == "__main__":
    main()