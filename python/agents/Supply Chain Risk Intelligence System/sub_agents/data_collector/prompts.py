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
Data Collector Sub-Agent Prompts

This module contains prompt templates for the DataCollectorAgent,
including instructions and optional tweet/news enrichment templates.
"""

from typing import Dict, List, Any


def return_instructions_data_collector() -> str:
    """Return main instructions for the DataCollectorAgent"""
    return """
    You are a specialized DataCollectorAgent for Supply Chain Risk Intelligence.

    **Primary Mission:**
    Perform real-time ingestion and normalization of structured and unstructured data
    from multiple external APIs to detect global supply chain disruptions.

    **Core Responsibilities:**
    1. **Multi-Source Data Ingestion:**
       - NOAA API: Weather alerts, typhoons, natural disasters
       - GDELT: Global news sentiment and event data
       - MarineTraffic API: Shipping routes, port congestion, vessel tracking
       - FRED: Economic indicators affecting supply chains
       - X (Twitter) API v2: Real-time social signals

    2. **Data Normalization & Processing:**
       - Convert all data to a standardized SupplyChainEvent format
       - Transform weather alerts into GeoJSON format
       - Extract location coordinates and geocode when missing
       - Classify event severity (low, medium, high, critical)

    3. **Real-Time Publishing:**
       - Stream all normalized events to Google Cloud Pub/Sub topic 'raw_events'
       - Ensure data integrity and proper error handling
       - Maintain stateless operation for distributed processing

    **Implementation Notes:**
    - Use helper tools in `data_collector/tools.py` for API calls and publishing
    - Keep the agent stateless and configuration-driven
    - Prioritize speed and relevance for live supply chain monitoring
    """


def generate_tweet_analysis_prompt(tweets: List[Dict[str, Any]]) -> str:
    """
    Generate prompt for analyzing tweets for supply chain relevance
    """
    tweet_samples = []
    for i, tweet in enumerate(tweets[:5]):
        text = tweet.get('text', '').replace('\n', ' ')[:200]
        tweet_samples.append(f"Tweet {i+1}: {text}...")
    sample_text = "\n".join(tweet_samples)

    return f"""
    Analyze the following tweets for supply chain disruption relevance:

    {sample_text}

    For each tweet, determine:
    1. Relevance to supply chain (0-10 scale)
    2. Severity level (low/medium/high/critical)
    3. Geographic location mentioned (if any)
    4. Specific impact type (e.g., port disruption, manufacturing delay, transportation issue, labor dispute, natural disaster)

    Provide output in JSON format with keys 'tweet_1', 'tweet_2', etc.
    """


def generate_news_analysis_prompt(articles: List[Dict[str, Any]]) -> str:
    """
    Generate prompt for analyzing news articles for supply chain impact
    """
    article_samples = []
    for i, article in enumerate(articles[:3]):
        title = article.get('title', '').strip()
        content = article.get('content', '').replace('\n', ' ')[:300]
        article_samples.append(f"Article {i+1}: {title} - {content}...")
    sample_text = "\n".join(article_samples)

    return f"""
    Analyze the following news articles for supply chain implications:

    {sample_text}

    For each article, provide:
    1. Supply chain relevance (0-10 scale)
    2. Impact severity (low/medium/high/critical)
    3. Affected regions
    4. Industry sectors impacted
    5. Timeline of impact (immediate/short-term/long-term)
    6. Confidence level (0-10)

    Return analysis in JSON format with keys 'article_1', 'article_2', etc.
    """


# ADD THESE MISSING FUNCTIONS:

def generate_weather_impact_prompt(weather_events: List[Dict[str, Any]]) -> str:
    """
    Generate prompt for analyzing weather events for supply chain impact
    """
    event_samples = []
    for i, event in enumerate(weather_events[:5]):
        event_type = event.get('event', 'Weather Event')
        area = event.get('areaDesc', 'Unknown Area')
        severity = event.get('severity', 'Unknown')
        headline = event.get('headline', 'No headline')
        event_samples.append(f"Event {i+1}: {event_type} in {area} - Severity: {severity} - {headline}")
    
    sample_text = "\n".join(event_samples)

    return f"""
    Assess the supply chain impact of these weather events:

    {sample_text}

    For each event, analyze:
    1. Supply chain disruption potential (0-10 scale)
    2. Affected infrastructure types (ports, airports, rail, manufacturing, agriculture, energy)
    3. Geographic impact radius (local/regional/national/international)
    4. Duration of potential impact (hours/days/weeks/months)
    5. Recovery timeline estimate

    Return analysis in JSON format with keys 'event_1', 'event_2', etc.
    """


def get_supply_chain_keywords() -> List[str]:
    """
    Return comprehensive list of supply chain monitoring keywords
    """
    return [
        # Port and Shipping
        "port strike", "port closure", "shipping delay", "container shortage",
        "cargo congestion", "vessel delay", "dock workers", "longshoremen",
        "freight rate", "supply bottleneck", "logistics disruption",
        
        # Manufacturing
        "factory fire", "factory closure", "production halt", "assembly line",
        "semiconductor shortage", "chip shortage", "component shortage",
        "manufacturing delay", "supplier bankruptcy", "quality control",
        
        # Natural Disasters
        "typhoon Taiwan", "hurricane", "earthquake", "tsunami", "flooding",
        "wildfire", "volcano", "cyclone", "storm", "drought",
        
        # Transportation
        "rail strike", "trucking shortage", "driver shortage", "fuel shortage",
        "pipeline closure", "bridge collapse", "highway closure", "airport closure",
        "flight cancellation", "cargo aircraft",
        
        # Trade and Economic
        "trade war", "tariff", "sanctions", "border closure", "customs delay",
        "currency crisis", "inflation", "recession", "supply chain finance",
        
        # Energy and Resources
        "power outage", "energy crisis", "oil shortage", "gas shortage",
        "coal shortage", "renewable energy", "blackout", "grid failure",
        
        # Labor and Social
        "labor dispute", "worker shortage", "strike", "lockout", "protest",
        "covid outbreak", "quarantine", "border restriction", "visa delay",
        
        # Technology and Cyber
        "cyber attack", "ransomware", "system outage", "data breach",
        "software glitch", "network failure", "automation failure",
        
        # Geographic Hotspots
        "Suez Canal", "Panama Canal", "Strait of Malacca", "Strait of Hormuz",
        "South China Sea", "Mediterranean", "Baltic Sea", "Persian Gulf"
    ]


def get_critical_facilities() -> List[Dict[str, Any]]:
    """
    Return list of critical supply chain facilities to monitor
    """
    return [
        # Major Ports
        {"name": "Shanghai Port", "lat": 31.2304, "lon": 121.4737, "type": "port", "region": "Asia"},
        {"name": "Singapore Port", "lat": 1.2966, "lon": 103.7764, "type": "port", "region": "Asia"},
        {"name": "Rotterdam Port", "lat": 51.9244, "lon": 4.4777, "type": "port", "region": "Europe"},
        {"name": "Los Angeles Port", "lat": 33.7175, "lon": -118.2718, "type": "port", "region": "North America"},
        {"name": "Long Beach Port", "lat": 33.7701, "lon": -118.2137, "type": "port", "region": "North America"},
        {"name": "Hamburg Port", "lat": 53.5511, "lon": 9.9937, "type": "port", "region": "Europe"},
        {"name": "Antwerp Port", "lat": 51.2194, "lon": 4.4025, "type": "port", "region": "Europe"},
        {"name": "Busan Port", "lat": 35.0796, "lon": 129.0756, "type": "port", "region": "Asia"},
        {"name": "Hong Kong Port", "lat": 22.3193, "lon": 114.1694, "type": "port", "region": "Asia"},
        
        # Manufacturing Hubs
        {"name": "Shenzhen Manufacturing", "lat": 22.5431, "lon": 114.0579, "type": "manufacturing", "region": "Asia"},
        {"name": "Taiwan Semiconductor", "lat": 24.7136, "lon": 120.9675, "type": "semiconductor", "region": "Asia"},
        {"name": "Detroit Manufacturing", "lat": 42.3314, "lon": -83.0458, "type": "automotive", "region": "North America"},
        {"name": "Stuttgart Manufacturing", "lat": 48.7758, "lon": 9.1829, "type": "automotive", "region": "Europe"},
        {"name": "Yokohama Manufacturing", "lat": 35.4437, "lon": 139.6380, "type": "manufacturing", "region": "Asia"},
        
        # Transit Corridors
        {"name": "Suez Canal", "lat": 30.0444, "lon": 32.3497, "type": "transit", "region": "Middle East"},
        {"name": "Panama Canal", "lat": 9.0820, "lon": -79.7674, "type": "transit", "region": "Central America"},
        {"name": "Strait of Malacca", "lat": 2.5000, "lon": 102.0000, "type": "transit", "region": "Asia"},
        {"name": "Strait of Hormuz", "lat": 26.5667, "lon": 56.2500, "type": "transit", "region": "Middle East"},
        
        # Energy Infrastructure
        {"name": "Texas Oil Refineries", "lat": 29.7604, "lon": -95.3698, "type": "energy", "region": "North America"},
        {"name": "North Sea Oil", "lat": 56.0000, "lon": 3.0000, "type": "energy", "region": "Europe"},
        {"name": "Persian Gulf Oil", "lat": 26.0000, "lon": 51.0000, "type": "energy", "region": "Middle East"},
        
        # Agricultural Centers
        {"name": "US Midwest Grain", "lat": 41.2033, "lon": -96.6720, "type": "agriculture", "region": "North America"},
        {"name": "Ukraine Wheat Belt", "lat": 49.0000, "lon": 32.0000, "type": "agriculture", "region": "Europe"},
        {"name": "Brazil Soybean", "lat": -15.7801, "lon": -47.9292, "type": "agriculture", "region": "South America"},
    ]