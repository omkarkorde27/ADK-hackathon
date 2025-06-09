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
Data Collector Sub-Agent Tools - ADK Compatible Version

This module contains all the tool functions for the DataCollectorAgent,
including API fetchers, data normalizers, and publishing utilities.
Fixed for ADK compatibility with proper type hints.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Union
from dataclasses import dataclass, asdict
import base64

import aiohttp
import requests
import tweepy
import geojson
import geopy.distance
from google.cloud import pubsub_v1, documentai
from google.adk.tools import ToolContext
from dotenv import load_dotenv

from .prompts import (
    generate_tweet_analysis_prompt,
    generate_news_analysis_prompt,
    generate_weather_impact_prompt,
    get_supply_chain_keywords,
    get_critical_facilities
)

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "raw_events")
DOCUMENTAI_PROCESSOR_ID = os.getenv("DOCUMENTAI_PROCESSOR_ID")
DOCUMENTAI_LOCATION = os.getenv("DOCUMENTAI_LOCATION", "us")

# API Configuration
NOAA_API_KEY = os.getenv("NOAA_API_KEY")
GDELT_API_KEY = os.getenv("GDELT_API_KEY")
MARINETRAFFIC_API_KEY = os.getenv("MARINETRAFFIC_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

# Initialize clients
try:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
    logger.info(f"Pub/Sub client initialized for topic: {topic_path}")
except Exception as e:
    logger.warning(f"Pub/Sub client initialization failed: {e}")
    publisher = None
    topic_path = None

if TWITTER_BEARER_TOKEN:
    try:
        twitter_client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN)
        logger.info("Twitter client initialized")
    except Exception as e:
        logger.warning(f"Twitter client initialization failed: {e}")
        twitter_client = None
else:
    twitter_client = None

if DOCUMENTAI_PROCESSOR_ID:
    try:
        documentai_client = documentai.DocumentProcessorServiceClient()
        processor_name = documentai_client.processor_path(
            PROJECT_ID, DOCUMENTAI_LOCATION, DOCUMENTAI_PROCESSOR_ID
        )
        logger.info("Document AI client initialized")
    except Exception as e:
        logger.warning(f"Document AI client initialization failed: {e}")
        documentai_client = None
        processor_name = None
else:
    logger.warning("DOCUMENTAI_PROCESSOR_ID not configured - Document processing disabled")
    documentai_client = None
    processor_name = None

@dataclass
class SupplyChainEvent:
    """Normalized supply chain event data structure"""
    source: str
    event_type: str
    timestamp: datetime
    location: Dict[str, float] = None  # {"lat": float, "lon": float}
    severity: str = "medium"  # "low", "medium", "high", "critical"
    description: str = ""
    metadata: Dict[str, Any] = None
    raw_data: Dict[str, Any] = None
    geojson: Dict = None
    impact_score: float = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.raw_data is None:
            self.raw_data = {}

async def fetch_from_noaa(
    alert_types: str = "all",
    region: str = None,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Fetch weather alerts and typhoon data from NOAA API
    
    Args:
        alert_types: Types of alerts to fetch ("all", "severe", "marine", etc.)
        region: Geographic region to focus on
        tool_context: ADK tool context
        
    Returns:
        Collection results with events and metadata
    """
    logger.info(f"Fetching NOAA data - Alert types: {alert_types}, Region: {region}")
    
    if not NOAA_API_KEY:
        logger.warning("NOAA API key not configured")
        return {"status": "error", "message": "NOAA API key not configured"}
    
    events = []
    
    try:
        # NOAA Weather Alerts API
        url = "https://api.weather.gov/alerts/active"
        headers = {"User-Agent": "SupplyChainAgent/1.0"}
        
        params = {}
        if region:
            params["area"] = region
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    for alert in data.get("features", []):
                        properties = alert.get("properties", {})
                        geometry = alert.get("geometry")
                        
                        # Filter for supply chain relevant events
                        event_type = properties.get("event", "").lower()
                        supply_chain_relevant = any(keyword in event_type for keyword in 
                                                  ["hurricane", "typhoon", "flood", "earthquake", 
                                                   "wildfire", "tornado", "blizzard", "ice storm"])
                        
                        if supply_chain_relevant:
                            # Extract location
                            location = None
                            if geometry and geometry.get("coordinates"):
                                coords = geometry["coordinates"]
                                if coords and len(coords) > 0:
                                    if isinstance(coords[0], list) and len(coords[0]) > 0:
                                        location = {"lat": coords[0][0][1], "lon": coords[0][0][0]}
                                    elif len(coords) >= 2:
                                        location = {"lat": coords[1], "lon": coords[0]}
                            
                            # Convert to GeoJSON using normalize_to_geojson
                            geojson_data = await normalize_to_geojson(
                                event_data={"geometry": geometry, "properties": properties},
                                tool_context=tool_context
                            )
                            
                            event = SupplyChainEvent(
                                source="NOAA",
                                event_type=f"weather_{event_type.replace(' ', '_')}",
                                timestamp=datetime.fromisoformat(
                                    properties.get("sent", datetime.utcnow().isoformat()).replace("Z", "+00:00")
                                ),
                                location=location,
                                severity=_map_noaa_severity(properties.get("severity", "unknown")),
                                description=properties.get("headline", "Weather alert"),
                                metadata={
                                    "urgency": properties.get("urgency"),
                                    "certainty": properties.get("certainty"),
                                    "areas": properties.get("areaDesc"),
                                    "instruction": properties.get("instruction"),
                                },
                                raw_data=properties,
                                geojson=geojson_data.get("geojson") if isinstance(geojson_data, dict) else None
                            )
                            events.append(event)
                    
                    logger.info(f"Collected {len(events)} weather events from NOAA")
                else:
                    logger.error(f"NOAA API request failed with status {response.status}")
                    return {"status": "error", "message": f"API request failed: {response.status}"}
        
        # Update tool context
        if tool_context:
            tool_context.state.setdefault("api_status", {})["NOAA"] = "connected"
            tool_context.state.setdefault("collection_stats", {})["noaa_events"] = len(events)
        
        return {
            "status": "success",
            "source": "NOAA",
            "events_collected": len(events),
            "events": [asdict(event) for event in events],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        error_msg = f"Error fetching NOAA data: {str(e)}"
        logger.error(error_msg)
        
        if tool_context:
            tool_context.state.setdefault("api_status", {})["NOAA"] = "error"
        
        return {"status": "error", "message": error_msg}

async def fetch_from_gdelt(
    keywords: List[str] = None,
    timespan: str = "24h",
    max_records: int = 100,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Fetch news sentiment and event data from GDELT
    
    Args:
        keywords: Specific keywords to search for
        timespan: Time period to search ("24h", "7d", etc.)
        max_records: Maximum number of records to fetch
        tool_context: ADK tool context
        
    Returns:
        Collection results with events and metadata
    """
    if keywords is None:
        keywords = get_supply_chain_keywords()
    
    logger.info(f"Fetching GDELT data - Keywords: {keywords}, Timespan: {timespan}")
    
    events = []
    
    try:
        # GDELT Event Database API
        base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
        
        # Build query from keywords
        query_terms = " OR ".join([f'"{term}"' for term in keywords[:10]])  # Limit keywords
        
        params = {
            "query": query_terms,
            "mode": "ArtList",
            "format": "json",
            "timespan": timespan,
            "maxrecords": str(max_records),
            "sort": "DateDesc"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    for article in data.get("articles", []):
                        # Parse location if available
                        location = None
                        if article.get("socialgeolat") and article.get("socialgeolong"):
                            try:
                                location = {
                                    "lat": float(article["socialgeolat"]),
                                    "lon": float(article["socialgeolong"])
                                }
                            except (ValueError, TypeError):
                                pass
                        
                        event = SupplyChainEvent(
                            source="GDELT",
                            event_type="news_event",
                            timestamp=datetime.fromisoformat(
                                article.get("seendate", datetime.utcnow().isoformat())
                            ),
                            location=location,
                            severity=_analyze_gdelt_severity(article),
                            description=article.get("title", "Supply chain news event"),
                            metadata={
                                "url": article.get("url"),
                                "domain": article.get("domain"),
                                "language": article.get("language"),
                                "tone": article.get("tone"),
                                "source_country": article.get("sourcecountry"),
                            },
                            raw_data=article
                        )
                        events.append(event)
                    
                    logger.info(f"Collected {len(events)} news events from GDELT")
                else:
                    logger.error(f"GDELT API request failed with status {response.status}")
                    return {"status": "error", "message": f"API request failed: {response.status}"}
        
        # Update tool context
        if tool_context:
            tool_context.state.setdefault("api_status", {})["GDELT"] = "connected"
            tool_context.state.setdefault("collection_stats", {})["gdelt_events"] = len(events)
        
        return {
            "status": "success",
            "source": "GDELT",
            "events_collected": len(events),
            "events": [asdict(event) for event in events],
            "search_keywords": keywords[:10],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        error_msg = f"Error fetching GDELT data: {str(e)}"
        logger.error(error_msg)
        
        if tool_context:
            tool_context.state.setdefault("api_status", {})["GDELT"] = "error"
        
        return {"status": "error", "message": error_msg}

async def fetch_from_marinetraffic(
    ports: List[str] = None,
    vessel_types: List[str] = None,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Fetch shipping and logistics data from MarineTraffic API
    
    Args:
        ports: Specific ports to monitor
        vessel_types: Types of vessels to track
        tool_context: ADK tool context
        
    Returns:
        Collection results with events and metadata
    """
    logger.info(f"Fetching MarineTraffic data - Ports: {ports}")
    
    if not MARINETRAFFIC_API_KEY:
        logger.warning("MarineTraffic API key not configured")
        return {"status": "error", "message": "MarineTraffic API key not configured"}
    
    events = []
    
    try:
        # Major ports to monitor if none specified
        target_ports = ports or [
            {"name": "Shanghai", "lat": 31.2304, "lon": 121.4737},
            {"name": "Singapore", "lat": 1.2966, "lon": 103.7764},
            {"name": "Rotterdam", "lat": 51.9244, "lon": 4.4777},
            {"name": "Los Angeles", "lat": 33.7175, "lon": -118.2718},
            {"name": "Long Beach", "lat": 33.7701, "lon": -118.2137}
        ]
        
        for port in target_ports:
            if isinstance(port, str):
                # If port is just a name, skip for now
                continue
                
            url = f"https://services.marinetraffic.com/api/exportvessels/{MARINETRAFFIC_API_KEY}/v:3/protocol:jsono"
            
            params = {
                "timespan": "60",  # Last 60 minutes
                "minlat": port["lat"] - 0.1,
                "maxlat": port["lat"] + 0.1,
                "minlon": port["lon"] - 0.1,
                "maxlon": port["lon"] + 0.1
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Analyze vessel congestion
                        vessel_count = len(data) if data else 0
                        cargo_vessels = [v for v in data if v.get("ship_type") == "70"] if data else []
                        
                        # Create congestion event if threshold exceeded
                        if vessel_count > 50:  # Threshold for congestion
                            event = SupplyChainEvent(
                                source="MarineTraffic",
                                event_type="port_congestion",
                                timestamp=datetime.utcnow(),
                                location={"lat": port["lat"], "lon": port["lon"]},
                                severity="high" if vessel_count > 100 else "medium",
                                description=f"High vessel traffic detected at {port['name']} port: {vessel_count} vessels",
                                metadata={
                                    "port_name": port["name"],
                                    "vessel_count": vessel_count,
                                    "cargo_vessels": len(cargo_vessels),
                                    "congestion_threshold": 50,
                                },
                                raw_data={"vessels": data[:10] if data else []}  # Limit raw data size
                            )
                            events.append(event)
                    
                    elif response.status == 401:
                        logger.error("MarineTraffic API authentication failed")
                        return {"status": "error", "message": "API authentication failed"}
                    else:
                        logger.warning(f"MarineTraffic API request failed for {port['name']}: {response.status}")
            
            # Rate limiting
            await asyncio.sleep(1)
        
        # Update tool context
        if tool_context:
            tool_context.state.setdefault("api_status", {})["MarineTraffic"] = "connected"
            tool_context.state.setdefault("collection_stats", {})["marinetraffic_events"] = len(events)
        
        logger.info(f"Collected {len(events)} shipping events from MarineTraffic")
        
        return {
            "status": "success",
            "source": "MarineTraffic",
            "events_collected": len(events),
            "events": [asdict(event) for event in events],
            "ports_monitored": len(target_ports),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        error_msg = f"Error fetching MarineTraffic data: {str(e)}"
        logger.error(error_msg)
        
        if tool_context:
            tool_context.state.setdefault("api_status", {})["MarineTraffic"] = "error"
        
        return {"status": "error", "message": error_msg}

async def fetch_from_fred(
    indicators: List[str] = None,
    change_threshold: float = 2.0,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Fetch economic indicators from FRED API
    
    Args:
        indicators: Specific economic indicators to fetch
        change_threshold: Minimum percentage change to trigger event
        tool_context: ADK tool context
        
    Returns:
        Collection results with events and metadata
    """
    logger.info(f"Fetching FRED data - Indicators: {indicators}")
    
    if not FRED_API_KEY:
        logger.warning("FRED API key not configured")
        return {"status": "error", "message": "FRED API key not configured"}
    
    events = []
    
    # Default economic indicators for supply chain
    default_indicators = [
        {"series_id": "CPIAUCSL", "name": "Consumer Price Index"},
        {"series_id": "UNRATE", "name": "Unemployment Rate"},
        {"series_id": "PAYEMS", "name": "Total Nonfarm Payrolls"},
        {"series_id": "DPSACBW027SBOG", "name": "Personal Saving Rate"},
        {"series_id": "DEXUSEU", "name": "US/Euro Exchange Rate"}
    ]
    
    target_indicators = indicators or default_indicators
    
    try:
        base_url = "https://api.stlouisfed.org/fred/series/observations"
        
        for indicator in target_indicators:
            if isinstance(indicator, str):
                indicator = {"series_id": indicator, "name": indicator}
            
            params = {
                "series_id": indicator["series_id"],
                "api_key": FRED_API_KEY,
                "file_type": "json",
                "limit": "5",
                "sort_order": "desc"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(base_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        observations = data.get("observations", [])
                        
                        if len(observations) >= 2:
                            try:
                                current = float(observations[0]["value"])
                                previous = float(observations[1]["value"])
                                change_pct = ((current - previous) / previous) * 100
                                
                                # Flag significant changes
                                if abs(change_pct) > change_threshold:
                                    event = SupplyChainEvent(
                                        source="FRED",
                                        event_type="economic_indicator",
                                        timestamp=datetime.fromisoformat(observations[0]["date"]),
                                        location={"lat": 39.8283, "lon": -98.5795},  # US geographic center
                                        severity="high" if abs(change_pct) > 5.0 else "medium",
                                        description=f"Significant change in {indicator['name']}: {change_pct:.2f}%",
                                        metadata={
                                            "indicator_name": indicator["name"],
                                            "series_id": indicator["series_id"],
                                            "current_value": current,
                                            "previous_value": previous,
                                            "change_percent": change_pct,
                                            "threshold_exceeded": abs(change_pct) > change_threshold
                                        },
                                        raw_data=observations[0]
                                    )
                                    events.append(event)
                            except (ValueError, TypeError, ZeroDivisionError) as e:
                                logger.warning(f"Error processing FRED indicator {indicator['series_id']}: {e}")
                    
                    elif response.status == 400:
                        logger.warning(f"Invalid FRED series ID: {indicator['series_id']}")
                    else:
                        logger.error(f"FRED API request failed: {response.status}")
            
            await asyncio.sleep(0.5)  # Rate limiting
        
        # Update tool context
        if tool_context:
            tool_context.state.setdefault("api_status", {})["FRED"] = "connected"
            tool_context.state.setdefault("collection_stats", {})["fred_events"] = len(events)
        
        logger.info(f"Collected {len(events)} economic events from FRED")
        
        return {
            "status": "success",
            "source": "FRED",
            "events_collected": len(events),
            "events": [asdict(event) for event in events],
            "indicators_checked": len(target_indicators),
            "change_threshold": change_threshold,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        error_msg = f"Error fetching FRED data: {str(e)}"
        logger.error(error_msg)
        
        if tool_context:
            tool_context.state.setdefault("api_status", {})["FRED"] = "error"
        
        return {"status": "error", "message": error_msg}

async def fetch_from_twitter(
    keywords: List[str] = None,
    max_results: int = 100,
    include_retweets: bool = False,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Fetch real-time social signals from Twitter API v2
    
    Args:
        keywords: Specific keywords to search for
        max_results: Maximum number of tweets to fetch
        include_retweets: Whether to include retweets
        tool_context: ADK tool context
        
    Returns:
        Collection results with events and metadata
    """
    if keywords is None:
        keywords = get_supply_chain_keywords()
    
    logger.info(f"Fetching Twitter data - Keywords: {keywords}, Max results: {max_results}")
    
    if not TWITTER_BEARER_TOKEN or twitter_client is None:
        logger.warning("Twitter Bearer Token not configured or client not initialized")
        return {"status": "error", "message": "Twitter Bearer Token not configured"}
    
    events = []
    
    try:
        # Build search query
        query_terms = " OR ".join([f'"{term}"' for term in keywords[:10]])  # Limit keywords
        query = f"({query_terms}) lang:en"
        
        if not include_retweets:
            query += " -is:retweet"
        
        # Recent search (last 7 days)
        tweets = tweepy.Paginator(
            twitter_client.search_recent_tweets,
            query=query,
            max_results=min(max_results, 100),  # API limit
            tweet_fields=["created_at", "author_id", "context_annotations", "geo", "public_metrics"],
            user_fields=["verified", "location"],
            expansions=["author_id", "geo.place_id"]
        ).flatten(limit=max_results)
        
        tweet_count = 0
        for tweet in tweets:
            tweet_count += 1
            
            # Analyze sentiment and relevance
            severity = _analyze_twitter_sentiment(tweet.text)
            
            # Extract location if available
            location = None
            if hasattr(tweet, 'geo') and tweet.geo:
                if 'coordinates' in tweet.geo:
                    coords = tweet.geo['coordinates']
                    location = {"lat": coords[1], "lon": coords[0]}
            
            # Calculate relevance score
            relevance_score = _calculate_tweet_relevance(tweet.text, keywords)
            
            # Only include highly relevant tweets
            if relevance_score >= 5:
                event = SupplyChainEvent(
                    source="Twitter",
                    event_type="social_signal",
                    timestamp=tweet.created_at,
                    location=location,
                    severity=severity,
                    description=tweet.text[:200] + "..." if len(tweet.text) > 200 else tweet.text,
                    metadata={
                        "tweet_id": tweet.id,
                        "author_id": tweet.author_id,
                        "relevance_score": relevance_score,
                        "retweet_count": getattr(tweet.public_metrics, "retweet_count", 0) if hasattr(tweet, 'public_metrics') else 0,
                        "like_count": getattr(tweet.public_metrics, "like_count", 0) if hasattr(tweet, 'public_metrics') else 0,
                        "reply_count": getattr(tweet.public_metrics, "reply_count", 0) if hasattr(tweet, 'public_metrics') else 0,
                    },
                    raw_data={"text": tweet.text, "created_at": tweet.created_at.isoformat()}
                )
                events.append(event)
        
        # Update tool context
        if tool_context:
            tool_context.state.setdefault("api_status", {})["Twitter"] = "connected"
            tool_context.state.setdefault("collection_stats", {})["twitter_events"] = len(events)
        
        logger.info(f"Collected {len(events)} relevant social signals from {tweet_count} tweets")
        
        return {
            "status": "success",
            "source": "Twitter",
            "events_collected": len(events),
            "events": [asdict(event) for event in events],
            "tweets_processed": tweet_count,
            "search_keywords": keywords[:10],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        error_msg = f"Error fetching Twitter data: {str(e)}"
        logger.error(error_msg)
        
        if tool_context:
            tool_context.state.setdefault("api_status", {})["Twitter"] = "error"
        
        return {"status": "error", "message": error_msg}

async def normalize_to_geojson(
    event_data: Dict[str, Any],
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Convert event data to GeoJSON format
    
    Args:
        event_data: Event data containing geometry and properties
        tool_context: ADK tool context
        
    Returns:
        GeoJSON formatted data
    """
    logger.info("Converting event data to GeoJSON format")
    
    try:
        geometry = event_data.get("geometry")
        properties = event_data.get("properties", {})
        
        if not geometry:
            # Try to extract location from properties
            location = event_data.get("location")
            if location and "lat" in location and "lon" in location:
                geometry = {
                    "type": "Point",
                    "coordinates": [location["lon"], location["lat"]]
                }
            else:
                return {
                    "status": "warning",
                    "message": "No geometry data available for GeoJSON conversion"
                }
        
        # Create GeoJSON feature
        geojson_feature = geojson.Feature(
            geometry=geometry,
            properties={
                **properties,
                "timestamp": datetime.utcnow().isoformat(),
                "source": event_data.get("source", "unknown"),
                "event_type": event_data.get("event_type", "unknown")
            }
        )
        
        # Validate GeoJSON
        if geojson.is_valid(geojson_feature):
            return {
                "status": "success",
                "geojson": geojson_feature,
                "geometry_type": geometry.get("type"),
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            return {
                "status": "error",
                "message": "Invalid GeoJSON generated"
            }
        
    except Exception as e:
        error_msg = f"Error converting to GeoJSON: {str(e)}"
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}

async def publish_to_pubsub(
    events: List[Dict[str, Any]],
    batch_size: int = 10,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Publish events to Google Cloud Pub/Sub topic
    
    Args:
        events: List of events to publish
        batch_size: Number of events to publish in each batch
        tool_context: ADK tool context
        
    Returns:
        Publishing results and statistics
    """
    logger.info(f"Publishing {len(events)} events to Pub/Sub topic: {PUBSUB_TOPIC}")
    
    if not events:
        return {
            "status": "warning",
            "message": "No events provided for publishing"
        }
    
    if not publisher or not topic_path:
        logger.warning("Pub/Sub publisher not initialized")
        return {
            "status": "error",
            "message": "Pub/Sub publisher not initialized"
        }
    
    try:
        published_count = 0
        failed_count = 0
        
        # Process events in batches
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            
            for event in batch:
                try:
                    # Ensure event has required fields
                    if not isinstance(event, dict):
                        event = asdict(event) if hasattr(event, '__dict__') else event
                    
                    # Handle datetime serialization
                    if 'timestamp' in event and isinstance(event['timestamp'], datetime):
                        event['timestamp'] = event['timestamp'].isoformat()
                    
                    # Publish to Pub/Sub
                    message_data = json.dumps(event, default=str).encode("utf-8")
                    future = publisher.publish(topic_path, message_data)
                    
                    # Optional: Wait for publish confirmation (can be removed for better performance)
                    # message_id = future.result(timeout=1.0)
                    
                    published_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to publish event: {str(e)}")
                    failed_count += 1
            
            # Small delay between batches to avoid overwhelming Pub/Sub
            if i + batch_size < len(events):
                await asyncio.sleep(0.1)
        
        # Update tool context
        if tool_context:
            stats = tool_context.state.setdefault("collection_stats", {})
            stats["total_published"] = stats.get("total_published", 0) + published_count
            stats["total_failed"] = stats.get("total_failed", 0) + failed_count
        
        logger.info(f"Published {published_count} events, {failed_count} failed")
        
        return {
            "status": "success",
            "events_published": published_count,
            "events_failed": failed_count,
            "total_events": len(events),
            "topic": PUBSUB_TOPIC,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        error_msg = f"Error publishing to Pub/Sub: {str(e)}"
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}

async def collect_all_sources(
    sources: str = "all",
    emergency_mode: bool = False,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Orchestrate data collection from all configured sources
    
    Args:
        sources: Comma-separated list of sources or "all"
        emergency_mode: Whether to run in emergency collection mode
        tool_context: ADK tool context
        
    Returns:
        Comprehensive collection results from all sources
    """
    logger.info(f"Collecting from sources: {sources}, Emergency mode: {emergency_mode}")
    
    collection_start = datetime.utcnow()
    results = {
        "collection_id": f"collect_{int(collection_start.timestamp())}",
        "start_time": collection_start.isoformat(),
        "emergency_mode": emergency_mode,
        "sources_requested": sources,
        "sources_processed": [],
        "total_events_collected": 0,
        "errors": [],
        "source_results": {}
    }
    
    # Determine which sources to collect from
    if sources == "all":
        active_sources = ["NOAA", "GDELT", "MarineTraffic", "FRED", "Twitter"]
    else:
        active_sources = [s.strip() for s in sources.split(",")]
    
    # Define collection tasks
    tasks = []
    
    if "NOAA" in active_sources:
        tasks.append(("NOAA", fetch_from_noaa(tool_context=tool_context)))
    
    if "GDELT" in active_sources:
        gdelt_keywords = get_supply_chain_keywords() if emergency_mode else None
        tasks.append(("GDELT", fetch_from_gdelt(keywords=gdelt_keywords, tool_context=tool_context)))
    
    if "MarineTraffic" in active_sources:
        tasks.append(("MarineTraffic", fetch_from_marinetraffic(tool_context=tool_context)))
    
    if "FRED" in active_sources:
        tasks.append(("FRED", fetch_from_fred(tool_context=tool_context)))
    
    if "Twitter" in active_sources:
        twitter_keywords = get_supply_chain_keywords() if emergency_mode else None
        max_tweets = 200 if emergency_mode else 100
        tasks.append(("Twitter", fetch_from_twitter(keywords=twitter_keywords, max_results=max_tweets, tool_context=tool_context)))
    
    # Execute collection tasks concurrently
    try:
        all_events = []
        
        for source_name, task_coro in tasks:
            try:
                source_result = await task_coro
                results["source_results"][source_name] = source_result
                
                if source_result.get("status") == "success":
                    results["sources_processed"].append(source_name)
                    events = source_result.get("events", [])
                    all_events.extend(events)
                    results["total_events_collected"] += len(events)
                else:
                    error_msg = f"{source_name}: {source_result.get('message', 'Unknown error')}"
                    results["errors"].append(error_msg)
                    
            except Exception as e:
                error_msg = f"Error collecting from {source_name}: {str(e)}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
        
        # Publish all collected events to Pub/Sub
        if all_events:
            publish_result = await publish_to_pubsub(all_events, tool_context=tool_context)
            results["publish_result"] = publish_result
        
        # Update tool context with collection summary
        if tool_context:
            tool_context.state["last_collection_results"] = results
            tool_context.state["last_collection_timestamp"] = collection_start.isoformat()
            
            # Update overall statistics
            collector_stats = tool_context.state.setdefault("data_collector", {})
            collector_stats["total_collections"] = collector_stats.get("total_collections", 0) + 1
            collector_stats["successful_collections"] = collector_stats.get("successful_collections", 0) + (1 if results["sources_processed"] else 0)
            collector_stats["total_events_published"] = collector_stats.get("total_events_published", 0) + results["total_events_collected"]
            collector_stats["last_collection_time"] = collection_start.isoformat()
        
    except Exception as e:
        error_msg = f"Error in collection orchestration: {str(e)}"
        logger.error(error_msg)
        results["errors"].append(error_msg)
    
    results["end_time"] = datetime.utcnow().isoformat()
    results["duration_seconds"] = (datetime.utcnow() - collection_start).total_seconds()
    
    logger.info(f"Collection completed: {results['total_events_collected']} events from {len(results['sources_processed'])} sources")
    
    return results

async def emergency_collect(
    crisis_keywords: List[str],
    geographic_focus: str = None,
    max_events_per_source: int = 200,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Trigger emergency data collection with enhanced parameters
    
    Args:
        crisis_keywords: Keywords related to the crisis
        geographic_focus: Geographic area to focus on
        max_events_per_source: Maximum events to collect per source
        tool_context: ADK tool context
        
    Returns:
        Emergency collection results
    """
    logger.info(f"Emergency collection triggered - Keywords: {crisis_keywords}, Geographic focus: {geographic_focus}")
    
    emergency_start = datetime.utcnow()
    results = {
        "emergency_id": f"emergency_{int(emergency_start.timestamp())}",
        "start_time": emergency_start.isoformat(),
        "crisis_keywords": crisis_keywords,
        "geographic_focus": geographic_focus,
        "sources_processed": [],
        "total_events_collected": 0,
        "high_priority_events": 0,
        "errors": []
    }
    
    try:
        # Enhanced keyword list for emergency
        enhanced_keywords = crisis_keywords + ["emergency", "breaking", "urgent", "critical", "alert"]
        
        # Parallel emergency collection from all sources
        emergency_tasks = [
            fetch_from_gdelt(keywords=enhanced_keywords, max_records=max_events_per_source, tool_context=tool_context),
            fetch_from_twitter(keywords=enhanced_keywords, max_results=max_events_per_source, include_retweets=True, tool_context=tool_context),
            fetch_from_noaa(region=geographic_focus, tool_context=tool_context),
        ]
        
        all_events = []
        
        for i, task in enumerate(emergency_tasks):
            source_names = ["GDELT", "Twitter", "NOAA"]
            source_name = source_names[i]
            
            try:
                source_result = await task
                
                if source_result.get("status") == "success":
                    results["sources_processed"].append(source_name)
                    events = source_result.get("events", [])
                    
                    # Prioritize high-severity events
                    high_priority = [e for e in events if e.get("severity") in ["high", "critical"]]
                    results["high_priority_events"] += len(high_priority)
                    
                    all_events.extend(events)
                    results["total_events_collected"] += len(events)
                else:
                    results["errors"].append(f"{source_name}: {source_result.get('message', 'Failed')}")
                    
            except Exception as e:
                error_msg = f"Emergency collection from {source_name} failed: {str(e)}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
        
        # Immediately publish high-priority events
        if all_events:
            # Sort by severity and publish critical/high events first
            priority_events = sorted(all_events, key=lambda x: {"critical": 4, "high": 3, "medium": 2, "low": 1}.get(x.get("severity", "low"), 1), reverse=True)
            
            publish_result = await publish_to_pubsub(priority_events, batch_size=5, tool_context=tool_context)
            results["publish_result"] = publish_result
        
        # Update emergency status in context
        if tool_context:
            tool_context.state["emergency_active"] = True
            tool_context.state["emergency_keywords"] = crisis_keywords
            tool_context.state["emergency_timestamp"] = emergency_start.isoformat()
            tool_context.state["last_emergency_results"] = results
        
    except Exception as e:
        error_msg = f"Emergency collection orchestration failed: {str(e)}"
        logger.error(error_msg)
        results["errors"].append(error_msg)
    
    results["end_time"] = datetime.utcnow().isoformat()
    results["duration_seconds"] = (datetime.utcnow() - emergency_start).total_seconds()
    
    logger.info(f"Emergency collection completed: {results['total_events_collected']} events, {results['high_priority_events']} high priority")
    
    return results

# Helper functions

def _map_noaa_severity(noaa_severity: str) -> str:
    """Map NOAA severity levels to standard levels"""
    mapping = {
        "extreme": "critical",
        "severe": "high",
        "moderate": "medium", 
        "minor": "low",
        "unknown": "medium"
    }
    return mapping.get(noaa_severity.lower(), "medium")

def _analyze_gdelt_severity(article: Dict[str, Any]) -> str:
    """Analyze GDELT article tone to determine severity"""
    tone = article.get("tone", 0)
    if isinstance(tone, str):
        try:
            tone = float(tone)
        except ValueError:
            tone = 0
    
    if tone < -5:
        return "critical"
    elif tone < -2:
        return "high"
    elif tone < 0:
        return "medium"
    else:
        return "low"

def _analyze_twitter_sentiment(text: str) -> str:
    """Simple sentiment analysis for Twitter text"""
    negative_words = ["crisis", "disaster", "emergency", "critical", "severe", "failure", "collapse", "shutdown"]
    urgent_words = ["breaking", "urgent", "alert", "warning", "immediate"]
    
    text_lower = text.lower()
    
    negative_count = sum(1 for word in negative_words if word in text_lower)
    urgent_count = sum(1 for word in urgent_words if word in text_lower)
    
    if negative_count >= 2 or urgent_count >= 2:
        return "critical"
    elif negative_count >= 1 and urgent_count >= 1:
        return "high"
    elif negative_count >= 1 or urgent_count >= 1:
        return "medium"
    else:
        return "low"

def _calculate_tweet_relevance(text: str, keywords: List[str]) -> int:
    """Calculate relevance score of tweet to supply chain keywords"""
    text_lower = text.lower()
    matches = sum(1 for keyword in keywords if keyword.lower() in text_lower)
    
    # Base score from keyword matches
    score = min(matches * 2, 8)
    
    # Boost for specific supply chain terms
    high_value_terms = ["supply chain", "port", "factory", "shipping", "logistics", "manufacturing"]
    if any(term in text_lower for term in high_value_terms):
        score += 2
    
    return min(score, 10)