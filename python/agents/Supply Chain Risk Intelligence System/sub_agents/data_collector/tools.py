# Copyright 2025 Google LLC - ADK State Compatible Version

"""
Data Collector Sub-Agent Tools - Fixed for ADK State compatibility

This version properly handles ADK's State object which doesn't have setdefault method.
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from dataclasses import dataclass, asdict

import aiohttp
import requests
import tweepy
import geojson
from google.cloud import pubsub_v1
from google.adk.tools import ToolContext
from dotenv import load_dotenv

from .prompts import get_supply_chain_keywords

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "raw_events")

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

@dataclass
class SupplyChainEvent:
    """Normalized supply chain event data structure"""
    source: str
    event_type: str
    timestamp: datetime
    location: dict = None
    severity: str = "medium"
    description: str = ""
    metadata: dict = None
    raw_data: dict = None
    geojson: dict = None
    impact_score: float = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.raw_data is None:
            self.raw_data = {}

def _update_state_safely(tool_context: ToolContext, key: str, value: any):
    """Safely update ADK state without using setdefault"""
    if tool_context and hasattr(tool_context, 'state'):
        try:
            # Check if key exists, if not initialize it
            if not hasattr(tool_context.state, key) or getattr(tool_context.state, key) is None:
                setattr(tool_context.state, key, {})
            
            # Get current value or empty dict
            current = getattr(tool_context.state, key, {})
            if not isinstance(current, dict):
                current = {}
            
            # Update the value
            if isinstance(value, dict):
                current.update(value)
                setattr(tool_context.state, key, current)
            else:
                setattr(tool_context.state, key, value)
                
        except Exception as e:
            logger.warning(f"Failed to update state {key}: {e}")

def _update_api_status(tool_context: ToolContext, api_name: str, status: str):
    """Safely update API status in state"""
    if tool_context:
        try:
            # Get current api_status or create new
            api_status = getattr(tool_context.state, 'api_status', {})
            if not isinstance(api_status, dict):
                api_status = {}
            
            # Update status
            api_status[api_name] = status
            setattr(tool_context.state, 'api_status', api_status)
            
        except Exception as e:
            logger.warning(f"Failed to update API status for {api_name}: {e}")

def _update_collection_stats(tool_context: ToolContext, stat_name: str, value: any):
    """Safely update collection stats in state"""
    if tool_context:
        try:
            # Get current collection_stats or create new
            stats = getattr(tool_context.state, 'collection_stats', {})
            if not isinstance(stats, dict):
                stats = {}
            
            # Update stat
            stats[stat_name] = value
            setattr(tool_context.state, 'collection_stats', stats)
            
        except Exception as e:
            logger.warning(f"Failed to update collection stat {stat_name}: {e}")

async def fetch_from_noaa(
    alert_types: str = "all",
    region: str = "",
    tool_context: ToolContext = None
):
    """
    Fetch weather alerts and typhoon data from NOAA API
    """
    logger.info(f"Fetching NOAA data - Alert types: {alert_types}, Region: {region}")
    
    events = []
    
    try:
        # NOAA Weather Alerts API
        url = "https://api.weather.gov/alerts/active"
        headers = {"User-Agent": "SupplyChainAgent/1.0"}
        
        params = {}
        if region and region != "global":
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
                            
                            event = SupplyChainEvent(
                                source="NOAA",
                                event_type=f"weather_{event_type.replace(' ', '_')}",
                                timestamp=datetime.utcnow(),
                                location=location,
                                severity=_map_noaa_severity(properties.get("severity", "unknown")),
                                description=properties.get("headline", "Weather alert"),
                                metadata={
                                    "urgency": properties.get("urgency"),
                                    "certainty": properties.get("certainty"),
                                    "areas": properties.get("areaDesc"),
                                },
                                raw_data=properties
                            )
                            events.append(event)
                    
                    logger.info(f"Collected {len(events)} weather events from NOAA")
                else:
                    logger.error(f"NOAA API request failed with status {response.status}")
                    _update_api_status(tool_context, "NOAA", "error")
                    return {"status": "error", "message": f"API request failed: {response.status}"}
        
        # Update tool context safely
        _update_api_status(tool_context, "NOAA", "connected")
        _update_collection_stats(tool_context, "noaa_events", len(events))
        
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
        _update_api_status(tool_context, "NOAA", "error")
        return {"status": "error", "message": error_msg}

async def fetch_from_gdelt(
    timespan: str = "24h",
    max_records: int = 100,
    tool_context: ToolContext = None
):
    """
    Fetch news sentiment and event data from GDELT
    """
    keywords = get_supply_chain_keywords()
    logger.info(f"Fetching GDELT data - Keywords: {len(keywords)} keywords, Timespan: {timespan}")
    
    events = []
    
    try:
        # GDELT Event Database API with simplified query
        base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
        
        # Use simpler query to avoid HTML response
        query_terms = "supply chain OR semiconductor OR chip shortage"
        
        params = {
            "query": query_terms,
            "mode": "ArtList",
            "format": "json",
            "timespan": timespan,
            "maxrecords": str(min(max_records, 100)),  # Limit to avoid API issues
            "sort": "DateDesc"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params, timeout=10) as response:
                if response.status == 200:
                    content_type = response.headers.get('content-type', '')
                    if 'application/json' in content_type:
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
                                timestamp=datetime.utcnow(),
                                location=location,
                                severity=_analyze_gdelt_severity(article),
                                description=article.get("title", "Supply chain news event"),
                                metadata={
                                    "url": article.get("url"),
                                    "domain": article.get("domain"),
                                    "language": article.get("language"),
                                    "tone": article.get("tone"),
                                },
                                raw_data=article
                            )
                            events.append(event)
                        
                        logger.info(f"Collected {len(events)} news events from GDELT")
                    else:
                        logger.warning(f"GDELT returned HTML instead of JSON, content-type: {content_type}")
                        _update_api_status(tool_context, "GDELT", "api_limit")
                        return {"status": "error", "message": "GDELT API returned HTML (possible rate limit)"}
                        
                elif response.status == 429:
                    logger.warning("GDELT API rate limit exceeded")
                    _update_api_status(tool_context, "GDELT", "rate_limited")
                    return {"status": "error", "message": "GDELT API rate limit exceeded"}
                else:
                    logger.error(f"GDELT API request failed with status {response.status}")
                    _update_api_status(tool_context, "GDELT", "error")
                    return {"status": "error", "message": f"API request failed: {response.status}"}
        
        # Update tool context safely
        _update_api_status(tool_context, "GDELT", "connected")
        _update_collection_stats(tool_context, "gdelt_events", len(events))
        
        return {
            "status": "success",
            "source": "GDELT",
            "events_collected": len(events),
            "events": [asdict(event) for event in events],
            "search_keywords": query_terms,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except asyncio.TimeoutError:
        error_msg = "GDELT API request timed out"
        logger.error(error_msg)
        _update_api_status(tool_context, "GDELT", "timeout")
        return {"status": "error", "message": error_msg}
    except Exception as e:
        error_msg = f"Error fetching GDELT data: {str(e)}"
        logger.error(error_msg)
        _update_api_status(tool_context, "GDELT", "error")
        return {"status": "error", "message": error_msg}

async def fetch_from_marinetraffic(
    tool_context: ToolContext = None
):
    """
    Fetch shipping and logistics data from MarineTraffic API
    """
    logger.info("Fetching MarineTraffic data")
    
    if not MARINETRAFFIC_API_KEY:
        logger.warning("MarineTraffic API key not configured")
        _update_api_status(tool_context, "MarineTraffic", "not_configured")
        return {"status": "error", "message": "MarineTraffic API key not configured"}
    
    events = []
    
    try:
        # Major ports to monitor
        target_ports = [
            {"name": "Shanghai", "lat": 31.2304, "lon": 121.4737},
            {"name": "Singapore", "lat": 1.2966, "lon": 103.7764},
            {"name": "Rotterdam", "lat": 51.9244, "lon": 4.4777},
        ]
        
        for port in target_ports[:2]:  # Limit to 2 ports to avoid rate limits
            url = f"https://services.marinetraffic.com/api/exportvessels/{MARINETRAFFIC_API_KEY}/v:3/protocol:jsono"
            
            params = {
                "timespan": "60",
                "minlat": port["lat"] - 0.1,
                "maxlat": port["lat"] + 0.1,
                "minlon": port["lon"] - 0.1,
                "maxlon": port["lon"] + 0.1
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Analyze vessel congestion
                        vessel_count = len(data) if data else 0
                        
                        # Create congestion event if threshold exceeded
                        if vessel_count > 50:
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
                                    "congestion_threshold": 50,
                                },
                                raw_data={"vessels": data[:5] if data else []}
                            )
                            events.append(event)
                    
                    elif response.status == 401:
                        logger.error("MarineTraffic API authentication failed")
                        _update_api_status(tool_context, "MarineTraffic", "auth_failed")
                        return {"status": "error", "message": "API authentication failed"}
                    else:
                        logger.warning(f"MarineTraffic API request failed for {port['name']}: {response.status}")
            
            # Rate limiting
            await asyncio.sleep(1)
        
        # Update tool context safely
        _update_api_status(tool_context, "MarineTraffic", "connected")
        _update_collection_stats(tool_context, "marinetraffic_events", len(events))
        
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
        _update_api_status(tool_context, "MarineTraffic", "error")
        return {"status": "error", "message": error_msg}

async def fetch_from_fred(
    change_threshold: float = 2.0,
    tool_context: ToolContext = None
):
    """
    Fetch economic indicators from FRED API
    """
    logger.info(f"Fetching FRED data - Change threshold: {change_threshold}%")
    
    if not FRED_API_KEY:
        logger.warning("FRED API key not configured")
        _update_api_status(tool_context, "FRED", "not_configured")
        return {"status": "error", "message": "FRED API key not configured"}
    
    events = []
    
    # Default economic indicators for supply chain
    default_indicators = [
        {"series_id": "CPIAUCSL", "name": "Consumer Price Index"},
        {"series_id": "UNRATE", "name": "Unemployment Rate"},
    ]
    
    try:
        base_url = "https://api.stlouisfed.org/fred/series/observations"
        
        for indicator in default_indicators:
            params = {
                "series_id": indicator["series_id"],
                "api_key": FRED_API_KEY,
                "file_type": "json",
                "limit": "5",
                "sort_order": "desc"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(base_url, params=params, timeout=10) as response:
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
                                        timestamp=datetime.utcnow(),
                                        location={"lat": 39.8283, "lon": -98.5795},
                                        severity="high" if abs(change_pct) > 5.0 else "medium",
                                        description=f"Significant change in {indicator['name']}: {change_pct:.2f}%",
                                        metadata={
                                            "indicator_name": indicator["name"],
                                            "series_id": indicator["series_id"],
                                            "current_value": current,
                                            "previous_value": previous,
                                            "change_percent": change_pct,
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
            
            await asyncio.sleep(0.5)
        
        # Update tool context safely
        _update_api_status(tool_context, "FRED", "connected")
        _update_collection_stats(tool_context, "fred_events", len(events))
        
        logger.info(f"Collected {len(events)} economic events from FRED")
        
        return {
            "status": "success",
            "source": "FRED",
            "events_collected": len(events),
            "events": [asdict(event) for event in events],
            "indicators_checked": len(default_indicators),
            "change_threshold": change_threshold,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        error_msg = f"Error fetching FRED data: {str(e)}"
        logger.error(error_msg)
        _update_api_status(tool_context, "FRED", "error")
        return {"status": "error", "message": error_msg}

async def fetch_from_twitter(
    max_results: int = 100,
    include_retweets: bool = False,
    tool_context: ToolContext = None
):
    """
    Fetch real-time social signals from Twitter API v2
    """
    logger.info(f"Fetching Twitter data - Max results: {max_results}")
    
    if not TWITTER_BEARER_TOKEN or twitter_client is None:
        logger.warning("Twitter Bearer Token not configured or client not initialized")
        _update_api_status(tool_context, "Twitter", "not_configured")
        return {"status": "error", "message": "Twitter Bearer Token not configured"}
    
    events = []
    
    try:
        # Simple search query
        query = "supply chain OR semiconductor"
        
        if not include_retweets:
            query += " -is:retweet"
        
        # Recent search
        tweets = tweepy.Paginator(
            twitter_client.search_recent_tweets,
            query=query,
            max_results=min(max_results, 50),  # Limit to avoid rate limits
            tweet_fields=["created_at", "author_id", "public_metrics"],
        ).flatten(limit=min(max_results, 50))
        
        tweet_count = 0
        for tweet in tweets:
            tweet_count += 1
            
            # Analyze sentiment and relevance
            severity = _analyze_twitter_sentiment(tweet.text)
            
            # Calculate relevance score
            relevance_score = _calculate_tweet_relevance(tweet.text, ["supply chain", "semiconductor"])
            
            # Only include highly relevant tweets
            if relevance_score >= 3:  # Lower threshold
                event = SupplyChainEvent(
                    source="Twitter",
                    event_type="social_signal",
                    timestamp=tweet.created_at,
                    severity=severity,
                    description=tweet.text[:200] + "..." if len(tweet.text) > 200 else tweet.text,
                    metadata={
                        "tweet_id": tweet.id,
                        "author_id": tweet.author_id,
                        "relevance_score": relevance_score,
                    },
                    raw_data={"text": tweet.text, "created_at": tweet.created_at.isoformat()}
                )
                events.append(event)
        
        # Update tool context safely
        _update_api_status(tool_context, "Twitter", "connected")
        _update_collection_stats(tool_context, "twitter_events", len(events))
        
        logger.info(f"Collected {len(events)} relevant social signals from {tweet_count} tweets")
        
        return {
            "status": "success",
            "source": "Twitter",
            "events_collected": len(events),
            "events": [asdict(event) for event in events],
            "tweets_processed": tweet_count,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        error_msg = f"Error fetching Twitter data: {str(e)}"
        logger.error(error_msg)
        _update_api_status(tool_context, "Twitter", "error")
        return {"status": "error", "message": error_msg}

async def normalize_to_geojson(
    event_data: dict,
    tool_context: ToolContext = None
):
    """Convert event data to GeoJSON format"""
    logger.info("Converting event data to GeoJSON format")
    
    try:
        geometry = event_data.get("geometry")
        properties = event_data.get("properties", {})
        
        if not geometry:
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
        
        geojson_feature = geojson.Feature(
            geometry=geometry,
            properties={
                **properties,
                "timestamp": datetime.utcnow().isoformat(),
                "source": event_data.get("source", "unknown"),
                "event_type": event_data.get("event_type", "unknown")
            }
        )
        
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
    events: list,
    batch_size: int = 10,
    tool_context: ToolContext = None
):
    """Publish events to Google Cloud Pub/Sub topic"""
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
        
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            
            for event in batch:
                try:
                    if not isinstance(event, dict):
                        event = asdict(event) if hasattr(event, '__dict__') else event
                    
                    if 'timestamp' in event and hasattr(event['timestamp'], 'isoformat'):
                        event['timestamp'] = event['timestamp'].isoformat()
                    
                    message_data = json.dumps(event, default=str).encode("utf-8")
                    future = publisher.publish(topic_path, message_data)
                    
                    published_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to publish event: {str(e)}")
                    failed_count += 1
            
            if i + batch_size < len(events):
                await asyncio.sleep(0.1)
        
        # Update tool context safely
        _update_collection_stats(tool_context, "total_published", published_count)
        _update_collection_stats(tool_context, "total_failed", failed_count)
        
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
):
    """Orchestrate data collection from all configured sources"""
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
        active_sources = ["NOAA", "GDELT", "FRED", "Twitter"]  # Removed MarineTraffic for now
    else:
        active_sources = [s.strip() for s in sources.split(",")]
    
    try:
        all_events = []
        
        # Collect from each source sequentially to avoid overwhelming APIs
        for source_name in active_sources:
            try:
                if source_name == "NOAA":
                    result = await fetch_from_noaa(tool_context=tool_context)
                elif source_name == "GDELT":
                    result = await fetch_from_gdelt(tool_context=tool_context)
                elif source_name == "MarineTraffic":
                    result = await fetch_from_marinetraffic(tool_context=tool_context)
                elif source_name == "FRED":
                    result = await fetch_from_fred(tool_context=tool_context)
                elif source_name == "Twitter":
                    max_tweets = 200 if emergency_mode else 100
                    result = await fetch_from_twitter(max_results=max_tweets, tool_context=tool_context)
                else:
                    continue
                
                results["source_results"][source_name] = result
                
                if result.get("status") == "success":
                    results["sources_processed"].append(source_name)
                    events = result.get("events", [])
                    all_events.extend(events)
                    results["total_events_collected"] += len(events)
                else:
                    error_msg = f"{source_name}: {result.get('message', 'Unknown error')}"
                    results["errors"].append(error_msg)
                    
            except Exception as e:
                error_msg = f"Error collecting from {source_name}: {str(e)}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
            
            # Small delay between sources
            await asyncio.sleep(0.5)
        
        # Publish all collected events to Pub/Sub
        if all_events:
            publish_result = await publish_to_pubsub(all_events, tool_context=tool_context)
            results["publish_result"] = publish_result
        
        # Update tool context safely
        if tool_context:
            setattr(tool_context.state, "last_collection_results", results)
            setattr(tool_context.state, "last_collection_timestamp", collection_start.isoformat())
            
            # Update overall statistics
            try:
                collector_stats = getattr(tool_context.state, 'data_collector', {})
                if not isinstance(collector_stats, dict):
                    collector_stats = {}
                
                collector_stats["total_collections"] = collector_stats.get("total_collections", 0) + 1
                collector_stats["successful_collections"] = collector_stats.get("successful_collections", 0) + (1 if results["sources_processed"] else 0)
                collector_stats["total_events_published"] = collector_stats.get("total_events_published", 0) + results["total_events_collected"]
                collector_stats["last_collection_time"] = collection_start.isoformat()
                
                setattr(tool_context.state, 'data_collector', collector_stats)
            except Exception as e:
                logger.warning(f"Failed to update collector stats: {e}")
        
    except Exception as e:
        error_msg = f"Error in collection orchestration: {str(e)}"
        logger.error(error_msg)
        results["errors"].append(error_msg)
    
    results["end_time"] = datetime.utcnow().isoformat()
    results["duration_seconds"] = (datetime.utcnow() - collection_start).total_seconds()
    
    logger.info(f"Collection completed: {results['total_events_collected']} events from {len(results['sources_processed'])} sources")
    
    return results

async def emergency_collect(
    crisis_keywords: list,
    geographic_focus: str = "",
    max_events_per_source: int = 200,
    tool_context: ToolContext = None
):
    """Trigger emergency data collection with enhanced parameters"""
    logger.info(f"Emergency collection triggered - Keywords: {len(crisis_keywords)} keywords, Geographic focus: {geographic_focus}")
    
    emergency_start = datetime.utcnow()
    results = {
        "emergency_id": f"emergency_{int(emergency_start.timestamp())}",
        "start_time": emergency_start.isoformat(),
        "crisis_keywords": crisis_keywords[:10],  # Limit keywords to avoid issues
        "geographic_focus": geographic_focus,
        "sources_processed": [],
        "total_events_collected": 0,
        "high_priority_events": 0,
        "errors": []
    }
    
    try:
        all_events = []
        
        # Collect from GDELT with enhanced focus
        try:
            result = await fetch_from_gdelt(max_records=max_events_per_source, tool_context=tool_context)
            if result.get("status") == "success":
                results["sources_processed"].append("GDELT")
                events = result.get("events", [])
                high_priority = [e for e in events if e.get("severity") in ["high", "critical"]]
                results["high_priority_events"] += len(high_priority)
                all_events.extend(events)
                results["total_events_collected"] += len(events)
            else:
                results["errors"].append(f"GDELT: {result.get('message', 'Failed')}")
        except Exception as e:
            results["errors"].append(f"GDELT: {str(e)}")
        
        # Collect from Twitter with enhanced focus
        try:
            result = await fetch_from_twitter(max_results=max_events_per_source, include_retweets=True, tool_context=tool_context)
            if result.get("status") == "success":
                results["sources_processed"].append("Twitter")
                events = result.get("events", [])
                high_priority = [e for e in events if e.get("severity") in ["high", "critical"]]
                results["high_priority_events"] += len(high_priority)
                all_events.extend(events)
                results["total_events_collected"] += len(events)
            else:
                results["errors"].append(f"Twitter: {result.get('message', 'Failed')}")
        except Exception as e:
            results["errors"].append(f"Twitter: {str(e)}")
        
        # Collect from NOAA for weather-related emergencies
        try:
            result = await fetch_from_noaa(region=geographic_focus, tool_context=tool_context)
            if result.get("status") == "success":
                results["sources_processed"].append("NOAA")
                events = result.get("events", [])
                high_priority = [e for e in events if e.get("severity") in ["high", "critical"]]
                results["high_priority_events"] += len(high_priority)
                all_events.extend(events)
                results["total_events_collected"] += len(events)
            else:
                results["errors"].append(f"NOAA: {result.get('message', 'Failed')}")
        except Exception as e:
            results["errors"].append(f"NOAA: {str(e)}")
        
        # Immediately publish high-priority events
        if all_events:
            # Sort by severity and publish critical/high events first
            priority_events = sorted(all_events, key=lambda x: {"critical": 4, "high": 3, "medium": 2, "low": 1}.get(x.get("severity", "low"), 1), reverse=True)
            
            publish_result = await publish_to_pubsub(priority_events, batch_size=5, tool_context=tool_context)
            results["publish_result"] = publish_result
        
        # Update emergency status in context safely
        if tool_context:
            try:
                setattr(tool_context.state, "emergency_active", True)
                setattr(tool_context.state, "emergency_keywords", crisis_keywords[:10])
                setattr(tool_context.state, "emergency_timestamp", emergency_start.isoformat())
                setattr(tool_context.state, "last_emergency_results", results)
            except Exception as e:
                logger.warning(f"Failed to update emergency state: {e}")
        
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

def _analyze_gdelt_severity(article: dict) -> str:
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

def _calculate_tweet_relevance(text: str, keywords: list) -> int:
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