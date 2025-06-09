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
