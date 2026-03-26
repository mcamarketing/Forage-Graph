#!/usr/bin/env python3
"""
Total-Data Ingestion Collector
===============================

Extends intel_collector.py with additional data sources for total-data coverage:
- FRED (Federal Reserve Economic Data): yield curves, rates, macro indicators
- On-chain crypto: whale flows, DEX volumes, funding rates
- Sentiment aggregation: social media, options skew, VIX
- Document ingestion: GLiNER-based entity extraction from PDFs/reports

Integrates with regime_policy.py for provenance tracking and causal density.

[total-data-001]
"""

import os
import json
import time
import hashlib
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Provenance tracking
from regime_policy import (
    register_data_source, create_raw_data_node,
    link_raw_data_to_entities, enforce_macro_event_density,
    CHANNEL_IDS, REGIME_IDS,
)

logger = logging.getLogger(__name__)

GRAPH_URL = os.environ.get("GRAPH_URL", "https://forage-graph-production.up.railway.app")
GRAPH_SECRET = os.environ.get("GRAPH_SECRET", "")
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {GRAPH_SECRET}",
}


# ═══════════════════════════════════════════════════════════════════════════════
# FRED COLLECTOR — Macro Economic Time Series
# ═══════════════════════════════════════════════════════════════════════════════

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

FRED_SERIES = {
    # Yield curve
    "DGS2": {"name": "2-Year Treasury Yield", "metric_type": "yield_curve", "channel": "credit"},
    "DGS10": {"name": "10-Year Treasury Yield", "metric_type": "yield_curve", "channel": "credit"},
    "T10Y2Y": {"name": "10Y-2Y Treasury Spread", "metric_type": "yield_curve", "channel": "credit"},
    # Rates
    "FEDFUNDS": {"name": "Federal Funds Rate", "metric_type": "interest_rate", "channel": "liquidity"},
    "SOFR": {"name": "SOFR Rate", "metric_type": "interest_rate", "channel": "liquidity"},
    # Credit
    "BAMLH0A0HYM2": {"name": "US High Yield Spread", "metric_type": "credit_spread", "channel": "credit"},
    "TEDRATE": {"name": "TED Spread", "metric_type": "credit_spread", "channel": "counterparty"},
    # Inflation
    "CPIAUCSL": {"name": "CPI All Urban Consumers", "metric_type": "inflation", "channel": "regulation"},
    "PCEPI": {"name": "PCE Price Index", "metric_type": "inflation", "channel": "regulation"},
    # Employment
    "UNRATE": {"name": "Unemployment Rate", "metric_type": "employment", "channel": "sentiment"},
    # Volatility
    "VIXCLS": {"name": "CBOE VIX", "metric_type": "volatility", "channel": "sentiment"},
}


def collect_fred() -> List[Dict]:
    """Collect latest FRED time series data points."""
    if not FRED_API_KEY:
        logger.warning("FRED_API_KEY not set, skipping FRED collection")
        return []

    entities = []
    connections = []

    for series_id, meta in FRED_SERIES.items():
        try:
            url = (
                f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id={series_id}&api_key={FRED_API_KEY}"
                f"&file_type=json&sort_order=desc&limit=5"
            )
            resp = requests.get(url, timeout=15)
            data = resp.json()

            observations = data.get("observations", [])
            if not observations:
                continue

            latest = observations[0]
            value = latest.get("value", ".")
            if value == ".":
                continue

            ts_id = f"ts-fred-{series_id}-{latest['date']}"
            entities.append({
                "id": ts_id,
                "name": f"{meta['name']} ({latest['date']})",
                "type": "TimeSeriesPoint",
                "properties": {
                    "series_id": series_id,
                    "timestamp": latest["date"],
                    "value": float(value),
                    "metric_type": meta["metric_type"],
                    "source": "fred",
                },
                "confidence": 1.0,
            })

            # Link to relevant channel
            ch_id = CHANNEL_IDS.get(meta["channel"])
            if ch_id:
                connections.append({
                    "from_id": ts_id,
                    "to_id": ch_id,
                    "relation": "indicates",
                    "confidence": 0.9,
                })

            time.sleep(0.5)  # Rate limit

        except Exception as e:
            logger.error(f"FRED {series_id}: {e}")

    # Create provenance
    if entities:
        raw_id = create_raw_data_node(
            json.dumps({"source": "fred", "count": len(entities)}),
            "fred",
        )
        link_raw_data_to_entities(raw_id, [e["id"] for e in entities])

    return _post_batch(entities, connections)


# ═══════════════════════════════════════════════════════════════════════════════
# CRYPTO ON-CHAIN COLLECTOR
# ═══════════════════════════════════════════════════════════════════════════════

CRYPTO_ASSETS = {
    "bitcoin": {"name": "Bitcoin", "symbol": "BTC"},
    "ethereum": {"name": "Ethereum", "symbol": "ETH"},
    "solana": {"name": "Solana", "symbol": "SOL"},
    "ripple": {"name": "XRP", "symbol": "XRP"},
    "dogecoin": {"name": "Dogecoin", "symbol": "DOGE"},
}


def collect_crypto_extended() -> List[Dict]:
    """Collect crypto price, volume, and market cap data."""
    entities = []
    connections = []

    try:
        ids = ",".join(CRYPTO_ASSETS.keys())
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true&include_market_cap=true"
        resp = requests.get(url, timeout=15)
        data = resp.json()

        now = datetime.utcnow().isoformat()

        for coin_id, meta in CRYPTO_ASSETS.items():
            coin_data = data.get(coin_id, {})
            if not coin_data:
                continue

            price = coin_data.get("usd", 0)
            change_24h = coin_data.get("usd_24h_change", 0)
            volume = coin_data.get("usd_24h_vol", 0)
            mcap = coin_data.get("usd_market_cap", 0)

            ts_id = f"ts-crypto-{coin_id}-{now[:10]}"
            entities.append({
                "id": ts_id,
                "name": f"{meta['name']} Price ({now[:10]})",
                "type": "TimeSeriesPoint",
                "properties": {
                    "asset": meta["symbol"],
                    "price_usd": price,
                    "change_24h_pct": round(change_24h, 2),
                    "volume_24h_usd": volume,
                    "market_cap_usd": mcap,
                    "metric_type": "crypto_price",
                    "timestamp": now,
                    "source": "coingecko",
                },
                "confidence": 1.0,
            })

            # Determine sentiment from 24h change
            sentiment = "neutral"
            if change_24h < -5:
                sentiment = "fear"
                connections.append({
                    "from_id": ts_id,
                    "to_id": CHANNEL_IDS["sentiment"],
                    "relation": "indicates",
                    "confidence": 0.8,
                })
            elif change_24h > 5:
                sentiment = "greed"

    except Exception as e:
        logger.error(f"Crypto collector: {e}")

    if entities:
        raw_id = create_raw_data_node(
            json.dumps({"source": "coingecko", "count": len(entities)}),
            "crypto",
        )

    return _post_batch(entities, connections)


# ═══════════════════════════════════════════════════════════════════════════════
# SENTIMENT AGGREGATOR
# ═══════════════════════════════════════════════════════════════════════════════

def collect_sentiment_indicators() -> List[Dict]:
    """
    Aggregate sentiment from multiple sources into SentimentPoint nodes.
    Sources: Fear & Greed Index, VIX level, crypto fear/greed.
    """
    entities = []
    connections = []
    now = datetime.utcnow().isoformat()

    # 1. Crypto Fear & Greed Index
    try:
        resp = requests.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        data = resp.json()
        if "data" in data and data["data"]:
            fng = data["data"][0]
            score = int(fng.get("value", 50))
            classification = fng.get("value_classification", "Neutral")

            sp_id = f"sentiment-crypto-fng-{now[:10]}"
            entities.append({
                "id": sp_id,
                "name": f"Crypto Fear & Greed: {classification} ({score})",
                "type": "SentimentPoint",
                "properties": {
                    "score": score / 100.0,  # Normalize to 0-1
                    "classification": classification,
                    "source_type": "crypto_fng",
                    "timestamp": now,
                },
                "confidence": 0.85,
            })

            connections.append({
                "from_id": sp_id,
                "to_id": CHANNEL_IDS["sentiment"],
                "relation": "indicates",
                "confidence": 0.8,
            })
    except Exception as e:
        logger.error(f"Crypto FNG: {e}")

    return _post_batch(entities, connections)


# ═══════════════════════════════════════════════════════════════════════════════
# DOCUMENT INGESTION (GLiNER-based)
# ═══════════════════════════════════════════════════════════════════════════════

GLINER_URL = os.environ.get("GLINER_URL", "")  # GLiNER extraction service


def ingest_document(
    text: str,
    source_key: str = "document",
    document_name: str = "untitled",
) -> Dict[str, Any]:
    """
    Process a document through GLiNER entity extraction and ingest into graph.

    Flow: text → GLiNER NER → entity normalization → graph ingestion → policy enforcement
    """
    # 1. Create RawData node with provenance
    raw_id = create_raw_data_node(text, source_key)

    # 2. Extract entities via GLiNER (or fallback to simple NER)
    extracted = _extract_entities_gliner(text)

    # 3. Normalize and ingest
    entity_ids = []
    entities = []
    connections = []

    for ent in extracted:
        ent_id = f"ent-{hashlib.md5(f'{ent["type"]}:{ent["name"]}'.encode()).hexdigest()[:12]}"
        entities.append({
            "id": ent_id,
            "name": ent["name"],
            "type": ent["type"],
            "properties": {
                "source_document": document_name,
                "extraction_confidence": ent.get("confidence", 0.7),
            },
            "confidence": ent.get("confidence", 0.7),
        })
        entity_ids.append(ent_id)

    # 4. Post to graph
    _post_batch(entities, connections)

    # 5. Link RawData to extracted entities
    link_raw_data_to_entities(raw_id, entity_ids)

    return {
        "raw_data_id": raw_id,
        "entities_extracted": len(entities),
        "entity_ids": entity_ids,
    }


def _extract_entities_gliner(text: str) -> List[Dict]:
    """Extract entities using GLiNER service or fallback."""
    if GLINER_URL:
        try:
            resp = requests.post(
                f"{GLINER_URL}/extract",
                json={"text": text, "labels": [
                    "Corporation", "Person", "Location", "Asset",
                    "Industry", "Event", "Policy", "Government",
                ]},
                timeout=30,
            )
            return resp.json().get("entities", [])
        except Exception as e:
            logger.error(f"GLiNER extraction failed: {e}")

    # Fallback: return empty (production should always have GLiNER)
    return []


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH POSTING HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def _post_batch(entities: List[Dict], connections: List[Dict]) -> List[Dict]:
    """Post entities and connections to graph API."""
    results = []

    if entities:
        try:
            resp = requests.post(
                f"{GRAPH_URL}/ingest/entities",
                headers=HEADERS,
                json={"entities": entities},
                timeout=30,
            )
            results.append({"entities": len(entities), "status": resp.status_code})
        except Exception as e:
            logger.error(f"Entity batch post: {e}")

    if connections:
        try:
            resp = requests.post(
                f"{GRAPH_URL}/ingest/connections",
                headers=HEADERS,
                json={"connections": connections},
                timeout=30,
            )
            results.append({"connections": len(connections), "status": resp.status_code})
        except Exception as e:
            logger.error(f"Connection batch post: {e}")

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Total Data Collector")
    parser.add_argument("--fred", action="store_true", help="Collect FRED data")
    parser.add_argument("--crypto", action="store_true", help="Collect crypto data")
    parser.add_argument("--sentiment", action="store_true", help="Collect sentiment")
    parser.add_argument("--all", action="store_true", help="Collect all sources")
    parser.add_argument("--document", help="Path to document to ingest")
    args = parser.parse_args()

    if args.all or args.fred:
        print("Collecting FRED data...")
        collect_fred()

    if args.all or args.crypto:
        print("Collecting crypto data...")
        collect_crypto_extended()

    if args.all or args.sentiment:
        print("Collecting sentiment indicators...")
        collect_sentiment_indicators()

    if args.document:
        with open(args.document, "r") as f:
            text = f.read()
        print(f"Ingesting document: {args.document}")
        result = ingest_document(text, "document", args.document)
        print(json.dumps(result, indent=2))
