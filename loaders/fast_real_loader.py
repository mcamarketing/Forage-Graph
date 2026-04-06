#!/usr/bin/env python3
"""
Fast Real Data Loader - Uses real APIs to populate graph with actual entities + relationships
Based on global_reality_graph_loaders spec
"""

import os
import json
import time
import random
import requests
from typing import List, Dict, Any

GRAPH_URL = os.environ.get(
    "GRAPH_URL", "https://forage-graph-production.up.railway.app"
)
GRAPH_SECRET = os.environ['GRAPH_API_SECRET']

HEADERS = {
    "Authorization": f"Bearer {GRAPH_SECRET}",
    "Content-Type": "application/json",
}

BATCH_SIZE = 30


def fetch_crypto_data() -> Dict:
    """Fetch real crypto data from CoinGecko"""
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 50,
                "page": 1,
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            return {"success": True, "data": resp.json()}
    except Exception as e:
        print(f"  CoinGecko error: {e}")
    return {"success": False, "data": []}


def fetch_countries() -> Dict:
    """Fetch country data"""
    try:
        resp = requests.get(
            "https://restcountries.com/v3.1/all?fields=name,cca2,region,subregion",
            timeout=10,
        )
        if resp.status_code == 200:
            return {"success": True, "data": resp.json()}
    except Exception as e:
        print(f"  Countries API error: {e}")
    return {"success": False, "data": []}


def create_batch_from_crypto(crypto_data: List[Dict]) -> Dict:
    """Create batch from crypto data with relationships"""
    entities = []
    connections = []

    for coin in crypto_data[:30]:
        # Token entity
        entities.append(
            {
                "type": "FinancialInstrument",
                "name": coin.get("name", ""),
                "properties": {
                    "symbol": coin.get("symbol", "").upper(),
                    "market_cap": coin.get("market_cap"),
                    "price": coin.get("current_price"),
                    "volume_24h": coin.get("total_volume"),
                    "rank": coin.get("market_cap_rank"),
                    "source": "coingecko",
                },
                "source": "crypto_loader",
                "confidence": 0.85,
            }
        )

        # Market relationship
        connections.append(
            {
                "from_type": "FinancialInstrument",
                "from_name": coin.get("name", ""),
                "to_type": "Market",
                "to_name": "Crypto Markets",
                "relation": "traded_in",
                "properties": {},
                "source": "crypto_loader",
                "confidence": 0.8,
            }
        )

        # Category relationship
        if coin.get("categories"):
            cat = coin["categories"][0] if coin["categories"] else "Cryptocurrency"
            connections.append(
                {
                    "from_type": "FinancialInstrument",
                    "from_name": coin.get("name", ""),
                    "to_type": "Industry",
                    "to_name": cat,
                    "relation": "in_category",
                    "properties": {},
                    "source": "crypto_loader",
                    "confidence": 0.75,
                }
            )

    # Add some exchange entities
    exchanges = [
        "Binance",
        "Coinbase",
        "Kraken",
        "KuCoin",
        "Bybit",
        "OKX",
        "Gemini",
        "Bitfinex",
    ]
    for exch in exchanges:
        entities.append(
            {
                "type": "FinancialInstitution",
                "name": exch,
                "properties": {"type": "Crypto Exchange", "source": "coingecko"},
                "source": "crypto_loader",
                "confidence": 0.9,
            }
        )

        for coin in crypto_data[:10]:
            connections.append(
                {
                    "from_type": "FinancialInstrument",
                    "from_name": coin.get("name", ""),
                    "to_type": "FinancialInstitution",
                    "to_name": exch,
                    "relation": "traded_on",
                    "properties": {},
                    "source": "crypto_loader",
                    "confidence": 0.7,
                }
            )

    return {"entities": entities, "connections": connections}


def create_batch_from_countries(country_data: List[Dict]) -> Dict:
    """Create batch from country data with relationships"""
    entities = []
    connections = []

    for country in country_data[:50]:
        name = country.get("name", {}).get("common", "")
        code = country.get("cca2", "")
        region = country.get("region", "Unknown")
        subregion = country.get("subregion", "Unknown")

        if not name or not code:
            continue

        # Country entity
        entities.append(
            {
                "type": "Location",
                "name": name,
                "properties": {
                    "iso_code": code,
                    "region": region,
                    "subregion": subregion,
                    "source": "restcountries",
                },
                "source": "country_loader",
                "confidence": 0.95,
            }
        )

        # Region relationship
        if region and region != "Unknown":
            connections.append(
                {
                    "from_type": "Location",
                    "from_name": name,
                    "to_type": "Jurisdiction",
                    "to_name": region,
                    "relation": "in_region",
                    "properties": {},
                    "source": "country_loader",
                    "confidence": 0.9,
                }
            )

        # Subregion relationship
        if subregion and subregion != "Unknown":
            connections.append(
                {
                    "from_type": "Location",
                    "from_name": name,
                    "to_type": "Location",
                    "to_name": subregion,
                    "relation": "in_subregion",
                    "properties": {},
                    "source": "country_loader",
                    "confidence": 0.85,
                }
            )

    # Add region entities
    regions = list(set([c.get("region") for c in country_data if c.get("region")]))
    for region in regions:
        if region and region != "Unknown":
            entities.append(
                {
                    "type": "Jurisdiction",
                    "name": region,
                    "properties": {"type": "UN Region", "source": "restcountries"},
                    "source": "country_loader",
                    "confidence": 0.9,
                }
            )

    return {"entities": entities, "connections": connections}


def post_batch(batch: Dict) -> Dict:
    """POST batch to graph API"""
    try:
        resp = requests.post(
            f"{GRAPH_URL}/ingest/bulk", headers=HEADERS, json=batch, timeout=30
        )
        if resp.status_code == 201:
            return resp.json()
        elif resp.status_code == 429:
            time.sleep(3)
            return {"error": "rate_limited"}
        else:
            return {"error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
    except Exception as e:
        return {"error": str(e)}


def run_loaders(num_rounds: int = 10):
    """Run multiple rounds of loading"""
    total_entities = 0
    total_connections = 0
    errors = 0

    print(f"Starting fast real data loaders ({num_rounds} rounds)")

    for round_num in range(num_rounds):
        # Load crypto data
        print(f"\n--- Round {round_num + 1}: Crypto Data ---")
        crypto_result = fetch_crypto_data()
        if crypto_result["success"]:
            batch = create_batch_from_crypto(crypto_result["data"])
            result = post_batch(batch)
            if "error" in result:
                errors += 1
                print(f"  Error: {result['error']}")
            else:
                ent = result.get("entities", {})
                conn = result.get("connections", {})
                total_entities += ent.get("added", 0) + ent.get("merged", 0)
                total_connections += conn.get("added", 0) + conn.get("merged", 0)
                print(
                    f"  +{ent.get('added', 0)} entities, +{conn.get('added', 0)} connections"
                )

        time.sleep(1)

        # Load countries
        print(f"\n--- Round {round_num + 1}: Countries ---")
        country_result = fetch_countries()
        if country_result["success"]:
            batch = create_batch_from_countries(country_result["data"])
            result = post_batch(batch)
            if "error" in result:
                errors += 1
                print(f"  Error: {result['error']}")
            else:
                ent = result.get("entities", {})
                conn = result.get("connections", {})
                total_entities += ent.get("added", 0) + ent.get("merged", 0)
                total_connections += conn.get("added", 0) + conn.get("merged", 0)
                print(
                    f"  +{ent.get('added', 0)} entities, +{conn.get('added', 0)} connections"
                )

        time.sleep(1)

    print(f"\n=== Load Complete ===")
    print(f"  Entities added: {total_entities}")
    print(f"  Connections added: {total_connections}")
    print(f"  Errors: {errors}")


if __name__ == "__main__":
    import sys

    rounds = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    run_loaders(rounds)
