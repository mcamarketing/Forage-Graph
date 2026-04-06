#!/usr/bin/env python3
"""
Direct bulk loader for FalkorDB Cloud.
Migrated from Railway - same loaders but targeting FalkorDB Cloud.
"""

import os
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# FalkorDB Cloud config
CLOUD_INSTANCE_ID = "instance-dyps0zi57"
CLOUD_PASSWORD = "13129208@"
# Real connection details from user's dashboard
CLOUD_HOST = (
    "r-6jissuruar.instance-dyps0zi57.hc-7up0crkyn.ap-south-1.aws.f2e0a955bb84.cloud"
)
CLOUD_PORT = "55750"

# No browser URL for this custom host - need to use Redis directly
BROWSER_URL = None


# Connect via Redis (FalkorDB uses Redis protocol)
import redis


def get_redis_client():
    """Get Redis client for FalkorDB Cloud."""
    try:
        r = redis.Redis(
            host=CLOUD_HOST,
            port=int(CLOUD_PORT),
            password=CLOUD_PASSWORD,
            decode_responses=True,
            socket_connect_timeout=30,
            socket_timeout=30,
        )
        r.ping()
        print(f"Connected to FalkorDB Cloud via Redis")
        return r
    except Exception as e:
        print(f"Redis connection error: {e}")
        return None


r = get_redis_client()
if r:
    print(f"FalkorDB Cloud ready: {CLOUD_HOST}:{CLOUD_PORT}")
else:
    print("Warning: Could not connect to FalkorDB Cloud")

HEADERS = {"Content-Type": "application/json"}

BATCH_DELAY = 0.15  # 150ms between batches


def post_batch(batch):
    """Post batch to FalkorDB Cloud via Redis protocol."""
    if not r:
        return {"error": "Not connected to Redis"}

    try:
        # Use GRAPH.QUERY command via Redis
        graph_name = "forage_graph"

        results = {"entities": 0, "connections": 0}

        # Add entities
        for ent in batch.get("entities", []):
            name = ent.get("name", "").replace("'", "\\'")
            etype = ent.get("type", "Entity")
            props = json.dumps(ent.get("properties", {}))

            query = f"GRAPH.QUERY {graph_name} \"MERGE (n:{etype} {{name: '{name}'}}) SET n = {{props: '{props}', source: '{ent.get('source', 'bulk')}'}}\""
            try:
                r.execute_command(
                    "GRAPH.QUERY",
                    graph_name,
                    f"MERGE (n:{etype} {{name: '{name}'}}) SET n.props = '{props}', n.source = '{ent.get('source', 'bulk')}'",
                )
                results["entities"] += 1
            except Exception as e:
                pass  # Ignore individual errors

        # Add relationships
        for conn in batch.get("connections", []):
            from_name = conn.get("from_name", "").replace("'", "\\'")
            to_name = conn.get("to_name", "").replace("'", "\\'")
            rel = conn.get("relation", "RELATED")

            try:
                r.execute_command(
                    "GRAPH.QUERY",
                    graph_name,
                    f"MATCH (a {{name: '{from_name}'}}), (b {{name: '{to_name}'}}) MERGE (a)-[r:{rel}]->(b)",
                )
                results["connections"] += 1
            except Exception as e:
                pass  # Ignore individual errors

        return {
            "entities": {"added": results["entities"]},
            "connections": {"added": results["connections"]},
        }

    except Exception as e:
        return {"error": str(e)}


def load_crypto():
    """CoinGecko - top 100 coins with exchange relationships"""
    entities = []
    connections = []

    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 100,
                "page": 1,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return entities, connections

        coins = resp.json()

        exchanges = [
            "Binance",
            "Coinbase",
            "Kraken",
            "KuCoin",
            "Bybit",
            "OKX",
            "Gemini",
            "Bitfinex",
            "Crypto.com",
            "Huobi",
        ]
        for exch in exchanges:
            entities.append(
                {
                    "type": "FinancialInstitution",
                    "name": exch,
                    "properties": {"type": "Crypto Exchange"},
                    "source": "coingecko",
                }
            )

        for coin in coins:
            entities.append(
                {
                    "type": "FinancialInstrument",
                    "name": coin["name"],
                    "properties": {
                        "symbol": coin["symbol"].upper(),
                        "market_cap": coin.get("market_cap"),
                        "price": coin.get("current_price"),
                        "rank": coin.get("market_cap_rank"),
                    },
                    "source": "coingecko",
                }
            )
            connections.append(
                {
                    "from_type": "FinancialInstrument",
                    "from_name": coin["name"],
                    "to_type": "Market",
                    "to_name": "Crypto Markets",
                    "relation": "traded_in",
                    "properties": {},
                    "source": "coingecko",
                }
            )

            if coin.get("market_cap_rank", 999) <= 20:
                for exch in exchanges[:5]:
                    connections.append(
                        {
                            "from_type": "FinancialInstrument",
                            "from_name": coin["name"],
                            "to_type": "FinancialInstitution",
                            "to_name": exch,
                            "relation": "traded_on",
                            "properties": {},
                            "source": "coingecko",
                        }
                    )

        print(f"  [CoinGecko] {len(entities)} entities, {len(connections)} connections")
    except Exception as e:
        print(f"  [CoinGecko] Error: {e}")

    return entities, connections


def load_countries():
    """REST Countries - all countries with regional relationships"""
    entities = []
    connections = []

    try:
        resp = requests.get(
            "https://restcountries.com/v3.1/all?fields=name,cca2,cca3,region,subregion",
            timeout=30,
        )
        if resp.status_code != 200:
            return entities, connections

        countries = resp.json()

        for country in countries:
            name = country.get("name", {}).get("common", "")
            if not name:
                continue

            entities.append(
                {
                    "type": "Location",
                    "name": name,
                    "properties": {
                        "iso_a2": country.get("cca2"),
                        "iso_a3": country.get("cca3"),
                        "region": country.get("region"),
                    },
                    "source": "restcountries",
                }
            )

            region = country.get("region")
            if region:
                connections.append(
                    {
                        "from_type": "Location",
                        "from_name": name,
                        "to_type": "Jurisdiction",
                        "to_name": region,
                        "relation": "in_region",
                        "properties": {},
                        "source": "restcountries",
                    }
                )

        print(
            f"  [REST Countries] {len(entities)} entities, {len(connections)} connections"
        )
    except Exception as e:
        print(f"  [REST Countries] Error: {e}")

    return entities, connections


def load_industries():
    """Industry taxonomy"""
    entities = []
    connections = []

    industries = [
        (
            "Technology",
            [
                "Software",
                "Hardware",
                "Semiconductors",
                "Cloud",
                "AI/ML",
                "Cybersecurity",
            ],
        ),
        ("Healthcare", ["Pharmaceuticals", "Biotech", "Medical Devices"]),
        ("Finance", ["Banking", "Insurance", "Asset Management"]),
        ("Energy", ["Oil & Gas", "Renewables", "Utilities"]),
        ("Consumer", ["Retail", "E-commerce", "Food & Beverage"]),
    ]

    for industry, subsectors in industries:
        entities.append(
            {
                "type": "Industry",
                "name": industry,
                "properties": {},
                "source": "taxonomy",
            }
        )
        for sub in subsectors:
            entities.append(
                {
                    "type": "Industry",
                    "name": sub,
                    "properties": {"parent": industry},
                    "source": "taxonomy",
                }
            )
            connections.append(
                {
                    "from_type": "Industry",
                    "from_name": sub,
                    "to_type": "Industry",
                    "to_name": industry,
                    "relation": "part_of",
                    "properties": {},
                    "source": "taxonomy",
                }
            )

    print(f"  [Industries] {len(entities)} entities, {len(connections)} connections")
    return entities, connections


def run_round(round_num):
    """Run one round of loading"""
    print(f"\n=== Round {round_num} ===")

    all_entities = []
    all_connections = []

    ents, conns = load_crypto()
    all_entities.extend(ents)
    all_connections.extend(conns)

    ents, conns = load_countries()
    all_entities.extend(ents)
    all_connections.extend(conns)

    ents, conns = load_industries()
    all_entities.extend(ents)
    all_connections.extend(conns)

    # Deduplicate
    seen = {}
    dedup = []
    for e in all_entities:
        key = f"{e['type']}:{e['name']}"
        if key not in seen:
            seen[key] = True
            dedup.append(e)

    # Chunk into batches
    batch_size = 50
    total_added = 0
    total_conn = 0

    for i in range(0, len(dedup), batch_size):
        batch_ents = dedup[i : i + batch_size]
        batch_conns = all_connections[i * 2 : (i + 1) * 2]

        batch = {
            "entities": batch_ents,
            "connections": batch_conns[: len(batch_ents) * 2],
        }

        time.sleep(BATCH_DELAY)
        result = post_batch(batch)

        if "error" not in result:
            total_added += result.get("entities", {}).get("added", 0)
            total_conn += result.get("connections", {}).get("added", 0)
        else:
            print(f"  Batch error: {result.get('error')}")

    print(f"  Round {round_num}: +{total_added} entities, +{total_conn} relationships")
    return total_added, total_conn


def main():
    print("=" * 50)
    print("FalkorDB Cloud Bulk Loader")
    print(f"Instance: {CLOUD_INSTANCE_ID}")
    print("=" * 50)

    total_entities = 0
    total_relationships = 0

    for round_num in range(1, 21):  # 20 rounds
        added, conns = run_round(round_num)
        total_entities += added
        total_relationships += conns
        print(
            f"\nCumulative: {total_entities} entities, {total_relationships} relationships"
        )

    print(f"\n=== FINAL ===")
    print(f"Added: {total_entities} entities, {total_relationships} relationships")


if __name__ == "__main__":
    main()
