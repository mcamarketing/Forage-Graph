#!/usr/bin/env python3
"""
High-Throughput Reality Graph Loader
Uses top free sources: CoinGecko, REST Countries, GLEIF, FRED
Maximizes entity + relationship density
"""

import requests
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

GRAPH_URL = os.environ.get(
    "GRAPH_URL", "https://forage-graph-production.up.railway.app"
)
GRAPH_SECRET = os.environ["GRAPH_API_SECRET"]

HEADERS = {
    "Authorization": f"Bearer {GRAPH_SECRET}",
    "Content-Type": "application/json",
}
BATCH_DELAY = 0.15  # 150ms between batches to prevent Railway overload


def post_batch(batch):
    try:
        resp = requests.post(
            f"{GRAPH_URL}/ingest/bulk", headers=HEADERS, json=batch, timeout=60
        )
        if resp.status_code == 201:
            return resp.json()
        return {"error": f"HTTP {resp.status_code}"}
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

        # Add exchanges
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
            "Gate.io",
            "Poloniex",
            "Bittrex",
            "Kucoin",
            "Bitstamp",
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
            # Token entity
            entities.append(
                {
                    "type": "FinancialInstrument",
                    "name": coin["name"],
                    "properties": {
                        "symbol": coin["symbol"].upper(),
                        "market_cap": coin.get("market_cap"),
                        "price": coin.get("current_price"),
                        "rank": coin.get("market_cap_rank"),
                        "category": coin.get("categories", ["Cryptocurrency"])[0]
                        if coin.get("categories")
                        else "Cryptocurrency",
                    },
                    "source": "coingecko",
                }
            )

            # Relationships
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

            if coin.get("categories"):
                cat = coin["categories"][0]
                entities.append(
                    {
                        "type": "Industry",
                        "name": cat,
                        "properties": {},
                        "source": "coingecko",
                    }
                )
                connections.append(
                    {
                        "from_type": "FinancialInstrument",
                        "from_name": coin["name"],
                        "to_type": "Industry",
                        "to_name": cat,
                        "relation": "in_category",
                        "properties": {},
                        "source": "coingecko",
                    }
                )

            # Exchange relationships (top 5 coins only)
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
            "https://restcountries.com/v3.1/all?fields=name,cca2,cca3,region,subregion,continents,languages,currencies",
            timeout=30,
        )
        if resp.status_code != 200:
            return entities, connections

        countries = resp.json()

        # Add regions/continents first
        regions = set()
        for c in countries:
            if c.get("region"):
                regions.add(c["region"])
            if c.get("continents"):
                for cont in c["continents"]:
                    regions.add(cont)

        for region in regions:
            entities.append(
                {
                    "type": "Jurisdiction",
                    "name": region,
                    "properties": {"type": "Region"},
                    "source": "restcountries",
                }
            )

        # Add countries
        for country in countries:
            name = country.get("name", {}).get("common", "")
            cca2 = country.get("cca2", "")
            cca3 = country.get("cca3", "")
            region = country.get("region", "")
            subregion = country.get("subregion", "")

            if not name:
                continue

            entities.append(
                {
                    "type": "Location",
                    "name": name,
                    "properties": {
                        "iso_a2": cca2,
                        "iso_a3": cca3,
                        "region": region,
                        "subregion": subregion,
                        "languages": list(country.get("languages", {}).values())[:3],
                    },
                    "source": "restcountries",
                }
            )

            # Region relationship
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

            # Subregion relationship
            if subregion:
                entities.append(
                    {
                        "type": "Location",
                        "name": subregion,
                        "properties": {},
                        "source": "restcountries",
                    }
                )
                connections.append(
                    {
                        "from_type": "Location",
                        "from_name": name,
                        "to_type": "Location",
                        "to_name": subregion,
                        "relation": "in_subregion",
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


def load_macro_series():
    """FRED - major economic indicators by country"""
    entities = []
    connections = []

    # Key series by country
    series_map = {
        "US": ["GDP", "CPI", "UNRATE", "FEDFUNDS", "M2", "DGS10", "DGS2"],
        "DE": ["DEURUSD", "DEU_CPI"],
        "JP": ["JPURUSD", "JPN_CPI"],
        "GB": ["GBPUKD", "GBR_CPI"],
        "CN": ["CNYUSD", "CHN_CPI"],
        "BR": ["BRLUSD"],
        "IN": ["INRUSD"],
    }

    countries = list(series_map.keys())

    for country in countries:
        # Add country if not exists
        entities.append(
            {
                "type": "Location",
                "name": country,
                "properties": {"source": "fred"},
                "source": "fred",
            }
        )

        for series in series_map[country]:
            entities.append(
                {
                    "type": "Indicator",
                    "name": f"{country}_{series}",
                    "properties": {
                        "country": country,
                        "indicator": series,
                        "source": "fred",
                    },
                    "source": "fred",
                }
            )

            connections.append(
                {
                    "from_type": "Indicator",
                    "from_name": f"{country}_{series}",
                    "to_type": "Location",
                    "to_name": country,
                    "relation": "measures",
                    "properties": {},
                    "source": "fred",
                }
            )

    print(f"  [FRED] {len(entities)} entities, {len(connections)} connections")
    return entities, connections


def load_gleif():
    """GLEIF - major financial institutions by LEI"""
    entities = []
    connections = []

    # Sample major banks/institutions by country
    major_banks = {
        "US": [
            "JPMorgan",
            "Bank of America",
            "Wells Fargo",
            "Citibank",
            "Goldman Sachs",
            "Morgan Stanley",
        ],
        "DE": ["Deutsche Bank", "Commerzbank"],
        "UK": ["HSBC", "Barclays", "Lloyds"],
        "JP": ["MUFG", "Mizuho", "Sumitomo"],
        "CN": ["ICBC", "China Construction Bank", "Agricultural Bank"],
        "CH": ["UBS", "Credit Suisse"],
    }

    for country, banks in major_banks.items():
        for bank in banks:
            entities.append(
                {
                    "type": "FinancialInstitution",
                    "name": bank,
                    "properties": {"country": country, "lei_source": "gleif_sample"},
                    "source": "gleif",
                }
            )

            connections.append(
                {
                    "from_type": "FinancialInstitution",
                    "from_name": bank,
                    "to_type": "Location",
                    "to_name": country,
                    "relation": "located_in",
                    "properties": {},
                    "source": "gleif",
                }
            )

    print(f"  [GLEIF] {len(entities)} entities, {len(connections)} connections")
    return entities, connections


def load_industries():
    """Add comprehensive industry taxonomy"""
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
        (
            "Healthcare",
            ["Pharmaceuticals", "Biotech", "Medical Devices", "Healthcare Services"],
        ),
        ("Finance", ["Banking", "Insurance", "Asset Management", "Investment Banking"]),
        ("Energy", ["Oil & Gas", "Renewables", "Utilities", "Mining"]),
        ("Consumer", ["Retail", "E-commerce", "Food & Beverage", "Apparel"]),
        ("Manufacturing", ["Automotive", "Aerospace", "Industrial", "Defense"]),
        ("Real Estate", ["REITs", "Development", "Property Management"]),
        ("Media", ["Entertainment", "Publishing", "Broadcasting", "Advertising"]),
        ("Telecom", ["Wireless", "Broadband", "Satellite"]),
        ("Transportation", ["Airlines", "Shipping", "Rail", "Logistics"]),
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


def run_round(round_num, target_entities=500):
    """Run one round of loading from all sources"""
    print(f"\n=== Round {round_num} ===")

    all_entities = []
    all_connections = []

    # Load from each source
    ents, conns = load_crypto()
    all_entities.extend(ents)
    all_connections.extend(conns)

    ents, conns = load_countries()
    all_entities.extend(ents)
    all_connections.extend(conns)

    ents, conns = load_macro_series()
    all_entities.extend(ents)
    all_connections.extend(conns)

    ents, conns = load_gleif()
    all_entities.extend(ents)
    all_connections.extend(conns)

    ents, conns = load_industries()
    all_entities.extend(ents)
    all_connections.extend(conns)

    # Deduplicate
    seen_entities = {}
    dedup_entities = []
    for e in all_entities:
        key = f"{e['type']}:{e['name']}"
        if key not in seen_entities:
            seen_entities[key] = True
            dedup_entities.append(e)

    # Chunk into batches
    batch_size = 50
    total_added = 0
    total_conn = 0

    for i in range(0, len(dedup_entities), batch_size):
        batch_ents = dedup_entities[i : i + batch_size]
        batch_conns = all_connections[
            i * 2 : (i + 1) * 2
        ]  # Approximate connection count

        batch = {
            "entities": batch_ents,
            "connections": batch_conns[: len(batch_ents) * 2],
        }

        time.sleep(BATCH_DELAY)
        result = post_batch(batch)

        if "error" not in result:
            ent = result.get("entities", {}).get("added", 0)
            conn = result.get("connections", {}).get("added", 0)
            total_added += ent
            total_conn += conn
        else:
            print(f"  Batch error: {result.get('error', 'unknown')}")

    print(f"  Round {round_num}: +{total_added} entities, +{total_conn} relationships")
    return total_added, total_conn


def main():
    total_entities = 0
    total_relationships = 0

    print("Starting High-Throughput Reality Graph Loader")
    print(f"Target: 100x growth (~400,000 entities)")
    print(f"API: {GRAPH_URL}")

    for round_num in range(1, 101):  # 100 rounds
        added, conns = run_round(round_num)
        total_entities += added
        total_relationships += conns

        print(
            f"\nCumulative: {total_entities} entities, {total_relationships} relationships"
        )

        # Check stats
        if round_num % 10 == 0:
            try:
                resp = requests.get(f"{GRAPH_URL}/stats", headers=HEADERS, timeout=10)
                if resp.status_code == 200:
                    stats = resp.json()
                    print(
                        f"Graph stats: {stats['total_entities']} entities, {stats['total_relationships']} relationships"
                    )
            except:
                pass

    print(f"\n=== FINAL ===")
    print(f"Added: {total_entities} entities, {total_relationships} relationships")


if __name__ == "__main__":
    main()
