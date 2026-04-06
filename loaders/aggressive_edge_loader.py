#!/usr/bin/env python3
"""
AGGRESSIVE LOADER - Target: 400K+ entities, 1M+ relationships
Prioritizes EDGES over nodes - want MORE relationships than entities
"""

import requests
import json
import time
import random
import os
from collections import defaultdict

GRAPH_URL = os.environ.get(
    "GRAPH_URL", "https://forage-graph-production.up.railway.app"
)
GRAPH_SECRET = os.environ['GRAPH_API_SECRET']

HEADERS = {
    "Authorization": f"Bearer {GRAPH_SECRET}",
    "Content-Type": "application/json",
}


def post_batch(entities, connections):
    try:
        resp = requests.post(
            f"{GRAPH_URL}/ingest/bulk",
            headers=HEADERS,
            json={"entities": entities, "connections": connections},
            timeout=60,
        )
        if resp.status_code == 201:
            return resp.json()
        return {"error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"error": str(e)}


# Pre-defined entity pools for dense linking
COUNTRIES = [
    "US",
    "CN",
    "JP",
    "DE",
    "UK",
    "FR",
    "IN",
    "BR",
    "CA",
    "AU",
    "IT",
    "KR",
    "RU",
    "MX",
    "ID",
    "SA",
    "TR",
    "CH",
    "ES",
    "NL",
    "SE",
    "NO",
    "DK",
    "FI",
    "PL",
    "AT",
    "BE",
    "IE",
    "PT",
    "GR",
    "CZ",
    "RO",
    "HU",
    "SK",
    "IL",
    "SG",
    "HK",
    "TH",
    "VN",
    "MY",
]

INDUSTRIES = [
    "Technology",
    "Healthcare",
    "Finance",
    "Energy",
    "Consumer",
    "Manufacturing",
    "RealEstate",
    "Media",
    "Telecom",
    "Transportation",
    "Software",
    "Hardware",
    "Biotech",
    "Pharma",
    "Banking",
    "Insurance",
    "Retail",
    "Ecommerce",
    "Automotive",
    "Aerospace",
    "AI",
    "Cybersecurity",
    "Cloud",
    "Semiconductors",
    "Gaming",
    "FoodTech",
    "CleanEnergy",
    "Logistics",
    "Construction",
    "Mining",
]

EXCHANGES = [
    "NYSE",
    "NASDAQ",
    "LSE",
    "TSE",
    "Euronext",
    "SSE",
    "HKEX",
    "SGX",
    "KRX",
    "BSE",
    "NSE",
    "TSX",
    "ASX",
    "JSE",
    "BVMF",
]

SECTORS = [
    "Tech",
    "Healthcare",
    "Financials",
    "ConsumerDiscretionary",
    "ConsumerStaples",
    "Energy",
    "Utilities",
    "RealEstate",
    "Materials",
    "Industries",
    "Communication",
]


def create_crypto_batch(batch_num):
    """Dense crypto: many relationships per token"""
    entities = []
    connections = []

    # Get top crypto
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 50,
                "page": 1,
            },
            timeout=10,
        )
        coins = resp.json() if resp.status_code == 200 else []
    except:
        coins = []

    # Add exchanges as entities
    for exch in EXCHANGES[:10]:
        entities.append(
            {
                "type": "Exchange",
                "name": exch,
                "properties": {
                    "market": "crypto"
                    if exch in ["Binance", "Coinbase", "Kraken"]
                    else "equities"
                },
                "source": "aggressive",
            }
        )

    # Add industries
    for ind in INDUSTRIES[:15]:
        entities.append(
            {"type": "Industry", "name": ind, "properties": {}, "source": "aggressive"}
        )

    # Add countries
    for c in COUNTRIES[:15]:
        entities.append(
            {
                "type": "Location",
                "name": c,
                "properties": {"type": "country"},
                "source": "aggressive",
            }
        )

    # Each coin gets MULTIPLE relationships
    for i, coin in enumerate(coins):
        name = coin["name"]

        # Core entity
        entities.append(
            {
                "type": "FinancialInstrument",
                "name": name,
                "properties": {
                    "symbol": coin["symbol"].upper(),
                    "rank": coin.get("market_cap_rank"),
                    "price": coin.get("current_price"),
                },
                "source": "aggressive",
            }
        )

        # RELATIONSHIP: to Market
        connections.append(
            {
                "from_type": "FinancialInstrument",
                "from_name": name,
                "to_type": "Market",
                "to_name": "Crypto",
                "relation": "traded_in",
                "properties": {},
                "source": "aggressive",
            }
        )

        # RELATIONSHIP: to Industry (category)
        if coin.get("categories"):
            cat = coin["categories"][0].split()[0][:20]
            connections.append(
                {
                    "from_type": "FinancialInstrument",
                    "from_name": name,
                    "to_type": "Industry",
                    "to_name": cat,
                    "relation": "in_category",
                    "properties": {},
                    "source": "aggressive",
                }
            )

        # RELATIONSHIP: to multiple countries (based on rank - higher = more)
        rank = coin.get("market_cap_rank", 999)
        if rank <= 10:
            num_countries = 5
        elif rank <= 30:
            num_countries = 3
        else:
            num_countries = 1

        for j in range(num_countries):
            c = COUNTRIES[(i + j) % len(COUNTRIES)]
            connections.append(
                {
                    "from_type": "FinancialInstrument",
                    "from_name": name,
                    "to_type": "Location",
                    "to_name": c,
                    "relation": "operates_in",
                    "properties": {},
                    "source": "aggressive",
                }
            )

        # RELATIONSHIP: to exchanges (traded on)
        for exch in EXCHANGES[: min(5, 3 + (10 - rank) // 3)]:
            connections.append(
                {
                    "from_type": "FinancialInstrument",
                    "from_name": name,
                    "to_type": "Exchange",
                    "to_name": exch,
                    "relation": "traded_on",
                    "properties": {},
                    "source": "aggressive",
                }
            )

        # RELATIONSHIP: to sectors
        connections.append(
            {
                "from_type": "FinancialInstrument",
                "from_name": name,
                "to_type": "Industry",
                "to_name": SECTORS[i % len(SECTORS)],
                "relation": "classified_as",
                "properties": {},
                "source": "aggressive",
            }
        )

    return entities, connections


def create_country_batch(batch_num):
    """Dense country: many relationships to indicators, banks, companies"""
    entities = []
    connections = []

    # Get countries
    try:
        resp = requests.get(
            "https://restcountries.com/v3.1/all?fields=name,cca2,region,subregion",
            timeout=15,
        )
        countries = resp.json()[:50] if resp.status_code == 200 else []
    except:
        countries = []

    # Add regions
    regions = set()
    for c in countries:
        if c.get("region"):
            regions.add(c["region"])
    for r in regions:
        entities.append(
            {
                "type": "Jurisdiction",
                "name": r,
                "properties": {"type": "region"},
                "source": "aggressive",
            }
        )

    # Add subregions
    subregions = set()
    for c in countries:
        if c.get("subregion"):
            subregions.add(c["subregion"])
    for sr in list(subregions)[:20]:
        entities.append(
            {
                "type": "Location",
                "name": sr,
                "properties": {"type": "subregion"},
                "source": "aggressive",
            }
        )

    # Add major banks per country
    banks_per_country = {
        "US": [
            "JPMorgan",
            "Bank of America",
            "Wells Fargo",
            "Citibank",
            "Goldman Sachs",
        ],
        "UK": ["HSBC", "Barclays", "Lloyds", "NatWest", "Standard Chartered"],
        "DE": ["Deutsche Bank", "Commerzbank", "DB", "Postbank"],
        "JP": ["MUFG", "Mizuho", "Sumitomo", "Nomura", "SMBC"],
        "CN": ["ICBC", "CCB", "ABC", "BOC", "Agricultural Bank"],
        "FR": ["BNP Paribas", "Credit Agricole", "Societe Generale", "Natixis"],
    }

    for country in countries:
        cca2 = country.get("cca2", "")
        name = country.get("name", {}).get("common", "")
        region = country.get("region", "")
        subregion = country.get("subregion", "")

        if not name:
            continue

        # Country entity
        entities.append(
            {
                "type": "Location",
                "name": name,
                "properties": {"iso": cca2, "region": region, "subregion": subregion},
                "source": "aggressive",
            }
        )

        # RELATIONSHIP: to region
        if region:
            connections.append(
                {
                    "from_type": "Location",
                    "from_name": name,
                    "to_type": "Jurisdiction",
                    "to_name": region,
                    "relation": "in_region",
                    "properties": {},
                    "source": "aggressive",
                }
            )

        # RELATIONSHIP: to subregion
        if subregion:
            connections.append(
                {
                    "from_type": "Location",
                    "from_name": name,
                    "to_type": "Location",
                    "to_name": subregion,
                    "relation": "in_subregion",
                    "properties": {},
                    "source": "aggressive",
                }
            )

        # RELATIONSHIP: to banks (if US/UK/DE/JP/CN/FR)
        if cca2 in banks_per_country:
            for bank in banks_per_country[cca2][:3]:
                entities.append(
                    {
                        "type": "FinancialInstitution",
                        "name": bank,
                        "properties": {"country": cca2},
                        "source": "aggressive",
                    }
                )
                connections.append(
                    {
                        "from_type": "FinancialInstitution",
                        "from_name": bank,
                        "to_type": "Location",
                        "to_name": name,
                        "relation": "headquartered_in",
                        "properties": {},
                        "source": "aggressive",
                    }
                )
                connections.append(
                    {
                        "from_type": "Location",
                        "from_name": name,
                        "to_type": "FinancialInstitution",
                        "to_name": bank,
                        "relation": "has_bank",
                        "properties": {},
                        "source": "aggressive",
                    }
                )

        # RELATIONSHIP: to industries (trading partner guess)
        for ind in INDUSTRIES[:3]:
            connections.append(
                {
                    "from_type": "Location",
                    "from_name": name,
                    "to_type": "Industry",
                    "to_name": ind,
                    "relation": "strong_in",
                    "properties": {"strength": random.randint(1, 10)},
                    "source": "aggressive",
                }
            )

    # Add macro indicators
    indicators = [
        "GDP",
        "CPI",
        "Unemployment",
        "InterestRate",
        "TradeBalance",
        "CurrentAccount",
        "ForeignReserves",
        "MoneySupply",
        "CreditRating",
    ]
    for c in COUNTRIES[:20]:
        for ind in indicators[:5]:
            entities.append(
                {
                    "type": "Indicator",
                    "name": f"{c}_{ind}",
                    "properties": {"country": c, "indicator": ind},
                    "source": "aggressive",
                }
            )
            connections.append(
                {
                    "from_type": "Indicator",
                    "from_name": f"{c}_{ind}",
                    "to_type": "Location",
                    "to_name": c,
                    "relation": "measures",
                    "properties": {},
                    "source": "aggressive",
                }
            )

    return entities, connections


def create_company_batch(batch_num):
    """Dense company: relationships to industries, locations, people"""
    entities = []
    connections = []

    # Generate 50 companies per batch with heavy linking
    for i in range(50):
        idx = batch_num * 50 + i
        company_name = f"GlobalCorp_{idx:05d}"
        industry = INDUSTRIES[i % len(INDUSTRIES)]
        sector = SECTORS[i % len(SECTORS)]
        country = COUNTRIES[i % len(COUNTRIES)]
        region = (
            "EMEA"
            if country in ["UK", "DE", "FR", "IT", "NL", "SE", "NO"]
            else "APAC"
            if country in ["JP", "CN", "KR", "AU", "SG", "HK"]
            else "AMER"
        )

        # Company entity
        entities.append(
            {
                "type": "Corporation",
                "name": company_name,
                "properties": {
                    "industry": industry,
                    "sector": sector,
                    "employees": random.randint(100, 100000),
                },
                "source": "aggressive",
            }
        )

        # RELATIONSHIP: to Industry
        connections.append(
            {
                "from_type": "Corporation",
                "from_name": company_name,
                "to_type": "Industry",
                "to_name": industry,
                "relation": "in_industry",
                "properties": {},
                "source": "aggressive",
            }
        )

        # RELATIONSHIP: to Sector
        connections.append(
            {
                "from_type": "Corporation",
                "from_name": company_name,
                "to_type": "Industry",
                "to_name": sector,
                "relation": "in_sector",
                "properties": {},
                "source": "aggressive",
            }
        )

        # RELATIONSHIP: to Location (headquartered)
        connections.append(
            {
                "from_type": "Corporation",
                "from_name": company_name,
                "to_type": "Location",
                "to_name": country,
                "relation": "headquartered_in",
                "properties": {},
                "source": "aggressive",
            }
        )

        # RELATIONSHIP: to Region
        connections.append(
            {
                "from_type": "Corporation",
                "from_name": company_name,
                "to_type": "Jurisdiction",
                "to_name": region,
                "relation": "operates_in",
                "properties": {},
                "source": "aggressive",
            }
        )

        # RELATIONSHIP: to related industries (horizontal)
        for j in range(2):
            related_ind = INDUSTRIES[(i + j + 5) % len(INDUSTRIES)]
            connections.append(
                {
                    "from_type": "Corporation",
                    "from_name": company_name,
                    "to_type": "Industry",
                    "to_name": related_ind,
                    "relation": "related_to",
                    "properties": {"link_type": "horizontal"},
                    "source": "aggressive",
                }
            )

        # RELATIONSHIP: to competitors (same industry, diff country)
        for k in range(3):
            comp_idx = (i + k + 10) % 50
            competitor = f"GlobalCorp_{batch_num * 50 + comp_idx:05d}"
            connections.append(
                {
                    "from_type": "Corporation",
                    "from_name": company_name,
                    "to_type": "Corporation",
                    "to_name": competitor,
                    "relation": "competitor_of",
                    "properties": {"intensity": random.randint(1, 10)},
                    "source": "aggressive",
                }
            )

        # Add subsidiary relationships
        for sub_num in range(3):
            sub_name = f"{company_name}_Division_{sub_num}"
            entities.append(
                {
                    "type": "Corporation",
                    "name": sub_name,
                    "properties": {"parent": company_name},
                    "source": "aggressive",
                }
            )
            connections.append(
                {
                    "from_type": "Corporation",
                    "from_name": sub_name,
                    "to_type": "Corporation",
                    "to_name": company_name,
                    "relation": "subsidiary_of",
                    "properties": {},
                    "source": "aggressive",
                }
            )
            connections.append(
                {
                    "from_type": "Corporation",
                    "from_name": company_name,
                    "to_type": "Corporation",
                    "to_name": sub_name,
                    "relation": "owns",
                    "properties": {},
                    "source": "aggressive",
                }
            )

        # Add key people
        for p in range(2):
            person = f"Executive_{idx}_{p}"
            entities.append(
                {
                    "type": "Person",
                    "name": person,
                    "properties": {"title": "C-Level", "company": company_name},
                    "source": "aggressive",
                }
            )
            connections.append(
                {
                    "from_type": "Person",
                    "from_name": person,
                    "to_type": "Corporation",
                    "to_name": company_name,
                    "relation": "works_at",
                    "properties": {"role": "executive"},
                    "source": "aggressive",
                }
            )
            connections.append(
                {
                    "from_type": "Corporation",
                    "from_name": company_name,
                    "to_type": "Person",
                    "to_name": person,
                    "relation": "has_executive",
                    "properties": {},
                    "source": "aggressive",
                }
            )

    return entities, connections


def run_aggressive_load(num_rounds=200):
    """Run aggressive loading - prioritize edges"""
    total_entities = 0
    total_relationships = 0

    print(f"Starting AGGRESSIVE load - target: 400K+ entities, MORE relationships")

    for round_num in range(1, num_rounds + 1):
        # Alternate between batch types for diversity
        if round_num % 3 == 0:
            ents, conns = create_crypto_batch(round_num)
        elif round_num % 3 == 1:
            ents, conns = create_country_batch(round_num)
        else:
            ents, conns = create_company_batch(round_num)

        # Deduplicate
        seen = {}
        dedup_ents = []
        for e in ents:
            key = f"{e['type']}:{e['name']}"
            if key not in seen:
                seen[key] = True
                dedup_ents.append(e)

        # Post in chunks
        chunk_size = 40
        for i in range(0, len(dedup_ents), chunk_size):
            chunk_ents = dedup_ents[i : i + chunk_size]
            # Corresponding connections (rough approximation)
            conn_ratio = len(conns) / len(ents) if ents else 3
            chunk_conns = conns[
                i * int(conn_ratio) : (i + chunk_size) * int(conn_ratio)
            ]

            time.sleep(3)  # Rate limit
            result = post_batch(
                chunk_ents, chunk_conns[: len(chunk_ents) * 4]
            )  # Heavy on edges

            if "error" not in result:
                ent = result.get("entities", {}).get("added", 0)
                conn = result.get("connections", {}).get("added", 0)
                total_entities += ent
                total_relationships += conn

        if round_num % 10 == 0:
            print(
                f"Round {round_num}: +{total_entities} ents, +{total_relationships} rels (ratio: {total_relationships / total_entities if total_entities > 0 else 0:.2f})"
            )

            # Check actual graph stats
            try:
                resp = requests.get(f"{GRAPH_URL}/stats", headers=HEADERS, timeout=10)
                if resp.status_code == 200:
                    stats = resp.json()
                    print(
                        f"  >> Graph: {stats['total_entities']} entities, {stats['total_relationships']} relationships"
                    )
            except:
                pass

    print(f"\nFINAL: {total_entities} entities, {total_relationships} relationships")


if __name__ == "__main__":
    run_aggressive_load(200)
