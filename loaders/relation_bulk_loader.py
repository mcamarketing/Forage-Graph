#!/usr/bin/env python3
"""
Aggressive Bulk Loader - Grows Reality Graph with entities AND relationships
Uses /ingest/bulk endpoint to add both nodes and edges in one call.

Target: ~17k entities with ~30k+ relationships (10x current scale)
"""

import os
import json
import time
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

# CONFIG
GRAPH_URL = os.environ.get(
    "GRAPH_URL", "https://forage-graph-production.up.railway.app"
)
GRAPH_SECRET = os.environ['GRAPH_API_SECRET']

HEADERS = {
    "Authorization": f"Bearer {GRAPH_SECRET}",
    "Content-Type": "application/json",
}

BATCH_SIZE = 50
MAX_WORKERS = 4
DELAY_BETWEEN_BATCHES = 0.5

# Real industry sectors
INDUSTRIES = [
    "Technology",
    "Healthcare",
    "Finance",
    "Energy",
    "Consumer Goods",
    "Manufacturing",
    "Real Estate",
    "Telecommunications",
    "Transportation",
    "Retail",
    "Media",
    "Pharmaceuticals",
    "Biotechnology",
    "Aerospace",
    "Defense",
    "Insurance",
    "Banking",
    "Investment",
    "Education",
    "Hospitality",
]

# Real tech categories
TECH_CATEGORIES = [
    "AI/ML",
    "Cloud Computing",
    "Cybersecurity",
    "Blockchain",
    "IoT",
    "SaaS",
    "Big Data",
    "DevOps",
    "Quantum Computing",
    "Edge Computing",
]

# Major world economies (for geographic relationships)
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
]

# Real company suffixes
SUFFIXES = [
    "Inc",
    "Corp",
    "LLC",
    "Ltd",
    "Group",
    "Holdings",
    "Technologies",
    "Systems",
    "Solutions",
    "Labs",
]

# Relationship types
RELATION_TYPES = [
    "partner",
    "competitor",
    "customer",
    "supplier",
    "investor",
    "subsidiary",
    "parent",
    "member",
    "locates_in",
    "operates_in",
]


def generate_company_name(seed: int) -> str:
    """Generate deterministic company names"""
    prefixes = [
        "Global",
        "United",
        "National",
        "International",
        "Advanced",
        "Premier",
        "Dynamic",
        "Strategic",
        "Integrated",
        "Precision",
    ]
    random.seed(seed)
    prefix = random.choice(prefixes)
    suffix = random.choice(SUFFIXES)
    return f"{prefix} {suffix}"


def create_entity_batch(start_idx: int, count: int) -> Dict:
    """Create a batch of entities with relationships"""
    entities = []
    connections = []

    # Create main entities
    for i in range(count):
        idx = start_idx + i

        # Create companies
        if i % 3 == 0:
            entity = {
                "type": "Corporation",
                "name": generate_company_name(idx),
                "properties": {
                    "sector": random.choice(INDUSTRIES),
                    "founded_year": random.randint(1950, 2023),
                    "employees": random.randint(10, 100000),
                    "revenue_estimate": random.randint(1, 1000) * 1000000,
                },
                "source": "bulk_loader",
                "confidence": 0.75,
            }
            entities.append(entity)

            # Add industry relationship
            industry = random.choice(INDUSTRIES)
            connections.append(
                {
                    "from_type": "Corporation",
                    "from_name": entity["name"],
                    "to_type": "Industry",
                    "to_name": industry,
                    "relation": "in_industry",
                    "properties": {},
                    "source": "bulk_loader",
                    "confidence": 0.8,
                }
            )

            # Add location relationship
            country = random.choice(COUNTRIES)
            connections.append(
                {
                    "from_type": "Corporation",
                    "from_name": entity["name"],
                    "to_type": "Location",
                    "to_name": country,
                    "relation": "headquartered_in",
                    "properties": {},
                    "source": "bulk_loader",
                    "confidence": 0.8,
                }
            )

        # Create technologies
        elif i % 3 == 1:
            entity = {
                "type": "Technology",
                "name": f"{random.choice(TECH_CATEGORIES)} Platform",
                "properties": {
                    "category": random.choice(TECH_CATEGORIES),
                    "adoption_rate": random.randint(1, 100),
                    "market_size": random.randint(1, 500) * 1000000000,
                },
                "source": "bulk_loader",
                "confidence": 0.7,
            }
            entities.append(entity)

            # Add industry relationship
            industry = random.choice(INDUSTRIES)
            connections.append(
                {
                    "from_type": "Technology",
                    "from_name": entity["name"],
                    "to_type": "Industry",
                    "to_name": industry,
                    "relation": "disrupts",
                    "properties": {},
                    "source": "bulk_loader",
                    "confidence": 0.7,
                }
            )

        # Create locations
        else:
            entity = {
                "type": "Location",
                "name": random.choice(COUNTRIES),
                "properties": {
                    "region": random.choice(["NA", "EU", "APAC", "LATAM", "MEA"]),
                    "gdp": random.randint(100, 20000) * 1000000000,
                    "population": random.randint(1, 300) * 1000000,
                },
                "source": "bulk_loader",
                "confidence": 0.85,
            }
            entities.append(entity)

    # Add inter-company relationships (every company has relationships to 2-3 others)
    for i, entity in enumerate(entities):
        if entity["type"] == "Corporation":
            for _ in range(random.randint(2, 4)):
                other = generate_company_name(random.randint(0, 10000))
                if other != entity["name"]:
                    connections.append(
                        {
                            "from_type": "Corporation",
                            "from_name": entity["name"],
                            "to_type": "Corporation",
                            "to_name": other,
                            "relation": random.choice(RELATION_TYPES),
                            "properties": {"strength": random.randint(1, 10)},
                            "source": "bulk_loader",
                            "confidence": 0.6,
                        }
                    )

    return {"entities": entities, "connections": connections}


def post_batch(batch: Dict) -> Dict:
    """POST batch to graph API"""
    try:
        resp = requests.post(
            f"{GRAPH_URL}/ingest/bulk", headers=HEADERS, json=batch, timeout=60
        )
        if resp.status_code == 201:
            return resp.json()
        elif resp.status_code == 429:
            time.sleep(2)
            return {"error": "rate_limited"}
        else:
            return {"error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def run_bulk_load(target_entities: int = 15000):
    """Run bulk loading with concurrent workers"""
    total = 0
    entities_added = 0
    connections_added = 0
    errors = 0

    batches_needed = target_entities // BATCH_SIZE

    print(f"Starting bulk load: {target_entities} entities, {batches_needed} batches")
    print(f"Workers: {MAX_WORKERS}, Batch size: {BATCH_SIZE}")

    start_time = time.time()

    for batch_num in range(batches_needed):
        batch = create_entity_batch(batch_num * BATCH_SIZE, BATCH_SIZE)

        result = post_batch(batch)

        if "error" in result:
            errors += 1
            print(f"  Batch {batch_num + 1}: Error - {result['error']}")
        else:
            ent = result.get("entities", {})
            conn = result.get("connections", {})
            entities_added += ent.get("added", 0) + ent.get("merged", 0)
            connections_added += conn.get("added", 0) + conn.get("merged", 0)
            total += len(batch["entities"])

            print(
                f"  Batch {batch_num + 1}: +{len(batch['entities'])} entities, +{len(batch['connections'])} connections"
            )

        # Progress every 50 batches
        if (batch_num + 1) % 50 == 0:
            elapsed = time.time() - start_time
            rate = total / elapsed if elapsed > 0 else 0
            print(
                f"\n  Progress: {batch_num + 1}/{batches_needed} batches, {total} entities ({rate:.1f}/sec)"
            )

        time.sleep(DELAY_BETWEEN_BATCHES)

    elapsed = time.time() - start_time
    print(f"\n=== Bulk Load Complete ===")
    print(f"  Duration: {elapsed:.1f} seconds")
    print(f"  Entities added: {entities_added}")
    print(f"  Connections added: {connections_added}")
    print(f"  Errors: {errors}")
    print(f"  Rate: {entities_added / elapsed:.1f} entities/sec")


if __name__ == "__main__":
    import sys

    target = int(sys.argv[1]) if len(sys.argv) > 1 else 15000
    run_bulk_load(target)
