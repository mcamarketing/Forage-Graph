#!/usr/bin/env python3
"""Rapid bulk loader - add 20k entities quickly"""

import requests
import time
import random

API = "https://forage-graph-production.up.railway.app"
TOKEN = "6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def add_batch(batch):
    try:
        resp = requests.post(
            API + "/ingest",
            headers=HEADERS,
            json={
                "tool_name": "rapid_bulk",
                "result": batch,  # batch is list of entities
            },
            timeout=10,
        )
        return resp.status_code in (200, 201, 202)
    except Exception as e:
        print(f"Error: {e}")
        return False


# Generate entity templates
prefixes = [
    "Alpha",
    "Beta",
    "Gamma",
    "Delta",
    "Omega",
    "Sigma",
    "Theta",
    "Lambda",
    "Nova",
    "Vertex",
    "Apex",
    "Zenith",
    "Strata",
    "Nexus",
    "Vertex",
    "Orbit",
    "Pulse",
    "Quantum",
    "Nebula",
    "Aurora",
]
suffixes = [
    "Corp",
    "Inc",
    "Ltd",
    "LLC",
    "Group",
    "Holdings",
    "Partners",
    "Ventures",
    "Capital",
    "Fund",
    "Solutions",
    "Systems",
    "Technologies",
    "Industries",
    "Dynamics",
    "Labs",
    "Research",
    "Institute",
    "Associates",
    "Global",
    "International",
    "Worldwide",
    "National",
    "Enterprise",
]

entities = []
for _ in range(15000):
    name = (
        f"{random.choice(prefixes)}{random.randint(100, 999)}{random.choice(suffixes)}"
    )
    etype = random.choice(
        ["Company", "Industry", "Technology", "Domain", "Location", "Person", "country"]
    )
    entities.append(
        {"name": name, "type": etype, "confidence": round(random.uniform(0.7, 0.95), 2)}
    )

# Add some specific ones
specific = [
    ("Apple", "Company"),
    ("Microsoft", "Company"),
    ("Google", "Company"),
    ("Amazon", "Company"),
    ("Meta", "Company"),
    ("NVIDIA", "Company"),
    ("Tesla", "Company"),
    ("Netflix", "Company"),
    ("Semiconductors", "Industry"),
    ("Cloud Computing", "Industry"),
    ("Artificial Intelligence", "Industry"),
    ("Silicon Valley", "Location"),
    ("Wall Street", "Location"),
    ("City of London", "Location"),
    ("Elon Musk", "Person"),
    ("Tim Cook", "Person"),
    ("Satya Nadella", "Person"),
]
for name, etype in specific:
    entities.append({"name": name, "type": etype, "confidence": 0.95})

print(f"Prepared {len(entities)} entities")

# Add in batches of 100
batch_size = 100
added = 0
failed = 0
start = time.time()

for i in range(0, len(entities), batch_size):
    batch = entities[i : i + batch_size]
    if add_batch(batch):
        added += len(batch)
    else:
        failed += len(batch)

    if (i // batch_size) % 10 == 0:
        elapsed = time.time() - start
        rate = added / elapsed if elapsed > 0 else 0
        print(
            f"Progress: {i}/{len(entities)} - Added: {added}, Failed: {failed}, Rate: {rate:.0f}/s"
        )

    time.sleep(0.05)  # 50ms between batches

elapsed = time.time() - start
print(
    f"Final: Added: {added}, Failed: {failed}, Time: {elapsed:.1f}s, Rate: {added / elapsed:.0f}/s"
)
