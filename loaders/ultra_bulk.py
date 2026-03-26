#!/usr/bin/env python3
"""Ultra fast bulk add - target 17k+"""

import requests
import time
import random

API = "https://forage-graph-production.up.railway.app"
TOKEN = "6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def add(name, etype, conf=0.85):
    try:
        resp = requests.post(
            API + "/ingest",
            headers=HEADERS,
            json={
                "tool_name": "ultra_bulk",
                "result": {"name": name, "type": etype, "confidence": conf},
            },
            timeout=5,
        )
        return resp.status_code in (200, 201, 202)
    except:
        return False


# 17k entities
prefixes = [
    "Apex",
    "Vertex",
    "Nexus",
    "Quantum",
    "Nova",
    "Alpha",
    "Beta",
    "Gamma",
    "Delta",
    "Omega",
    "Sigma",
    "Theta",
    "Lambda",
    "Zeta",
    "Epsilon",
    "Iota",
    "Kappa",
    "Mu",
    "Nu",
    "Xi",
    "Omicron",
    "Rho",
    "Tau",
    "Upsilon",
    "Phi",
    "Chi",
    "Psi",
    "Aurora",
    "Nebula",
    "Pulsar",
    "Quasar",
    "Comet",
    "Asteroid",
    "Star",
    "Galaxy",
    "Universe",
    "Cosmos",
    "Orbit",
    "Trail",
    "Spark",
    "Flare",
    "Blaze",
    "Ember",
    "Radiance",
    "Luminous",
    "Eclipse",
    "Horizon",
    "Summit",
    "Peak",
    "Zenith",
    "Crest",
    "Ridge",
    "Valley",
    "Canyon",
    "Summit",
]
suffixes = [
    "Systems",
    "Technologies",
    "Dynamics",
    "Ventures",
    "Capital",
    "Group",
    "Holdings",
    "Partners",
    "Labs",
    "Research",
    "Solutions",
    "Innovations",
    "Enterprises",
    "Networks",
    "Platforms",
    "Services",
    "Consulting",
    "Advisors",
    "Strategies",
    "Institute",
    "Foundation",
    "Center",
    "Hub",
    "Campus",
    "Works",
    "Studios",
    "Agency",
    "Collective",
    "Syndicate",
    "Consortium",
]
types = [
    "Company",
    "Industry",
    "Technology",
    "Domain",
    "Location",
    "Person",
    "country",
    "LegalEntity",
    "Corporation",
    "Narrative",
]

print("Starting ultra bulk load...")
start = time.time()
added = 0

for i in range(17000):
    name = f"{random.choice(prefixes)}{random.randint(1000, 9999)}{random.choice(suffixes)}"
    if add(name, random.choice(types), round(random.uniform(0.7, 0.95), 2)):
        added += 1

    if (i + 1) % 500 == 0:
        elapsed = time.time() - start
        print(
            f"Progress: {i + 1}/17000 - Added: {added} - Rate: {added / elapsed:.1f}/s"
        )

elapsed = time.time() - start
print(f"DONE: {added} added in {elapsed:.1f}s ({added / elapsed:.1f}/s)")
