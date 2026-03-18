"""
Wikidata Bulk Loader
====================
Uses the Wikidata SPARQL endpoint (free, no auth required) to load:
- All sovereign countries with ISO codes, capitals, populations
- Heads of government / state for each country
- Central banks and their relationships to countries
- Major international organisations (UN, NATO, EU, ASEAN, etc.)

This creates the foundational geopolitical skeleton of the graph —
every other entity (company, event, person) can then be linked to
a country node, enabling causal traversal across borders.

Usage:
  python3 wikidata_loader.py

Rate limit: Wikidata asks for max 1 request/second from bots.
The script respects this with a 1.1s delay between SPARQL queries.
"""

import os
import json
import time
import requests

GRAPH_URL = os.environ.get("GRAPH_URL", "https://forage-graph-production.up.railway.app")
GRAPH_SECRET = os.environ.get("GRAPH_SECRET", "6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959")
BATCH_SIZE = 25
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS_GRAPH = {
    "Authorization": f"Bearer {GRAPH_SECRET}",
    "Content-Type": "application/json"
}
HEADERS_WIKIDATA = {
    "User-Agent": "ForageGraphLoader/1.0 (https://github.com/mcamarketing/Forage-Graph)",
    "Accept": "application/sparql-results+json"
}


def sparql_query(query: str) -> list:
    """Execute a SPARQL query against Wikidata and return bindings."""
    try:
        resp = requests.get(
            SPARQL_ENDPOINT,
            params={"query": query, "format": "json"},
            headers=HEADERS_WIKIDATA,
            timeout=30
        )
        resp.raise_for_status()
        return resp.json().get("results", {}).get("bindings", [])
    except Exception as e:
        print(f"[SPARQL Error] {e}")
        return []


def post_batch(batch: list) -> dict:
    try:
        resp = requests.post(
            f"{GRAPH_URL}/ingest_raw_batch",
            headers=HEADERS_GRAPH,
            json={"batch": batch},
            timeout=30
        )
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


def val(binding: dict, key: str) -> str:
    return binding.get(key, {}).get("value", "").strip()


# ─── 1. SOVEREIGN COUNTRIES ───────────────────────────────────────────────────
COUNTRIES_QUERY = """
SELECT ?country ?countryLabel ?iso2 ?iso3 ?capital ?capitalLabel ?population ?continent ?continentLabel WHERE {
  ?country wdt:P31 wd:Q3624078 .
  OPTIONAL { ?country wdt:P297 ?iso2 }
  OPTIONAL { ?country wdt:P298 ?iso3 }
  OPTIONAL { ?country wdt:P36 ?capital }
  OPTIONAL { ?country wdt:P1082 ?population }
  OPTIONAL { ?country wdt:P30 ?continent }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
ORDER BY ?countryLabel
"""

def load_countries():
    print("[Wikidata] Loading countries...")
    rows = sparql_query(COUNTRIES_QUERY)
    time.sleep(1.1)

    batch = []
    count = 0
    for row in rows:
        name = val(row, "countryLabel")
        qid = val(row, "country").split("/")[-1]
        if not name or name.startswith("Q"):
            continue

        entity = {
            "name": name,
            "type": "country",
            "properties": {
                "wikidata_qid": qid,
                "iso2": val(row, "iso2"),
                "iso3": val(row, "iso3"),
                "capital": val(row, "capitalLabel"),
                "population": val(row, "population"),
                "continent": val(row, "continentLabel"),
                "provenance": ["Wikidata"]
            },
            "relationships": [],
            "source": "Wikidata",
            "confidence": 0.95,
            "causal_weight": 0.0,
            "mechanism": "entity_registry"
        }

        continent = val(row, "continentLabel")
        if continent:
            entity["relationships"].append({
                "targetName": continent,
                "targetType": "country",
                "relation": "located_in",
                "confidence": 0.99,
                "causal_weight": 0.1,
                "mechanism": "geography"
            })

        batch.append(entity)
        count += 1
        if len(batch) >= BATCH_SIZE:
            post_batch(batch)
            batch = []

    if batch:
        post_batch(batch)
    print(f"[Wikidata] Countries loaded: {count}")
    return count


# ─── 2. HEADS OF GOVERNMENT ───────────────────────────────────────────────────
HOG_QUERY = """
SELECT ?country ?countryLabel ?leader ?leaderLabel ?role ?roleLabel WHERE {
  ?country wdt:P31 wd:Q3624078 .
  { ?country wdt:P6 ?leader . BIND(wd:P6 AS ?role) }
  UNION
  { ?country wdt:P35 ?leader . BIND(wd:P35 AS ?role) }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
"""

def load_heads_of_government():
    print("[Wikidata] Loading heads of government...")
    rows = sparql_query(HOG_QUERY)
    time.sleep(1.1)

    batch = []
    count = 0
    for row in rows:
        leader_name = val(row, "leaderLabel")
        country_name = val(row, "countryLabel")
        qid = val(row, "leader").split("/")[-1]
        role_qid = val(row, "role").split("/")[-1]

        if not leader_name or leader_name.startswith("Q") or not country_name:
            continue

        role = "head_of_government" if role_qid == "P6" else "head_of_state"

        entity = {
            "name": leader_name,
            "type": "person",
            "properties": {
                "wikidata_qid": qid,
                "role": role,
                "country": country_name,
                "provenance": ["Wikidata"]
            },
            "relationships": [{
                "targetName": country_name,
                "targetType": "country",
                "relation": role,
                "confidence": 0.90,
                "causal_weight": 0.7,
                "mechanism": "political_authority"
            }],
            "source": "Wikidata",
            "confidence": 0.90,
            "causal_weight": 0.7,
            "mechanism": "political_authority"
        }

        batch.append(entity)
        count += 1
        if len(batch) >= BATCH_SIZE:
            post_batch(batch)
            batch = []

    if batch:
        post_batch(batch)
    print(f"[Wikidata] Heads of government loaded: {count}")
    return count


# ─── 3. CENTRAL BANKS ─────────────────────────────────────────────────────────
CB_QUERY = """
SELECT ?bank ?bankLabel ?country ?countryLabel WHERE {
  ?bank wdt:P31 wd:Q1329623 .
  OPTIONAL { ?bank wdt:P17 ?country }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
"""

def load_central_banks():
    print("[Wikidata] Loading central banks...")
    rows = sparql_query(CB_QUERY)
    time.sleep(1.1)

    batch = []
    count = 0
    for row in rows:
        name = val(row, "bankLabel")
        country_name = val(row, "countryLabel")
        qid = val(row, "bank").split("/")[-1]

        if not name or name.startswith("Q"):
            continue

        entity = {
            "name": name,
            "type": "central_bank",
            "properties": {
                "wikidata_qid": qid,
                "country": country_name,
                "provenance": ["Wikidata"]
            },
            "relationships": [],
            "source": "Wikidata",
            "confidence": 0.95,
            "causal_weight": 0.9,
            "mechanism": "monetary_policy"
        }

        if country_name:
            entity["relationships"].append({
                "targetName": country_name,
                "targetType": "country",
                "relation": "central_bank_of",
                "confidence": 0.97,
                "causal_weight": 0.9,
                "mechanism": "monetary_authority"
            })

        batch.append(entity)
        count += 1
        if len(batch) >= BATCH_SIZE:
            post_batch(batch)
            batch = []

    if batch:
        post_batch(batch)
    print(f"[Wikidata] Central banks loaded: {count}")
    return count


# ─── 4. INTERNATIONAL ORGANISATIONS ──────────────────────────────────────────
ORGS_QUERY = """
SELECT ?org ?orgLabel ?member ?memberLabel WHERE {
  VALUES ?org {
    wd:Q1065    # United Nations
    wd:Q7184    # NATO
    wd:Q458     # European Union
    wd:Q791     # ASEAN
    wd:Q7825    # G20
    wd:Q19842   # G7
    wd:Q7930    # BRICS
    wd:Q8908    # IMF
    wd:Q7748    # World Bank
    wd:Q11430   # WTO
    wd:Q41550   # OPEC
    wd:Q8932    # African Union
    wd:Q170481  # Shanghai Cooperation Organisation
  }
  ?org wdt:P17|wdt:P150|wdt:P527 ?member .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
"""

def load_international_orgs():
    print("[Wikidata] Loading international organisations...")
    rows = sparql_query(ORGS_QUERY)
    time.sleep(1.1)

    # Group by org
    orgs = {}
    for row in rows:
        org_name = val(row, "orgLabel")
        member_name = val(row, "memberLabel")
        qid = val(row, "org").split("/")[-1]
        if not org_name or org_name.startswith("Q"):
            continue
        if org_name not in orgs:
            orgs[org_name] = {"qid": qid, "members": []}
        if member_name and not member_name.startswith("Q"):
            orgs[org_name]["members"].append(member_name)

    batch = []
    count = 0
    for org_name, data in orgs.items():
        entity = {
            "name": org_name,
            "type": "regional_bloc",
            "properties": {
                "wikidata_qid": data["qid"],
                "member_count": len(data["members"]),
                "provenance": ["Wikidata"]
            },
            "relationships": [
                {
                    "targetName": m,
                    "targetType": "country",
                    "relation": "member_of",
                    "confidence": 0.92,
                    "causal_weight": 0.5,
                    "mechanism": "international_membership"
                }
                for m in data["members"][:50]  # cap at 50 members per org
            ],
            "source": "Wikidata",
            "confidence": 0.95,
            "causal_weight": 0.5,
            "mechanism": "international_membership"
        }
        batch.append(entity)
        count += 1
        if len(batch) >= BATCH_SIZE:
            post_batch(batch)
            batch = []

    if batch:
        post_batch(batch)
    print(f"[Wikidata] International organisations loaded: {count}")
    return count


if __name__ == "__main__":
    print("=" * 60)
    print("Wikidata Bulk Loader — Forage Graph")
    print(f"Target: {GRAPH_URL}")
    print("=" * 60)

    start = time.time()
    c1 = load_countries()
    c2 = load_heads_of_government()
    c3 = load_central_banks()
    c4 = load_international_orgs()
    elapsed = time.time() - start

    print(f"\nDone in {elapsed:.1f}s")
    print(f"Countries          : {c1}")
    print(f"Heads of govt      : {c2}")
    print(f"Central banks      : {c3}")
    print(f"Int'l organisations: {c4}")
    print(f"Total              : {c1+c2+c3+c4}")
