#!/usr/bin/env python3
"""
Direct SQLite-based loader that bypasses the broken API
Stores data locally and can sync when API is fixed
"""

import sqlite3
import json
import os
import time
import requests
from datetime import datetime

DB_PATH = "reality_graph_cache.db"
GRAPH_URL = os.environ.get(
    "GRAPH_URL", "https://forage-graph-production.up.railway.app"
)
GRAPH_SECRET = os.environ.get(
    "GRAPH_SECRET", "6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959"
)


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pending_entities (
            id INTEGER PRIMARY KEY,
            type TEXT, name TEXT, properties TEXT,
            source TEXT, confidence REAL, created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pending_connections (
            id INTEGER PRIMARY KEY,
            from_type TEXT, from_name TEXT,
            to_type TEXT, to_name TEXT,
            relation TEXT, properties TEXT,
            source TEXT, confidence REAL, created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY,
            action TEXT, count INTEGER, result TEXT, timestamp TEXT
        )
    """)
    conn.commit()
    return conn


def fetch_crypto_data():
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 30,
                "page": 1,
            },
            timeout=10,
        )
        return resp.json() if resp.status_code == 200 else []
    except:
        return []


def fetch_countries():
    try:
        resp = requests.get(
            "https://restcountries.com/v3.1/all?fields=name,cca2,region,subregion",
            timeout=10,
        )
        return resp.json() if resp.status_code == 200 else []
    except:
        return []


def add_entity(conn, etype, name, props, source, confidence):
    conn.execute(
        """
        INSERT INTO pending_entities (type, name, properties, source, confidence, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        (
            etype,
            name,
            json.dumps(props),
            source,
            confidence,
            datetime.utcnow().isoformat(),
        ),
    )


def add_connection(
    conn, from_type, from_name, to_type, to_name, relation, props, source, confidence
):
    conn.execute(
        """
        INSERT INTO pending_connections (from_type, from_name, to_type, to_name, relation, properties, source, confidence, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            from_type,
            from_name,
            to_type,
            to_name,
            relation,
            json.dumps(props),
            source,
            confidence,
            datetime.utcnow().isoformat(),
        ),
    )


def load_crypto(conn, data):
    count = 0
    for coin in data:
        add_entity(
            conn,
            "FinancialInstrument",
            coin.get("name", ""),
            {
                "symbol": coin.get("symbol", "").upper(),
                "market_cap": coin.get("market_cap"),
                "price": coin.get("current_price"),
                "rank": coin.get("market_cap_rank"),
            },
            "coingecko",
            0.85,
        )
        count += 1

        add_connection(
            conn,
            "FinancialInstrument",
            coin.get("name", ""),
            "Market",
            "Crypto Markets",
            "traded_in",
            {},
            "coingecko",
            0.8,
        )

        if coin.get("categories"):
            cat = coin["categories"][0]
            add_connection(
                conn,
                "FinancialInstrument",
                coin.get("name", ""),
                "Industry",
                cat,
                "in_category",
                {},
                "coingecko",
                0.75,
            )

    for exch in [
        "Binance",
        "Coinbase",
        "Kraken",
        "KuCoin",
        "Bybit",
        "OKX",
        "Gemini",
        "Bitfinex",
    ]:
        add_entity(
            conn,
            "FinancialInstitution",
            exch,
            {"type": "Crypto Exchange"},
            "coingecko",
            0.9,
        )
        for coin in data[:10]:
            add_connection(
                conn,
                "FinancialInstrument",
                coin.get("name", ""),
                "FinancialInstitution",
                exch,
                "traded_on",
                {},
                "coingecko",
                0.7,
            )
            count += 1

    conn.commit()
    return count


def load_countries(conn, data):
    count = 0
    regions = set()

    for country in data[:50]:
        name = country.get("name", {}).get("common", "")
        code = country.get("cca2", "")
        region = country.get("region", "Unknown")
        subregion = country.get("subregion", "")

        if not name or not code:
            continue

        add_entity(
            conn,
            "Location",
            name,
            {"iso_code": code, "region": region, "subregion": subregion},
            "restcountries",
            0.95,
        )
        count += 1

        if region and region != "Unknown":
            add_connection(
                conn,
                "Location",
                name,
                "Jurisdiction",
                region,
                "in_region",
                {},
                "restcountries",
                0.9,
            )
            regions.add(region)

    for region in regions:
        add_entity(
            conn, "Jurisdiction", region, {"type": "UN Region"}, "restcountries", 0.9
        )

    conn.commit()
    return count


def sync_pending(conn):
    """Try to sync pending data to API"""
    headers = {
        "Authorization": f"Bearer {GRAPH_SECRET}",
        "Content-Type": "application/json",
    }

    entities = conn.execute(
        "SELECT type, name, properties, source, confidence FROM pending_entities LIMIT 50"
    ).fetchall()
    connections = conn.execute(
        "SELECT from_type, from_name, to_type, to_name, relation, properties, source, confidence FROM pending_connections LIMIT 50"
    ).fetchall()

    if not entities and not connections:
        return 0

    payload = {
        "entities": [
            {
                "type": e[0],
                "name": e[1],
                "properties": json.loads(e[2]),
                "source": e[3],
                "confidence": e[4],
            }
            for e in entities
        ],
        "connections": [
            {
                "from_type": c[0],
                "from_name": c[1],
                "to_type": c[2],
                "to_name": c[3],
                "relation": c[4],
                "properties": json.loads(c[5]),
                "source": c[6],
                "confidence": c[7],
            }
            for c in connections
        ],
    }

    try:
        resp = requests.post(
            f"{GRAPH_URL}/ingest/bulk", headers=headers, json=payload, timeout=30
        )
        print(f"  Response: {resp.status_code} - {resp.text[:200]}")
        if resp.status_code == 201:
            result = resp.json()
            ent_count = result.get("entities", {}).get("added", 0)
            conn_count = result.get("connections", {}).get("added", 0)

            conn.execute(
                "DELETE FROM pending_entities WHERE id IN (SELECT id FROM pending_entities LIMIT 50)"
            )
            conn.execute(
                "DELETE FROM pending_connections WHERE id IN (SELECT id FROM pending_connections LIMIT 50)"
            )
            conn.commit()

            return ent_count + conn_count
    except Exception as e:
        print(f"  Error: {e}")
        return 0

    return 0

    payload = {
        "entities": [
            {
                "type": e[0],
                "name": e[1],
                "properties": json.loads(e[2]),
                "source": e[3],
                "confidence": e[4],
            }
            for e in entities
        ],
        "connections": [
            {
                "from_type": c[0],
                "from_name": c[1],
                "to_type": c[2],
                "to_name": c[3],
                "relation": c[4],
                "properties": json.loads(c[5]),
                "source": c[6],
                "confidence": c[7],
            }
            for c in connections
        ],
    }

    try:
        resp = requests.post(
            f"{GRAPH_URL}/ingest/bulk", headers=headers, json=payload, timeout=30
        )
        if resp.status_code == 201:
            result = resp.json()
            ent_count = result.get("entities", {}).get("added", 0)
            conn_count = result.get("connections", {}).get("added", 0)

            conn.execute(
                "DELETE FROM pending_entities WHERE id IN (SELECT id FROM pending_entities LIMIT 50)"
            )
            conn.execute(
                "DELETE FROM pending_connections WHERE id IN (SELECT id FROM pending_connections LIMIT 50)"
            )
            conn.commit()

            conn.execute(
                "INSERT INTO sync_log (action, count, result, timestamp) VALUES (?, ?, ?, ?)",
                (
                    "sync",
                    ent_count + conn_count,
                    "success",
                    datetime.utcnow().isoformat(),
                ),
            )
            conn.commit()

            return ent_count + conn_count
    except Exception as e:
        conn.execute(
            "INSERT INTO sync_log (action, count, result, timestamp) VALUES (?, ?, ?, ?)",
            ("sync", 0, str(e)[:100], datetime.utcnow().isoformat()),
        )
        conn.commit()
        return 0

    return 0


def main():
    conn = init_db()

    print("Fetching crypto data...")
    crypto_data = fetch_crypto_data()
    print(f"  Loaded {len(crypto_data)} tokens")

    print("Fetching country data...")
    country_data = fetch_countries()
    print(f"  Loaded {len(country_data)} countries")

    print("Populating local cache...")
    ent_count = load_crypto(conn, crypto_data)
    ent_count += load_countries(conn, country_data)
    print(f"  Cached {ent_count} entities")

    print("\nTrying to sync with API...")
    synced = sync_pending(conn)
    print(f"  Synced {synced} records")

    print("\nLocal DB ready for when API is fixed!")


if __name__ == "__main__":
    main()
