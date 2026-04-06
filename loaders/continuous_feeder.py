#!/usr/bin/env python3
"""
Forage Graph Continuous Feeder
Runs all data collectors in sequence forever, feeding the graph
"""

import os
import sys
import time
import json
import requests
import traceback
from datetime import datetime

GRAPH_URL = os.environ.get(
    "GRAPH_URL", "https://forage-graph-production.up.railway.app"
)
GRAPH_SECRET = os.environ['GRAPH_API_SECRET']
HEADERS = {
    "Authorization": f"Bearer {GRAPH_SECRET}",
    "Content-Type": "application/json",
}

print(f"=== FORAGE GRAPH FEEDER ===")
print(f"Graph URL: {GRAPH_URL}")


def check_graph():
    """Check graph connectivity"""
    try:
        resp = requests.get(f"{GRAPH_URL}/health", timeout=10)
        return resp.status_code == 200
    except:
        return False


def log(msg):
    """Log with timestamp"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def run_collector(name, func, *args, **kwargs):
    """Run a collector function"""
    try:
        log(f"Running {name}...")
        result = func(*args, **kwargs)
        if result:
            log(f"✓ {name} complete")
            return True
        else:
            log(f"✗ {name} returned no data")
            return False
    except Exception as e:
        log(f"✗ {name} error: {str(e)[:100]}")
        return False


def add_entities(entities, batch_size=50):
    """Add entities to graph"""
    if not entities:
        return 0
    count = 0
    for i in range(0, len(entities), batch_size):
        batch = entities[i : i + batch_size]
        try:
            resp = requests.post(
                f"{GRAPH_URL}/ingest_raw_batch",
                headers=HEADERS,
                json={"entities": batch},
                timeout=30,
            )
            if resp.status_code in (200, 201, 202):
                count += len(batch)
        except Exception as e:
            log(f"Batch error: {str(e)[:50]}")
    return count


def feed_loop():
    """Main feeding loop"""
    log("Checking graph connection...")
    if not check_graph():
        log("ERROR: Cannot connect to graph!")
        return

    cycle = 0
    while True:
        cycle += 1
        log(f"=== CYCLE {cycle} ===")

        # Import and run collectors
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

        # 1. Wikidata
        try:
            from intel_collector import collect_wikidata

            run_collector("Wikidata", collect_wikidata)
        except Exception as e:
            log(f"Wikidata skip: {e}")

        time.sleep(2)

        # 2. Policy Loader - add corporate connections
        try:
            import policy_loader

            log("Running policy connections...")
            # This module adds connections directly
            if hasattr(policy_loader, "main"):
                policy_loader.main()
        except Exception as e:
            log(f"Policy loader: {e}")

        time.sleep(2)

        # 3. Check stats
        try:
            resp = requests.get(f"{GRAPH_URL}/stats", headers=HEADERS, timeout=10)
            data = resp.json()
            log(
                f"Graph: {data.get('total_entities', 0)} entities, {data.get('total_relationships', 0)} relations"
            )
        except Exception as e:
            log(f"Stats error: {e}")

        # Sleep before next cycle
        log(f"Sleeping 60s...")
        time.sleep(60)


if __name__ == "__main__":
    try:
        feed_loop()
    except KeyboardInterrupt:
        log("Stopped by user")
    except Exception as e:
        log(f"FATAL: {e}")
        traceback.print_exc()
