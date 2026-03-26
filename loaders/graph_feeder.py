#!/usr/bin/env python3
"""
Forage Graph Feeder - Continuous data ingestion
Runs all loaders in sequence, then repeats forever
"""

import os
import sys
import time
import subprocess
import threading

GRAPH_URL = os.environ.get(
    "GRAPH_URL", "https://forage-graph-production.up.railway.app"
)
GRAPH_SECRET = os.environ.get(
    "GRAPH_SECRET", "6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959"
)

# Loaders to run in sequence
LOADERS = [
    ("Policy Loader", "python policy_loader.py"),
    ("Connection Policy", "python connection_policy.py"),
    ("Bulk Connections", "python bulk_connections.py"),
    ("Add Relationships", "python add_relationships.py"),
    ("Wikidata Loader", "python wikidata_loader.py"),
    ("GLEIF Loader", "python gleif_loader.py"),
    ("Intel Collector", "python intel_collector.py"),
]


def run_loader(name, cmd, timeout=120):
    """Run a single loader with timeout"""
    print(f"[{name}] Starting...")
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        if result.returncode == 0:
            print(f"[{name}] ✓ Complete")
            return True
        else:
            print(f"[{name}] ✗ Failed: {result.stderr[:200]}")
            return False
    except subprocess.TimeoutExpired:
        print(f"[{name}] ⚠ Timeout after {timeout}s")
        return False
    except Exception as e:
        print(f"[{name}] ✗ Error: {e}")
        return False


def feed_graph_forever(interval=300):
    """Main loop - feed graph continuously"""
    print("=" * 60)
    print("FORAGE GRAPH FEEDER STARTED")
    print(f"Graph: {GRAPH_URL}")
    print(f"Interval: {interval}s between cycles")
    print("=" * 60)

    cycle = 0
    while True:
        cycle += 1
        print(f"\n--- CYCLE {cycle} ---")

        # Run all loaders
        success_count = 0
        for name, cmd in LOADERS:
            if run_loader(name, cmd, timeout=180):
                success_count += 1
            time.sleep(2)  # Brief pause between loaders

        print(f"Cycle {cycle}: {success_count}/{len(LOADERS)} loaders succeeded")

        # Check graph stats
        try:
            import requests

            resp = requests.get(
                f"{GRAPH_URL}/stats",
                headers={"Authorization": f"Bearer {GRAPH_SECRET}"},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                print(
                    f"Graph status: {data.get('total_entities', 0)} entities, {data.get('total_relationships', 0)} relations"
                )
        except Exception as e:
            print(f"Stats check failed: {e}")

        print(f"Sleeping {interval}s before next cycle...")
        time.sleep(interval)


if __name__ == "__main__":
    interval = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    feed_graph_forever(interval)
