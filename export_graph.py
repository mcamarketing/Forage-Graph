#!/usr/bin/env python3
"""
Export graph data from Railway FalkorDB to JSON.
Usage: python export_graph.py [--output export.json]
"""

import os
import json
import argparse
import requests
from datetime import datetime

GRAPH_URL = os.environ.get(
    "GRAPH_URL", "https://forage-graph-production.up.railway.app"
)
GRAPH_SECRET = os.environ.get("GRAPH_API_SECRET")

HEADERS = {
    "Authorization": f"Bearer {GRAPH_SECRET}",
    "Content-Type": "application/json",
}


def get_stats():
    """Get current graph stats."""
    resp = requests.get(f"{GRAPH_URL}/stats", headers=HEADERS, timeout=30)
    if resp.status_code == 200:
        return resp.json()
    return {}


def export_all_entities(limit=10000):
    """Export all entities from the graph."""
    entities = []
    offset = 0
    batch = limit

    while offset < limit:
        try:
            resp = requests.post(
                f"{GRAPH_URL}/query",
                json={"name": "*", "type": "any", "min_confidence": 0.0},
                headers=HEADERS,
                timeout=30,
            )
            if resp.status_code != 200:
                break
            data = resp.json()
            ents = data.get("entities", [])
            if not ents:
                break
            entities.extend(ents)
            offset += len(ents)
            print(f"  Exported {len(entities)} entities...")
        except Exception as e:
            print(f"  Error: {e}")
            break

    return entities


def export_all_relationships(limit=50000):
    """Export all relationships from the graph."""
    relationships = []

    # Query for connected entities to get relationships
    try:
        # Get some sample entities first
        resp = requests.post(
            f"{GRAPH_URL}/query",
            json={"name": "*", "type": "any", "min_confidence": 0.0},
            headers=HEADERS,
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"  Query failed: {resp.status_code}")
            return relationships

        data = resp.json()
        entities = data.get("entities", [])[:500]  # Limit for performance

        for ent in entities:
            ent_name = ent.get("name")
            if not ent_name:
                continue
            try:
                # Get claims for this entity (contains relationships)
                claims_resp = requests.get(
                    f"{GRAPH_URL}/claims/{requests.utils.quote(ent_name)}",
                    headers=HEADERS,
                    timeout=10,
                )
                if claims_resp.status_code == 200:
                    claims_data = claims_resp.json()
                    claims = claims_data.get("claims", [])
                    for claim in claims:
                        relationships.append(
                            {
                                "from": ent_name,
                                "relation": claim.get("relation"),
                                "to": claim.get("target"),
                                "assertion": claim.get("assertion"),
                                "source": claim.get("source"),
                                "confidence": claim.get("confidence"),
                            }
                        )
            except Exception as e:
                continue

            if len(relationships) >= limit:
                break

        print(f"  Exported {len(relationships)} relationships...")

    except Exception as e:
        print(f"  Error exporting relationships: {e}")

    return relationships


def main():
    parser = argparse.ArgumentParser(description="Export FalkorDB graph to JSON")
    parser.add_argument(
        "--output", default="forage_graph_export.json", help="Output file"
    )
    parser.add_argument(
        "--limit", type=int, default=10000, help="Max entities to export"
    )
    args = parser.parse_args()

    print("=" * 50)
    print("FalkorDB Graph Exporter")
    print("=" * 50)

    # Get stats
    print("\n[1/3] Getting graph stats...")
    stats = get_stats()
    print(f"  Total entities: {stats.get('total_entities', 'unknown')}")
    print(f"  Total relationships: {stats.get('total_relationships', 'unknown')}")

    # Export entities
    print(f"\n[2/3] Exporting entities (limit={args.limit})...")
    entities = export_all_entities(limit=args.limit)
    print(f"  Exported {len(entities)} entities")

    # Export relationships
    print(f"\n[3/3] Exporting relationships...")
    relationships = export_all_relationships(limit=50000)
    print(f"  Exported {len(relationships)} relationships")

    # Write output
    export_data = {
        "exported_at": datetime.utcnow().isoformat(),
        "stats": stats,
        "entities": entities,
        "relationships": relationships,
    }

    with open(args.output, "w") as f:
        json.dump(export_data, f, indent=2, default=str)

    print(f"\n✓ Exported to {args.output}")
    print(f"  Entities: {len(entities)}")
    print(f"  Relationships: {len(relationships)}")


if __name__ == "__main__":
    main()
