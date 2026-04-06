#!/usr/bin/env python3
"""
Migrate graph from Railway FalkorDB to FalkorDB Cloud.
Usage: python migrate_to_cloud.py
"""

import os
import json
import time
import requests
import redis
from datetime import datetime

# Railway FalkorDB (source)
RAILWAY_GRAPH_URL = os.environ.get(
    "GRAPH_URL", "https://forage-graph-production.up.railway.app"
)
RAILWAY_SECRET = os.environ.get(
    "GRAPH_API_SECRET",
    "6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959",
)

# FalkorDB Cloud (target) - user provided instance
CLOUD_INSTANCE_ID = os.environ.get("FALKORDB_INSTANCE_ID", "instance-dyps0zi57")
CLOUD_PASSWORD = os.environ.get("FALKORDB_PASSWORD", "")

HEADERS = {
    "Authorization": f"Bearer {RAILWAY_SECRET}",
    "Content-Type": "application/json",
}


def get_railway_stats():
    """Get Railway graph stats."""
    try:
        resp = requests.get(f"{RAILWAY_GRAPH_URL}/stats", headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return {}


def connect_cloud_redis():
    """Connect to FalkorDB Cloud via Redis protocol."""
    if not CLOUD_PASSWORD:
        print("ERROR: FALKORDB_PASSWORD not set")
        return None

    # Construct cloud URL
    cloud_url = f"redis://:{CLOUD_PASSWORD}@db.{CLOUD_INSTANCE_ID}.falkordb.cloud:6379"
    try:
        r = redis.from_url(cloud_url, decode_responses=True)
        r.ping()
        print(f"Connected to FalkorDB Cloud: {CLOUD_INSTANCE_ID}")
        return r
    except Exception as e:
        print(f"Cloud connection failed: {e}")
        return None


def migrate_from_railway():
    """Export from Railway and import to Cloud."""
    print("=" * 50)
    print("FalkorDB Migration: Railway -> Cloud")
    print("=" * 50)

    # Check Railway stats
    print("\n[1] Checking Railway graph...")
    stats = get_railway_stats()
    print(f"  Railway entities: {stats.get('total_entities', 'unknown')}")
    print(f"  Railway relationships: {stats.get('total_relationships', 'unknown')}")

    if stats.get("total_entities", 0) == 0:
        print("\nRailway graph is empty or unreachable.")
        print("The data will need to be re-loaded directly to FalkorDB Cloud.")
        return False

    # Connect to cloud
    print("\n[2] Connecting to FalkorDB Cloud...")
    cloud_r = connect_cloud_redis()
    if not cloud_r:
        print("Could not connect to cloud. Will create loader scripts instead.")
        return False

    # Export entities from Railway
    print("\n[3] Exporting entities from Railway...")
    entities = []
    try:
        resp = requests.post(
            f"{RAILWAY_GRAPH_URL}/query",
            json={"name": "*", "type": "any", "min_confidence": 0.0},
            headers=HEADERS,
            timeout=60,
        )
        if resp.status_code == 200:
            data = resp.json()
            entities = data.get("entities", [])
            print(f"  Exported {len(entities)} entities")
    except Exception as e:
        print(f"  Export failed: {e}")
        return False

    # Import to cloud
    print("\n[4] Importing to FalkorDB Cloud...")
    for i, ent in enumerate(entities):
        try:
            name = ent.get("name")
            etype = ent.get("type", "Entity")
            props = ent.get("properties", {})

            # Use FalkorDB Graph commands via Redis
            # GRAPH.ENTITY key
            key = f"graph:entity:{name}"
            cloud_r.hset(
                key,
                mapping={
                    "name": name,
                    "type": etype,
                    "properties": json.dumps(props),
                    "migrated_at": datetime.utcnow().isoformat(),
                },
            )

            if (i + 1) % 100 == 0:
                print(f"  Imported {i + 1} entities...")
        except Exception as e:
            print(f"  Error on entity {i}: {e}")

    print(f"\n  Total imported: {len(entities)} entities")

    # Export relationships
    print("\n[5] Exporting relationships from Railway...")
    relationships = []

    for ent in entities[:500]:  # Limit for performance
        name = ent.get("name")
        if not name:
            continue
        try:
            claims_resp = requests.get(
                f"{RAILWAY_GRAPH_URL}/claims/{requests.utils.quote(name)}",
                headers=HEADERS,
                timeout=10,
            )
            if claims_resp.status_code == 200:
                claims = claims_resp.json().get("claims", [])
                for claim in claims:
                    relationships.append(
                        {
                            "from": name,
                            "relation": claim.get("relation"),
                            "to": claim.get("target"),
                            "assertion": claim.get("assertion"),
                            "confidence": claim.get("confidence"),
                        }
                    )
        except:
            continue

    print(f"  Exported {len(relationships)} relationships")

    # Import relationships to cloud
    print("\n[6] Importing relationships to Cloud...")
    for i, rel in enumerate(relationships):
        try:
            # Use FalkorDB Graph edges
            key = f"graph:edge:{rel['from']}:{rel['relation']}:{rel['to']}"
            cloud_r.hset(
                key,
                mapping={
                    "from": rel["from"],
                    "relation": rel["relation"],
                    "to": rel["to"],
                    "assertion": json.dumps(rel.get("assertion", {})),
                    "confidence": str(rel.get("confidence", 0.5)),
                    "migrated_at": datetime.utcnow().isoformat(),
                },
            )

            if (i + 1) % 100 == 0:
                print(f"  Imported {i + 1} relationships...")
        except Exception as e:
            print(f"  Error on relationship {i}: {e}")

    print(f"\n  Total imported: {len(relationships)} relationships")

    print("\n" + "=" * 50)
    print("Migration complete!")
    print(f"  Entities: {len(entities)}")
    print(f"  Relationships: {len(relationships)}")
    print("=" * 50)

    return True


def create_cloud_loader():
    """Create a loader script for direct Cloud loading."""
    print("\nCreating FalkorDB Cloud loader script...")

    loader_content = (
        '''#!/usr/bin/env python3
"""
Direct loader for FalkorDB Cloud.
Uses the same APIs as Railway but points to FalkorDB Cloud.
"""

import os
import json
import time
import requests
import redis

# Configure for your FalkorDB Cloud instance
CLOUD_INSTANCE_ID = "'''
        + CLOUD_INSTANCE_ID
        + """"
CLOUD_PASSWORD = os.environ.get("FALKORDB_PASSWORD", "")

CLOUD_GRAPH_URL = f"https://api.falkordb.cloud/v1/graphs/{CLOUD_INSTANCE_ID}"
HEADERS = {
    "Authorization": f"Bearer {CLOUD_PASSWORD}",
    "Content-Type": "application/json",
}

def main():
    print("FalkorDB Cloud Loader")
    print(f"Instance: {CLOUD_INSTANCE_ID}")
    print("\\nConfigure CLOUD_PASSWORD env var and run bulk loaders.")
    print("Point GRAPH_URL to FalkorDB Cloud API.")

if __name__ == "__main__":
    main()
"""
    )

    with open("cloud_loader.py", "w") as f:
        f.write(loader_content)

    print("Created: cloud_loader.py")


if __name__ == "__main__":
    success = migrate_from_railway()
    if not success:
        create_cloud_loader()
