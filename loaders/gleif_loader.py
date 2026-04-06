"""
GLEIF Bulk Data Loader
======================
Loads the Global Legal Entity Identifier Foundation (GLEIF) Level 1 (entity data)
and Level 2 (ownership/relationship data) bulk files into the Forage Graph.

License: CC0 — no restrictions, no rate limits.

Data provides:
- 2.3M+ legal entities (companies, funds, banks) with LEI codes
- Canonical legal names, registered addresses, jurisdiction
- Level 2: parent-child ownership relationships (who owns whom)

Usage:
  python3 gleif_loader.py

Requirements:
  pip install requests

The script downloads the latest concatenated files (~500MB compressed),
streams them to avoid memory issues, and POSTs batches of 25 to the graph.
"""

import os
import json
import requests
import zipfile
import csv
import io
import time
from datetime import datetime

GRAPH_URL = os.environ.get("GRAPH_URL", "https://forage-graph-production.up.railway.app")
GRAPH_SECRET = os.environ['GRAPH_API_SECRET']
BATCH_SIZE = 25
MAX_ENTITIES = int(os.environ.get("GLEIF_MAX_ENTITIES", "50000"))  # set to 0 for full load

GLEIF_L1_URL = "https://leidata.gleif.org/api/v1/concatenated-files/lei2/current/zip"
GLEIF_L2_URL = "https://leidata.gleif.org/api/v1/concatenated-files/rr/current/zip"

HEADERS = {
    "Authorization": f"Bearer {GRAPH_SECRET}",
    "Content-Type": "application/json"
}


def post_batch(batch: list) -> dict:
    """POST a batch of entities to the graph API."""
    try:
        resp = requests.post(
            f"{GRAPH_URL}/ingest_raw_batch",
            headers=HEADERS,
            json={"batch": batch},
            timeout=30
        )
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


def stream_gleif_l1(max_entities: int = 0):
    """
    Stream GLEIF Level 1 (entity) data and yield batches.
    Each entity becomes a 'company' node with LEI as canonical ID.
    """
    print(f"[GLEIF L1] Downloading from {GLEIF_L1_URL} ...")
    resp = requests.get(GLEIF_L1_URL, stream=True, timeout=120)
    resp.raise_for_status()

    # Write to temp file (zip is ~500MB)
    tmp_path = "/tmp/gleif_l1.zip"
    with open(tmp_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
    print(f"[GLEIF L1] Downloaded. Processing...")

    batch = []
    count = 0
    total_batches = 0

    with zipfile.ZipFile(tmp_path) as zf:
        # Find the CSV file inside the zip
        csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_files:
            print("[GLEIF L1] No CSV found in zip")
            return

        with zf.open(csv_files[0]) as csvfile:
            reader = csv.DictReader(io.TextIOWrapper(csvfile, encoding="utf-8"))
            for row in reader:
                lei = row.get("LEI", "").strip()
                name = row.get("Entity.LegalName", "").strip()
                if not lei or not name:
                    continue

                jurisdiction = row.get("Entity.LegalJurisdiction", "")
                country = jurisdiction.split("-")[0] if jurisdiction else ""
                status = row.get("Entity.EntityStatus", "")
                category = row.get("Entity.EntityCategory", "")
                legal_form = row.get("Entity.LegalForm.OtherLegalForm", "")
                city = row.get("Entity.LegalAddress.City", "")
                registered = row.get("Registration.InitialRegistrationDate", "")

                entity = {
                    "name": name,
                    "type": "company",
                    "properties": {
                        "lei": lei,
                        "jurisdiction": jurisdiction,
                        "country": country,
                        "status": status,
                        "category": category,
                        "legal_form": legal_form,
                        "city": city,
                        "registered": registered,
                        "provenance": ["GLEIF"],
                        "valid_from": registered or datetime.utcnow().isoformat()
                    },
                    "relationships": [],
                    "source": "GLEIF",
                    "confidence": 0.97,
                    "causal_weight": 0.0,
                    "mechanism": "entity_registry"
                }

                # Link to country node
                if country:
                    entity["relationships"].append({
                        "targetName": country,
                        "targetType": "country",
                        "relation": "registered_in",
                        "confidence": 0.99,
                        "causal_weight": 0.1,
                        "mechanism": "jurisdiction"
                    })

                batch.append(entity)
                count += 1

                if len(batch) >= BATCH_SIZE:
                    result = post_batch(batch)
                    total_batches += 1
                    if total_batches % 100 == 0:
                        print(f"[GLEIF L1] Processed {count} entities ({total_batches} batches)")
                    batch = []

                if max_entities > 0 and count >= max_entities:
                    print(f"[GLEIF L1] Reached max_entities limit ({max_entities})")
                    break

    if batch:
        post_batch(batch)
        total_batches += 1

    print(f"[GLEIF L1] Complete. {count} entities in {total_batches} batches.")
    return count


def stream_gleif_l2(max_relationships: int = 0):
    """
    Stream GLEIF Level 2 (relationship) data and yield ownership edges.
    Creates 'subsidiary_of' and 'owned_by' relationships between LEI entities.
    """
    print(f"[GLEIF L2] Downloading from {GLEIF_L2_URL} ...")
    resp = requests.get(GLEIF_L2_URL, stream=True, timeout=120)
    resp.raise_for_status()

    tmp_path = "/tmp/gleif_l2.zip"
    with open(tmp_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
    print(f"[GLEIF L2] Downloaded. Processing...")

    batch = []
    count = 0
    total_batches = 0

    with zipfile.ZipFile(tmp_path) as zf:
        csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_files:
            print("[GLEIF L2] No CSV found in zip")
            return

        with zf.open(csv_files[0]) as csvfile:
            reader = csv.DictReader(io.TextIOWrapper(csvfile, encoding="utf-8"))
            for row in reader:
                child_lei = row.get("Relationship.StartNode.NodeID", "").strip()
                parent_lei = row.get("Relationship.EndNode.NodeID", "").strip()
                rel_type = row.get("Relationship.RelationshipType", "").strip()
                status = row.get("Relationship.RelationshipStatus", "").strip()

                if not child_lei or not parent_lei or status != "ACTIVE":
                    continue

                # We use LEI as the entity name for relationship lookups
                # (the graph will match by name, which we set to LEI in L1 load)
                relation = "subsidiary_of" if rel_type == "IS_DIRECTLY_CONSOLIDATED_BY" else "owned_by"

                entity = {
                    "name": child_lei,
                    "type": "company",
                    "properties": {"lei": child_lei, "provenance": ["GLEIF_L2"]},
                    "relationships": [{
                        "targetName": parent_lei,
                        "targetType": "company",
                        "relation": relation,
                        "confidence": 0.97,
                        "causal_weight": 0.6,
                        "mechanism": "ownership_chain"
                    }],
                    "source": "GLEIF",
                    "confidence": 0.97,
                    "causal_weight": 0.6,
                    "mechanism": "ownership_chain"
                }

                batch.append(entity)
                count += 1

                if len(batch) >= BATCH_SIZE:
                    post_batch(batch)
                    total_batches += 1
                    if total_batches % 100 == 0:
                        print(f"[GLEIF L2] Processed {count} relationships ({total_batches} batches)")
                    batch = []

                if max_relationships > 0 and count >= max_relationships:
                    print(f"[GLEIF L2] Reached limit ({max_relationships})")
                    break

    if batch:
        post_batch(batch)
        total_batches += 1

    print(f"[GLEIF L2] Complete. {count} ownership relationships in {total_batches} batches.")
    return count


if __name__ == "__main__":
    print("=" * 60)
    print("GLEIF Bulk Loader — Forage Graph")
    print(f"Target: {GRAPH_URL}")
    print(f"Max entities: {MAX_ENTITIES if MAX_ENTITIES > 0 else 'unlimited'}")
    print("=" * 60)

    start = time.time()

    # Load Level 1 (entities) first
    l1_count = stream_gleif_l1(MAX_ENTITIES)

    # Load Level 2 (ownership relationships)
    l2_count = stream_gleif_l2(MAX_ENTITIES)

    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.1f}s")
    print(f"L1 entities loaded : {l1_count}")
    print(f"L2 relationships   : {l2_count}")
