#!/usr/bin/env python3
"""
Regime-Aware Connection Policy Extensions
==========================================

Extends connection_policy.py with:
- Minimum causal density for MacroEvents (5-10 entities, 2-3 channels, 1 narrative, 1 regime)
- Channel-based propagation requirements
- Narrative connectivity enforcement
- Portfolio exposure scoring per regime/channel
- DataSource and RawData provenance enforcement

[policy-regime-001]
"""

import os
import json
import hashlib
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

GRAPH_URL = os.environ.get("GRAPH_URL", "https://forage-graph-production.up.railway.app")
GRAPH_SECRET = os.environ.get("GRAPH_SECRET", "")

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {GRAPH_SECRET}",
}

# ═══════════════════════════════════════════════════════════════════════════════
# CHANNEL TYPES — canonical list
# ═══════════════════════════════════════════════════════════════════════════════

CHANNEL_TYPES = [
    "credit",          # defaults, downgrades, spread widening
    "liquidity",       # funding freeze, bid-ask widening, repo stress
    "sentiment",       # panic, herd behavior, narrative contagion
    "supply_chain",    # shortages, delays, price spikes
    "regulation",      # new rules, enforcement, policy shifts
    "counterparty",    # defaults, margin calls, collateral chains
]

CHANNEL_IDS = {ch: f"channel-{ch.replace('_', '-')}" for ch in CHANNEL_TYPES}

# ═══════════════════════════════════════════════════════════════════════════════
# REGIME TYPES
# ═══════════════════════════════════════════════════════════════════════════════

REGIME_TYPES = ["normal", "stressed", "pre_tipping", "post_event"]
REGIME_IDS = {r: f"regime-{r.replace('_', '-')}" for r in REGIME_TYPES}

# ═══════════════════════════════════════════════════════════════════════════════
# EXTENDED POLICIES
# ═══════════════════════════════════════════════════════════════════════════════

REGIME_AWARE_POLICIES = {
    # Every MacroEvent must have minimum causal density
    "MacroEvent": {
        "min_entity_links": 5,
        "max_entity_links": 10,
        "min_channel_links": 2,
        "max_channel_links": 4,
        "min_narrative_links": 1,
        "min_regime_links": 1,
        "required_relationships": [
            ("impacts", ["Corporation", "Company", "FinancialInstitution", "Asset"], 3),
            ("affects", ["Industry", "EconomicSector"], 2),
            ("TRANSMITS_THROUGH", ["Channel"], 2),
            ("DRIVES_NARRATIVE", ["Narrative"], 1),
            ("IN_REGIME", ["Regime"], 1),
        ],
    },

    # Every MarketCrash must have full causal chain
    "MarketCrash": {
        "min_entity_links": 8,
        "min_channel_links": 3,
        "min_narrative_links": 1,
        "min_regime_links": 1,
        "required_relationships": [
            ("CRASHED", ["Corporation", "Company", "Asset", "FinancialInstitution"], 5),
            ("TRANSMITTED_THROUGH", ["Channel"], 2),
            ("DRIVES_NARRATIVE", ["Narrative"], 1),
            ("IN_REGIME", ["Regime"], 1),
            ("PART_OF_CHAIN", ["CausalChain"], 1),
        ],
    },

    # Every Channel must connect to entities it affects
    "Channel": {
        "min_entity_links": 5,
        "required_relationships": [
            ("CHANNEL_AFFECTS", ["Corporation", "Asset", "Industry"], 3),
        ],
    },

    # Every Narrative must link to entities and events
    "Narrative": {
        "min_entity_links": 2,
        "required_relationships": [
            ("NARRATIVE_INFLUENCES", ["Corporation", "Asset", "Industry"], 2),
        ],
    },

    # Portfolio must have exposure scores
    "Portfolio": {
        "min_entity_links": 1,
        "required_relationships": [
            ("PORTFOLIO_HOLDS", ["Corporation", "Asset"], 1),
            ("PORTFOLIO_EXPOSED_TO", ["Channel"], 1),
        ],
    },

    # DataSource must link to RawData
    "DataSource": {
        "min_entity_links": 1,
        "required_relationships": [
            ("EXTRACTED_FROM", ["RawData"], 1),
        ],
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# POLICY ENFORCEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def check_entity_compliance(entity_id: str, entity_type: str) -> Dict[str, Any]:
    """
    Check if an entity meets the regime-aware policy requirements.
    Returns compliance report with missing relationships.
    """
    policy = REGIME_AWARE_POLICIES.get(entity_type)
    if not policy:
        return {"entity_id": entity_id, "compliant": True, "policy": "none"}

    # Get current neighbors
    try:
        resp = requests.post(
            f"{GRAPH_URL}/enrich",
            headers=HEADERS,
            json={"entity": entity_id},
            timeout=10,
        )
        data = resp.json()
    except Exception as e:
        return {"entity_id": entity_id, "compliant": False, "error": str(e)}

    relationships = data.get("relationships", [])

    # Count by type
    rel_counts: Dict[str, int] = {}
    for rel in relationships:
        rel_type = rel.get("relation", "unknown")
        neighbor_type = rel.get("neighbor_type", "unknown")
        key = f"{rel_type}→{neighbor_type}"
        rel_counts[key] = rel_counts.get(key, 0) + 1

    # Check required relationships
    missing = []
    for rel_name, target_types, min_count in policy.get("required_relationships", []):
        count = 0
        for t in target_types:
            count += rel_counts.get(f"{rel_name}→{t}", 0)
        if count < min_count:
            missing.append({
                "relation": rel_name,
                "target_types": target_types,
                "required": min_count,
                "found": count,
            })

    total_links = len(relationships)
    min_links = policy.get("min_entity_links", 0)

    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "compliant": len(missing) == 0 and total_links >= min_links,
        "total_links": total_links,
        "min_required": min_links,
        "missing_relationships": missing,
    }


def enforce_macro_event_density(event_id: str, event_data: Dict[str, Any]) -> List[Dict]:
    """
    Enforce minimum causal density for a new MacroEvent.
    Returns list of relationships that were auto-created.
    """
    created = []
    event_name = event_data.get("name", "")

    # 1. Ensure at least 2 channel links
    channels = event_data.get("channels", [])
    if len(channels) < 2:
        # Auto-assign sentiment + the most likely secondary channel
        channels = list(set(channels + ["sentiment", "liquidity"]))

    for ch in channels[:4]:
        ch_id = CHANNEL_IDS.get(ch)
        if ch_id:
            rel = {
                "from_name": event_name,
                "to_id": ch_id,
                "relation": "TRANSMITS_THROUGH",
                "confidence": 0.7,
            }
            _post_connection(rel)
            created.append(rel)

    # 2. Ensure at least 1 narrative link
    narratives = event_data.get("narratives", [])
    if not narratives:
        # Auto-create a default narrative from the event
        narr_name = f"Impact of {event_name}"
        narr_id = f"narr-auto-{hashlib.md5(narr_name.encode()).hexdigest()[:8]}"
        _create_entity(narr_id, narr_name, "Narrative", {
            "theme": "auto_generated",
            "sentiment_score": event_data.get("sentiment", -0.5),
        })
        _post_connection({
            "from_name": event_name,
            "to_id": narr_id,
            "relation": "DRIVES_NARRATIVE",
            "confidence": 0.6,
        })
        created.append({"auto_narrative": narr_name})

    # 3. Ensure at least 1 regime link
    regime = event_data.get("regime", "normal")
    regime_id = REGIME_IDS.get(regime, REGIME_IDS["normal"])
    _post_connection({
        "from_name": event_name,
        "to_id": regime_id,
        "relation": "IN_REGIME",
        "confidence": 0.8,
    })
    created.append({"regime_link": regime})

    return created


def enforce_portfolio_exposure(
    portfolio_id: str,
    positions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Compute and store portfolio exposure scores per regime and channel.
    Returns the computed exposure map.
    """
    channel_exposures = {ch: 0.0 for ch in CHANNEL_TYPES}
    regime_exposures = {r: 0.0 for r in REGIME_TYPES}
    total_weight = sum(p.get("weight", 0) for p in positions)

    if total_weight == 0:
        return {"channel_exposures": channel_exposures, "regime_exposures": regime_exposures}

    for pos in positions:
        entity = pos.get("entity", "")
        weight = pos.get("weight", 0) / total_weight

        # Get entity's channel and regime exposure
        try:
            resp = requests.post(
                f"{GRAPH_URL}/enrich",
                headers=HEADERS,
                json={"entity": entity},
                timeout=10,
            )
            data = resp.json()
            for rel in data.get("relationships", []):
                rel_type = rel.get("relation", "")
                neighbor = rel.get("neighbor", {})

                if neighbor.get("type") == "Channel":
                    ch_type = neighbor.get("channel_type", "")
                    if ch_type in channel_exposures:
                        conf = rel.get("confidence", 0.5)
                        channel_exposures[ch_type] += weight * conf

                if neighbor.get("type") == "Regime":
                    regime_state = neighbor.get("regime_state", "")
                    if regime_state in regime_exposures:
                        conf = rel.get("confidence", 0.5)
                        regime_exposures[regime_state] += weight * conf
        except Exception:
            continue

    # Store as PORTFOLIO_EXPOSED_TO edges
    for ch, score in channel_exposures.items():
        if score > 0:
            _post_connection({
                "from_id": portfolio_id,
                "to_id": CHANNEL_IDS[ch],
                "relation": "PORTFOLIO_EXPOSED_TO",
                "confidence": min(score, 1.0),
                "properties": {"score": round(score, 4)},
            })

    return {
        "portfolio_id": portfolio_id,
        "channel_exposures": {k: round(v, 4) for k, v in channel_exposures.items()},
        "regime_exposures": {k: round(v, 4) for k, v in regime_exposures.items()},
    }


# ═══════════════════════════════════════════════════════════════════════════════
# DATA SOURCE & PROVENANCE TRACKING
# ═══════════════════════════════════════════════════════════════════════════════

DATA_SOURCES = {
    "fred": {"type": "api", "name": "FRED (Federal Reserve Economic Data)", "cadence": "daily", "url": "https://api.stlouisfed.org"},
    "bis": {"type": "api", "name": "Bank for International Settlements", "cadence": "monthly", "url": "https://data.bis.org"},
    "wikidata": {"type": "sparql", "name": "Wikidata", "cadence": "daily", "url": "https://query.wikidata.org"},
    "opensanctions": {"type": "api", "name": "OpenSanctions", "cadence": "6h", "url": "https://data.opensanctions.org"},
    "sec_edgar": {"type": "api", "name": "SEC EDGAR", "cadence": "12h", "url": "https://efts.sec.gov"},
    "worldbank": {"type": "api", "name": "World Bank", "cadence": "daily", "url": "https://api.worldbank.org"},
    "news_rss": {"type": "rss", "name": "News RSS Feeds", "cadence": "15m", "url": "multiple"},
    "gdelt": {"type": "api", "name": "GDELT Project", "cadence": "30m", "url": "https://api.gdeltproject.org"},
    "crypto": {"type": "api", "name": "Crypto Price Feeds", "cadence": "1h", "url": "https://api.coingecko.com"},
    "github": {"type": "api", "name": "GitHub Trending", "cadence": "2h", "url": "https://api.github.com"},
}


def register_data_source(source_key: str) -> Optional[str]:
    """Register a DataSource node in the graph for provenance tracking."""
    source = DATA_SOURCES.get(source_key)
    if not source:
        return None

    source_id = f"datasource-{source_key}"
    _create_entity(source_id, source["name"], "DataSource", {
        "source_type": source["type"],
        "url": source["url"],
        "cadence": source["cadence"],
    })
    return source_id


def create_raw_data_node(
    content: str,
    source_key: str,
    timestamp: Optional[str] = None,
) -> str:
    """
    Create a RawData node with content hash for deduplication.
    Links to its DataSource via EXTRACTED_FROM.
    """
    content_hash = hashlib.sha256(content.encode()).hexdigest()
    raw_id = f"raw-{content_hash[:16]}"
    ts = timestamp or datetime.utcnow().isoformat()

    _create_entity(raw_id, f"RawData {raw_id}", "RawData", {
        "content_hash": content_hash,
        "text": content[:500],  # Truncate for graph storage
        "timestamp": ts,
        "source_key": source_key,
    })

    # Link to DataSource
    source_id = f"datasource-{source_key}"
    _post_connection({
        "from_id": raw_id,
        "to_id": source_id,
        "relation": "EXTRACTED_FROM",
        "confidence": 1.0,
    })

    return raw_id


def link_raw_data_to_entities(
    raw_data_id: str,
    entity_ids: List[str],
    relation: str = "CONTAINS",
):
    """Link a RawData node to entities extracted from it."""
    for eid in entity_ids:
        _post_connection({
            "from_id": raw_data_id,
            "to_id": eid,
            "relation": relation,
            "confidence": 0.8,
        })


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _create_entity(entity_id: str, name: str, entity_type: str, properties: Dict = None):
    """Create or merge an entity via the graph API."""
    try:
        requests.post(
            f"{GRAPH_URL}/ingest/entities",
            headers=HEADERS,
            json={"entities": [{
                "id": entity_id,
                "name": name,
                "type": entity_type,
                "properties": properties or {},
                "confidence": 0.9,
            }]},
            timeout=10,
        )
    except Exception:
        pass


def _post_connection(rel: Dict):
    """Create a relationship via the graph API."""
    try:
        conn = {
            "relation": rel.get("relation", "related_to"),
            "confidence": rel.get("confidence", 0.8),
        }

        if "from_name" in rel:
            conn["from_name"] = rel["from_name"]
        elif "from_id" in rel:
            conn["from_id"] = rel["from_id"]

        if "to_name" in rel:
            conn["to_name"] = rel["to_name"]
        elif "to_id" in rel:
            conn["to_id"] = rel["to_id"]

        for k in ["causal_weight", "mechanism", "lag_hours", "properties"]:
            if k in rel:
                conn[k] = rel[k]

        requests.post(
            f"{GRAPH_URL}/ingest/connections",
            headers=HEADERS,
            json={"connections": [conn]},
            timeout=10,
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Regime-Aware Policy Check")
    parser.add_argument("--check", help="Check compliance for entity ID")
    parser.add_argument("--type", default="MacroEvent", help="Entity type")
    parser.add_argument("--register-sources", action="store_true",
                        help="Register all data sources in graph")
    args = parser.parse_args()

    if args.register_sources:
        for key in DATA_SOURCES:
            sid = register_data_source(key)
            print(f"  Registered: {key} → {sid}")
        print("Done.")

    elif args.check:
        report = check_entity_compliance(args.check, args.type)
        print(json.dumps(report, indent=2))
