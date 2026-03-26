#!/usr/bin/env python3
"""
Graph Connection Policy Specification
======================================
Enforces minimum edge requirements per entity type and defines
standard macro-event patterns for the Reality Graph.

Target: 5-10 edges per node
Current: ~0.53 edges per node
"""

from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional
from enum import Enum

# ═══════════════════════════════════════════════════════════════════════════════
# ENTITY TYPES
# ═══════════════════════════════════════════════════════════════════════════════

class EntityType(Enum):
    CORPORATION = "Corporation"
    MACRO_EVENT = "MacroEvent"
    PERSON = "Person"
    LOCATION = "Location"
    INDUSTRY = "Industry"
    ASSET = "Asset"
    GOVERNMENT = "Government"
    NARRATIVE = "Narrative"
    INDEX = "Index"
    CENTRAL_BANK = "CentralBank"

# ═══════════════════════════════════════════════════════════════════════════════
# CONNECTION POLICY SPEC
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class EdgeRequirement:
    """Defines a required outgoing edge type."""
    relation: str
    target_type: EntityType
    min_count: int
    max_count: int = 10
    priority: str = "required"  # required, recommended, optional

@dataclass
class EntityPolicy:
    """Connection policy for an entity type."""
    entity_type: EntityType
    min_total_edges: int
    required_edges: List[EdgeRequirement]
    recommended_edges: List[EdgeRequirement] = field(default_factory=list)

# ═══════════════════════════════════════════════════════════════════════════════
# POLICY DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

ENTITY_POLICIES: Dict[EntityType, EntityPolicy] = {
    EntityType.CORPORATION: EntityPolicy(
        entity_type=EntityType.CORPORATION,
        min_total_edges=5,
        required_edges=[
            EdgeRequirement("headquartered_in", EntityType.LOCATION, 1, 1),
            EdgeRequirement("operates_in", EntityType.INDUSTRY, 1, 3),
            EdgeRequirement("competes_with", EntityType.CORPORATION, 2, 5),
        ],
        recommended_edges=[
            EdgeRequirement("invests_in", EntityType.CORPORATION, 0, 5),
            EdgeRequirement("owns", EntityType.CORPORATION, 0, 10),
            EdgeRequirement("procures_from", EntityType.CORPORATION, 0, 5),
            EdgeRequirement("supplies_to", EntityType.CORPORATION, 0, 5),
            EdgeRequirement("partner_of", EntityType.CORPORATION, 0, 3),
            EdgeRequirement("regulated_by", EntityType.GOVERNMENT, 0, 2),
            EdgeRequirement("listed_on", EntityType.INDEX, 0, 3),
        ]
    ),

    EntityType.MACRO_EVENT: EntityPolicy(
        entity_type=EntityType.MACRO_EVENT,
        min_total_edges=8,
        required_edges=[
            EdgeRequirement("impacts", EntityType.CORPORATION, 3, 10),
            EdgeRequirement("affects", EntityType.INDUSTRY, 2, 5),
            EdgeRequirement("occurs_in", EntityType.LOCATION, 1, 3),
        ],
        recommended_edges=[
            EdgeRequirement("moves", EntityType.ASSET, 1, 5),
            EdgeRequirement("involves", EntityType.PERSON, 0, 3),
            EdgeRequirement("issued_by", EntityType.GOVERNMENT, 0, 2),
            EdgeRequirement("signals", EntityType.NARRATIVE, 0, 2),
            EdgeRequirement("precedes", EntityType.MACRO_EVENT, 0, 3),
            EdgeRequirement("amplifies", EntityType.MACRO_EVENT, 0, 2),
        ]
    ),

    EntityType.PERSON: EntityPolicy(
        entity_type=EntityType.PERSON,
        min_total_edges=4,
        required_edges=[
            EdgeRequirement("works_at", EntityType.CORPORATION, 1, 2),
        ],
        recommended_edges=[
            EdgeRequirement("board_member_of", EntityType.CORPORATION, 0, 5),
            EdgeRequirement("invests_in", EntityType.CORPORATION, 0, 10),
            EdgeRequirement("founded", EntityType.CORPORATION, 0, 3),
            EdgeRequirement("connected_to", EntityType.PERSON, 0, 10),
        ]
    ),

    EntityType.INDUSTRY: EntityPolicy(
        entity_type=EntityType.INDUSTRY,
        min_total_edges=5,
        required_edges=[
            EdgeRequirement("contains", EntityType.CORPORATION, 3, 20),
        ],
        recommended_edges=[
            EdgeRequirement("depends_on", EntityType.INDUSTRY, 0, 3),
            EdgeRequirement("supplies", EntityType.INDUSTRY, 0, 3),
            EdgeRequirement("regulated_by", EntityType.GOVERNMENT, 0, 2),
            EdgeRequirement("tracked_by", EntityType.INDEX, 0, 3),
        ]
    ),

    EntityType.ASSET: EntityPolicy(
        entity_type=EntityType.ASSET,
        min_total_edges=4,
        required_edges=[
            EdgeRequirement("issued_by", EntityType.CORPORATION, 1, 1),
        ],
        recommended_edges=[
            EdgeRequirement("correlated_with", EntityType.ASSET, 0, 5),
            EdgeRequirement("tracks", EntityType.INDUSTRY, 0, 2),
            EdgeRequirement("traded_in", EntityType.LOCATION, 0, 3),
            EdgeRequirement("part_of", EntityType.INDEX, 0, 5),
        ]
    ),

    EntityType.GOVERNMENT: EntityPolicy(
        entity_type=EntityType.GOVERNMENT,
        min_total_edges=5,
        required_edges=[
            EdgeRequirement("governs", EntityType.LOCATION, 1, 1),
        ],
        recommended_edges=[
            EdgeRequirement("regulates", EntityType.INDUSTRY, 0, 10),
            EdgeRequirement("regulates", EntityType.CORPORATION, 0, 20),
            EdgeRequirement("allied_with", EntityType.GOVERNMENT, 0, 5),
            EdgeRequirement("sanctions", EntityType.CORPORATION, 0, 10),
        ]
    ),
}

# ═══════════════════════════════════════════════════════════════════════════════
# MACRO-EVENT PATTERNS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class MacroEventPattern:
    """Defines the standard edge pattern for a type of macro event."""
    event_category: str
    description: str
    min_entities: int
    entity_slots: List[Tuple[str, EntityType, int, int]]  # (relation, type, min, max)
    example_event: str

MACRO_EVENT_PATTERNS: Dict[str, MacroEventPattern] = {
    "fed_decision": MacroEventPattern(
        event_category="fed_decision",
        description="Federal Reserve rate decision or policy announcement",
        min_entities=8,
        entity_slots=[
            ("issued_by", EntityType.CENTRAL_BANK, 1, 1),
            ("impacts", EntityType.CORPORATION, 3, 10),  # Major banks, REITs
            ("affects", EntityType.INDUSTRY, 2, 4),      # Banking, Real Estate, Auto
            ("moves", EntityType.ASSET, 2, 5),           # Treasury yields, USD, Gold
            ("signals", EntityType.NARRATIVE, 1, 2),     # Hawkish/Dovish
        ],
        example_event="Fed raises rates 25bp, signals two more hikes in 2024"
    ),

    "earnings_report": MacroEventPattern(
        event_category="earnings_report",
        description="Quarterly earnings release from a major company",
        min_entities=6,
        entity_slots=[
            ("reported_by", EntityType.CORPORATION, 1, 1),
            ("impacts", EntityType.CORPORATION, 2, 5),   # Competitors, suppliers
            ("affects", EntityType.INDUSTRY, 1, 2),
            ("moves", EntityType.ASSET, 1, 3),           # Stock, sector ETF
            ("involves", EntityType.PERSON, 0, 2),       # CEO, CFO
        ],
        example_event="NVIDIA Q4 earnings beat estimates, guides higher on AI demand"
    ),

    "ma_announcement": MacroEventPattern(
        event_category="ma_announcement",
        description="M&A, acquisition, or major investment round",
        min_entities=7,
        entity_slots=[
            ("involves", EntityType.CORPORATION, 2, 3),  # Buyer, target, (advisor)
            ("impacts", EntityType.CORPORATION, 2, 5),   # Competitors
            ("affects", EntityType.INDUSTRY, 1, 3),
            ("involves", EntityType.PERSON, 0, 3),       # CEOs, investors
            ("signals", EntityType.NARRATIVE, 1, 2),     # Consolidation, expansion
        ],
        example_event="Microsoft acquires Activision for $69B"
    ),

    "regulatory_action": MacroEventPattern(
        event_category="regulatory_action",
        description="Government regulatory decision, lawsuit, or sanction",
        min_entities=6,
        entity_slots=[
            ("issued_by", EntityType.GOVERNMENT, 1, 1),
            ("targets", EntityType.CORPORATION, 1, 5),
            ("affects", EntityType.INDUSTRY, 1, 3),
            ("occurs_in", EntityType.LOCATION, 1, 2),
            ("signals", EntityType.NARRATIVE, 0, 2),
        ],
        example_event="DOJ files antitrust suit against Google"
    ),

    "geopolitical_event": MacroEventPattern(
        event_category="geopolitical_event",
        description="Major geopolitical development, conflict, or treaty",
        min_entities=10,
        entity_slots=[
            ("involves", EntityType.GOVERNMENT, 2, 5),
            ("occurs_in", EntityType.LOCATION, 1, 3),
            ("impacts", EntityType.CORPORATION, 3, 10),
            ("affects", EntityType.INDUSTRY, 2, 5),
            ("moves", EntityType.ASSET, 2, 5),           # Oil, defense stocks, etc.
            ("signals", EntityType.NARRATIVE, 1, 3),
        ],
        example_event="US-China trade tensions escalate, new tariffs announced"
    ),

    "economic_data": MacroEventPattern(
        event_category="economic_data",
        description="Major economic data release (CPI, jobs, GDP)",
        min_entities=6,
        entity_slots=[
            ("issued_by", EntityType.GOVERNMENT, 1, 1),
            ("occurs_in", EntityType.LOCATION, 1, 1),
            ("affects", EntityType.INDUSTRY, 2, 5),
            ("moves", EntityType.ASSET, 2, 5),
            ("signals", EntityType.NARRATIVE, 1, 2),
        ],
        example_event="US CPI comes in hot at 3.5%, above 3.2% expected"
    ),

    "product_launch": MacroEventPattern(
        event_category="product_launch",
        description="Major product announcement or technology breakthrough",
        min_entities=5,
        entity_slots=[
            ("announced_by", EntityType.CORPORATION, 1, 1),
            ("impacts", EntityType.CORPORATION, 2, 5),   # Competitors
            ("affects", EntityType.INDUSTRY, 1, 3),
            ("involves", EntityType.PERSON, 0, 2),
        ],
        example_event="Apple announces Vision Pro headset"
    ),

    "supply_disruption": MacroEventPattern(
        event_category="supply_disruption",
        description="Supply chain disruption, shortage, or crisis",
        min_entities=8,
        entity_slots=[
            ("occurs_in", EntityType.LOCATION, 1, 3),
            ("impacts", EntityType.CORPORATION, 3, 10),
            ("affects", EntityType.INDUSTRY, 2, 5),
            ("involves", EntityType.GOVERNMENT, 0, 2),
            ("signals", EntityType.NARRATIVE, 1, 2),
        ],
        example_event="TSMC fab hit by earthquake, chip supply concerns rise"
    ),
}

# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION & HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def validate_entity_edges(entity_type: EntityType, edges: List[dict]) -> Tuple[bool, List[str]]:
    """
    Validate that an entity meets its connection policy requirements.
    Returns (is_valid, list_of_missing_requirements)
    """
    policy = ENTITY_POLICIES.get(entity_type)
    if not policy:
        return True, []

    missing = []

    # Check total edge count
    if len(edges) < policy.min_total_edges:
        missing.append(f"Needs {policy.min_total_edges - len(edges)} more edges (has {len(edges)}, needs {policy.min_total_edges})")

    # Check required edges
    for req in policy.required_edges:
        count = sum(1 for e in edges if e.get('relation') == req.relation)
        if count < req.min_count:
            missing.append(f"Needs {req.min_count - count} more '{req.relation}' edges to {req.target_type.value}")

    return len(missing) == 0, missing


def generate_event_edges(event_category: str, event_data: dict) -> List[dict]:
    """
    Generate edges for a macro event based on its pattern.
    event_data should contain entity references for each slot.
    """
    pattern = MACRO_EVENT_PATTERNS.get(event_category)
    if not pattern:
        return []

    edges = []
    event_id = event_data.get('event_id', 'unknown')

    for relation, target_type, min_count, max_count in pattern.entity_slots:
        targets = event_data.get(f"{relation}_targets", [])
        for target in targets[:max_count]:
            edges.append({
                'from_id': event_id,
                'to_name': target,
                'relation': relation,
                'confidence': 0.9,
                'source': f'macro_event:{event_category}'
            })

    return edges


def print_policy_summary():
    """Print a summary of all policies."""
    print("=" * 70)
    print("GRAPH CONNECTION POLICY SPECIFICATION")
    print("=" * 70)
    print("\nTARGET: 5-10 edges per node\n")

    print("ENTITY POLICIES:")
    print("-" * 70)
    for entity_type, policy in ENTITY_POLICIES.items():
        print(f"\n{entity_type.value}:")
        print(f"  Minimum edges: {policy.min_total_edges}")
        print(f"  Required:")
        for req in policy.required_edges:
            print(f"    - {req.relation} -> {req.target_type.value} (min: {req.min_count})")
        if policy.recommended_edges:
            print(f"  Recommended:")
            for rec in policy.recommended_edges[:3]:
                print(f"    - {rec.relation} -> {rec.target_type.value}")

    print("\n" + "=" * 70)
    print("MACRO-EVENT PATTERNS:")
    print("-" * 70)
    for category, pattern in MACRO_EVENT_PATTERNS.items():
        print(f"\n{category}:")
        print(f"  Min entities: {pattern.min_entities}")
        print(f"  Slots:")
        for relation, target_type, min_c, max_c in pattern.entity_slots:
            print(f"    - {relation} -> {target_type.value} ({min_c}-{max_c})")
        print(f"  Example: {pattern.example_event[:60]}...")


if __name__ == '__main__':
    print_policy_summary()
