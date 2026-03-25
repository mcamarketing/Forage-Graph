"""
Reality Graph Pydantic Models v0.1

FIBO-aligned data models for:
- Asset (FinancialInstrument)
- Organization (LegalEntity / AutonomousAgent)
- Episode (GraphRAG + simulation events)
- Three-layer Memory (Semantic, Episodic, Reasoning)

Invariant: Simulation-layer nodes use :SimAgent/:SimEpisode/:SimStatement labels
           and may only create READS_FROM/OBSERVED edges into Reality Graph.
"""

from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


# ─── ENUMS ────────────────────────────────────────────────────────────────────


class AssetClass(str, Enum):
    EQUITY = "EquityInstrument"
    DEBT = "DebtInstrument"
    DERIVATIVE = "DerivativeInstrument"
    CRYPTO = "CryptoAsset"
    COMMODITY = "Commodity"
    FIAT = "FiatCurrency"


class OrgType(str, Enum):
    LEGAL_ENTITY = "LegalEntity"
    AUTONOMOUS_AGENT = "AutonomousAgent"
    FINANCIAL_INSTITUTION = "FinancialInstitution"
    CORPORATION = "Corporation"
    SOLE_PROPRIETOR = "SoleProprietor"


class EpisodeType(str, Enum):
    DOCUMENT = "document"
    SIMULATION_ACTION = "simulation_action"
    HAWKES_EVENT = "hawkes_event"
    CLAIM = "claim"


class MemoryLayer(str, Enum):
    SEMANTIC = "semantic"
    EPISODIC = "episodic"
    REASONING = "reasoning"


# ─── ASSET (FIBO: FinancialInstrument) ────────────────────────────────────────


class Asset(BaseModel):
    """FIBO-aligned Financial Instrument.

    Maps to FinancialInstrument in FIBO ontology.
    issuer_id is FK to Organization.semantic_id.
    """

    semantic_id: str = Field(..., description="ULEM sha3_256 identity")
    content_hash: str = Field(..., description="ULEM blake3 content hash")

    # FIBO Classification
    asset_class: AssetClass
    fibo_type: str = Field(..., description="FIBO class URI fragment")

    # Identifiers
    symbol: Optional[str] = None
    isin: Optional[str] = None
    lei: Optional[str] = None

    # Properties
    name: str
    issuer_id: Optional[str] = None  # FK → Organization.semantic_id
    jurisdiction: Optional[str] = None

    # Reality Graph metadata
    first_seen: datetime = Field(default_factory=datetime.utcnow)
    last_seen: datetime = Field(default_factory=datetime.utcnow)
    confidence: float = Field(0.75, ge=0.0, le=1.0)
    sources: List[str] = Field(default_factory=list)

    # Hawkes Process state
    intensity: float = 0.0
    branching_ratio: float = 0.0


# ─── ORGANIZATION (FIBO: LegalEntity / AutonomousAgent) ──────────────────────


class Organization(BaseModel):
    """FIBO-aligned Organization with LegalEntity/AutonomousAgent decomposition.

    LegalEntity: Traditional organizations (banks, corporations)
    AutonomousAgent: AI agents, bots, autonomous systems
    """

    semantic_id: str = Field(..., description="ULEM sha3_256 identity")
    content_hash: str = Field(..., description="ULEM blake3 content hash")

    # FIBO Classification
    org_type: OrgType
    fibo_type: str = Field(..., description="FIBO class URI fragment")

    # Identifiers
    name: str
    aliases: List[str] = Field(default_factory=list)
    lei: Optional[str] = None  # For legal entities
    agent_id: Optional[str] = None  # For autonomous agents

    # Relationships
    parent_org_id: Optional[str] = None  # FK → Organization.semantic_id
    jurisdiction: Optional[str] = None
    industry_sector: Optional[str] = None

    # Hawkes state
    intensity: float = 0.0
    branching_ratio: float = 0.0

    # Metadata
    first_seen: datetime = Field(default_factory=datetime.utcnow)
    last_seen: datetime = Field(default_factory=datetime.utcnow)
    confidence: float = Field(0.75, ge=0.0, le=1.0)
    sources: List[str] = Field(default_factory=list)

    @property
    def is_agent(self) -> bool:
        return self.org_type == OrgType.AUTONOMOUS_AGENT


# ─── EPISODE (GraphRAG + Events) ─────────────────────────────────────────────


class Episode(BaseModel):
    """Timestamped episodic memory for GraphRAG and simulation traces."""

    episode_id: str = Field(..., description="UUID or deterministic hash")
    episode_type: EpisodeType

    # Source
    source_id: str
    source_type: str  # "news", "pdf", "twitter", "reddit"
    timestamp: datetime

    # Content
    content_hash: str  # blake3 of raw content
    text: str
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Entity references with GLiNER span for debugging/training
    entity_refs: List[Dict[str, Any]] = Field(default_factory=list)
    # Format: [{"semantic_id": "...", "surface": "ECB", "role": "subject",
    #           "span_start": 4, "span_end": 7, "label": "central_bank", "score": 0.94}]

    # Simulation fields
    agent_id: Optional[int] = None
    action_type: Optional[str] = None
    round_num: Optional[int] = None

    confidence: float = 0.8


# ─── THREE-LAYER MEMORY ──────────────────────────────────────────────────────


class SemanticMemory(BaseModel):
    """Long-lived facts and ontological constraints.

    These are stable truths that persist indefinitely.
    Never overwritten by Simulation Layer.
    """

    entity_semantic_id: str
    fact: str
    confidence: float
    first_asserted: datetime = Field(default_factory=datetime.utcnow)
    last_corroborated: datetime = Field(default_factory=datetime.utcnow)
    corroboration_count: int = 1
    sources: List[str] = Field(default_factory=list)


class EpisodicMemory(BaseModel):
    """Timestamped interaction logs and Cypher retrieval traces.

    Used for few-shot prompting and context retrieval.
    """

    episode: Episode
    retrieved_at: datetime = Field(default_factory=datetime.utcnow)
    retrieval_reason: str
    cypher_trace: Optional[str] = None


class ReasoningMemory(BaseModel):
    """Immutable decision traces for full provenance.

    Records tool calls, rationales, and state transitions.
    Never modified after creation.
    """

    decision_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    agent_id: str
    reasoning: str
    tool_calls: List[Dict[str, Any]] = Field(default_factory=list)
    input_state: Dict[str, Any] = Field(default_factory=dict)
    output_state: Dict[str, Any] = Field(default_factory=dict)
    rationale: str
    confidence: float


# ─── SIMULATION LAYER (Separate from Reality) ────────────────────────────────


class SimAgent(BaseModel):
    """Simulation agent — uses :SimAgent label, never writes to Reality Graph.

    Constraint: May only create READS_FROM edges to Reality Graph entities.
    """

    sim_id: str
    reality_entity_id: Optional[str] = None  # FK → Entity.semantic_id via READS_FROM
    name: str
    persona: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class SimEpisode(BaseModel):
    """Simulation episode — uses :SimEpisode label, isolated from Reality Graph.

    Constraint: May only create OBSERVED edges to Reality Graph episodes.
    """

    sim_episode_id: str
    reality_episode_id: Optional[str] = None  # FK → Episode.episode_id via OBSERVED
    simulation_id: str
    round_num: int
    actions: List[Dict[str, Any]] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
