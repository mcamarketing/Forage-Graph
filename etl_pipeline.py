"""
Reality Graph ETL Pipeline

Transforms GLiNER output into FalkorDB-compatible batches with:
- ULEM identity generation (sha3_256 + blake3)
- FIBO ontology alignment
- Endpoint-first UNWIND batch creation

Flow: RawDocument → GLiNER NER → ULEM normalize → FalkorDB UNWIND
"""

from __future__ import annotations
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
from hashlib import sha3_256

try:
    from blake3 import blake3
except ImportError:
    # Fallback: use sha256 as stand-in if blake3 not installed
    def blake3(data: bytes):
        import hashlib

        return hashlib.sha256(data)


# ─── GLiNER Output Schema ────────────────────────────────────────────────────


class GLiNEREntity(BaseModel):
    """Raw entity from GLiNER service."""

    text: str
    start: int
    end: int
    label: str
    score: float


# ─── ULEM Identity ───────────────────────────────────────────────────────────


class ULEMIdentity(BaseModel):
    """Universal Logical Entity Model — deterministic identity.

    Requirements:
    1. entity_type IS included in canonical string to prevent cross-class collisions
    2. content_hash MUST be set by caller from blake3(raw_payload_bytes) before persistence
    """

    semantic_id: str = Field(
        ..., description="sha3_256(entity_type:normalized_business_keys)"
    )
    content_hash: str = Field(
        ..., description="blake3(raw_payload_bytes) — caller must populate"
    )
    canonical_form: str = Field(..., description="entity_type:k1=v1:k2=v2 format")

    @staticmethod
    def generate(entity_type: str, business_keys: Dict[str, str]) -> ULEMIdentity:
        """Generate ULEM identity from business keys.

        Rule: entity_type is ALWAYS part of canonical to prevent cross-class collisions.
        """
        canonical = f"{entity_type}:" + ":".join(
            f"{k}={v.lower().strip()}" for k, v in sorted(business_keys.items())
        )
        semantic_id = sha3_256(canonical.encode()).hexdigest()[:32]
        return ULEMIdentity(
            semantic_id=semantic_id,
            content_hash="",  # ETL MUST populate via blake3(raw_payload) before persistence
            canonical_form=canonical,
        )

    def set_content_hash(self, raw_payload: bytes) -> None:
        """Populate content_hash from raw payload bytes. Required before persistence."""
        self.content_hash = blake3(raw_payload).hexdigest()[:32]


# ─── FIBO Ontology Mapping ──────────────────────────────────────────────────

# Maps GLiNER labels to FIBO types
FIBO_ONTOLOGY_MAP: Dict[str, str] = {
    "central bank": "FinancialInstitution",
    "financial institution": "FinancialInstitution",
    "corporation": "Corporation",
    "crypto asset": "CryptoAsset",
    "fiat currency": "FiatCurrency",
    "stock index": "FinancialInstrument",
    "commodity": "Commodity",
    "government agency": "LegalEntity",
    "regulatory body": "LegalEntity",
    "country": "Jurisdiction",
    "jurisdiction": "Jurisdiction",
    "macro event": "Event",
    "policy change": "Policy",
    "market event": "Event",
    "person": "Person",
}


# ─── ETL Batch Schema ────────────────────────────────────────────────────────


class ExtractedEntity(BaseModel):
    """Normalized entity for FalkorDB ingestion."""

    surface: str
    label: str
    semantic_type: str
    normalized_name: str
    semantic_id: str
    content_hash: str
    score: float
    # GLiNER span for debugging/training
    span_start: int
    span_end: int


class EpisodeData(BaseModel):
    """Episode node for GraphRAG."""

    episode_id: str
    source_id: str
    source_type: str
    timestamp: str
    content_hash: str
    text: str
    episode_type: str = "document"


class IntelETLBatch(BaseModel):
    """Intermediate schema between GLiNER and FalkorDB."""

    episodes: List[Dict[str, Any]]
    entities: List[Dict[str, Any]]

    @classmethod
    def from_gliner_output(
        cls,
        document_id: str,
        source_type: str,
        raw_text: str,
        gliner_results: List[GLiNEREntity],
        ontology_map: Optional[Dict[str, str]] = None,
    ) -> IntelETLBatch:
        """Transform GLiNER output to ETL batch format.

        Note: normalized_name should be passed through a normalizer component
        (lowercase, trim, alias resolution) once available. The contract accepts
        normalized_name as-is; swap the normalizer without changing the schema.
        """
        if ontology_map is None:
            ontology_map = FIBO_ONTOLOGY_MAP

        content_hash = blake3(raw_text.encode()).hexdigest()[:32]

        episode = {
            "episode_id": f"ep_{document_id}",
            "source_id": document_id,
            "source_type": source_type,
            "timestamp": datetime.utcnow().isoformat(),
            "content_hash": content_hash,
            "text": raw_text[:500],  # Truncate for storage
            "episode_type": "document",
        }

        entities = []
        for ent in gliner_results:
            fibo_type = ontology_map.get(ent.label, "Entity")
            ulem = ULEMIdentity.generate(fibo_type, {"name": ent.text})
            ulem.set_content_hash(raw_text[ent.start : ent.end].encode())

            entities.append(
                {
                    "surface": ent.text,
                    "label": ent.label,
                    "semantic_type": fibo_type,
                    "normalized_name": ent.text.lower().strip(),  # Basic normalization
                    "semantic_id": ulem.semantic_id,
                    "content_hash": ulem.content_hash,
                    "score": ent.score,
                    "span_start": ent.start,
                    "span_end": ent.end,
                }
            )

        return cls(episodes=[episode], entities=entities)


# ─── FalkorDB UNWIND Cypher Generator ────────────────────────────────────────


def generate_unwind_cypher() -> str:
    """Generate endpoint-first UNWIND Cypher for batch ingestion.

    Uses endpoint-first MERGE to prevent matrix duplication in GraphBLAS.
    """
    return """
WITH $batch AS episodes
UNWIND episodes AS ep

// 1. MERGE Episode endpoint (endpoint-first)
MERGE (e:Episode {episode_id: ep.episode.episode_id})
ON CREATE SET
  e.source_id     = ep.episode.source_id,
  e.source_type   = ep.episode.source_type,
  e.timestamp     = datetime(ep.episode.timestamp),
  e.content_hash  = ep.episode.content_hash,
  e.text          = ep.episode.text,
  e.episode_type  = ep.episode.episode_type

// 2. MERGE Entity endpoints (independent clause - endpoint-first)
WITH e, ep.entities AS ents
UNWIND ents AS ent
MERGE (n:Entity {semantic_id: ent.semantic_id})
ON CREATE SET
  n.surface         = ent.surface,
  n.normalized_name = ent.normalized_name,
  n.semantic_type   = ent.semantic_type,
  n.content_hash    = ent.content_hash,
  n.first_seen      = datetime()
ON MATCH SET
  n.last_seen = datetime()

// 3. MERGE relationship (after both endpoints exist)
MERGE (e)-[r:REFS_ENTITY]->(n)
ON CREATE SET
  r.confidence = ent.score,
  r.span_start = ent.span_start,
  r.span_end   = ent.span_end
"""


# ─── Usage Example ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Example: Transform GLiNER output to ETL batch
    gliner_output = [
        GLiNEREntity(text="ECB", start=4, end=7, label="central bank", score=0.94),
        GLiNEREntity(text="BTC", start=28, end=31, label="crypto asset", score=0.91),
    ]

    batch = IntelETLBatch.from_gliner_output(
        document_id="news_reuters_20260324_001",
        source_type="news",
        raw_text="The ECB cut rates as BTC rallied above $70k.",
        gliner_results=gliner_output,
    )

    print(f"Episodes: {len(batch.episodes)}")
    print(f"Entities: {len(batch.entities)}")
    for ent in batch.entities:
        print(f"  - {ent['surface']} ({ent['semantic_type']}): {ent['semantic_id']}")

    print(f"\nCypher:\n{generate_unwind_cypher()}")
