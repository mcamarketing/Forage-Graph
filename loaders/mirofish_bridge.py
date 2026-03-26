"""
MiroFish Bridge — Connect Reality Graph to MiroFish Simulation Engine

This module bridges your Forage Reality Graph with MiroFish to:
1. Export Reality Graph entities/relationships to MiroFish ontology format
2. Run MiroFish simulations on crash scenarios
3. Import simulation results back as SimAgent/SimEpisode nodes
4. Generate counterfactual crash analyses

Boundary Constraint:
- SimAgent/SimEpisode nodes can only READS_FROM Reality nodes
- Simulation cannot modify Reality Graph — only observe and hypothesize

Usage:
    from loaders.mirofish_bridge import MiroFishBridge

    bridge = MiroFishBridge(reality_graph_url, mirofish_url)

    # Export Reality Graph to MiroFish
    ontology = bridge.export_ontology()

    # Run crash simulation
    results = bridge.simulate_crash("2008 Financial Crisis", shock_magnitude=1.5)

    # Import simulation results back
    bridge.import_simulation_results(results)

[mirofish-bridge-001]
"""

import os
import json
import uuid
import asyncio
import aiohttp
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum

import redis


# ─── CONFIGURATION ────────────────────────────────────────────────────────────

FALKORDB_URL = os.environ.get('FALKORDB_URL', os.environ.get('REDIS_URL', 'redis://localhost:6379'))
MIROFISH_API_URL = os.environ.get('MIROFISH_API_URL', 'http://localhost:8000')
GRAPH_NAME = 'forage_v1'


# ─── DATA CLASSES ─────────────────────────────────────────────────────────────

class CrashEntityType(str, Enum):
    """Reality Graph crash-related entity types"""
    MARKET_CRASH = "MarketCrash"
    SECTOR_CRASH = "SectorCrash"
    FLASH_CRASH = "FlashCrash"
    CURRENCY_CRISIS = "CurrencyCrisis"
    BANK_RUN = "BankRun"
    LIQUIDITY_CRISIS = "LiquidityCrisis"
    CRASH_FACTOR = "CrashFactor"
    CRASH_MECHANISM = "CrashMechanism"
    RISK_CHANNEL = "RiskChannel"


class SimulationRelationType(str, Enum):
    """Simulation-layer relationship types"""
    READS_FROM = "READS_FROM"
    SIMULATES = "SIMULATES"
    HYPOTHESIZES = "HYPOTHESIZES"
    OBSERVED = "OBSERVED"


@dataclass
class MiroFishEntity:
    """MiroFish-compatible entity"""
    name: str
    entity_type: str
    description: str = ""
    attributes: Dict[str, Any] = field(default_factory=dict)
    reality_id: Optional[str] = None  # FK to Reality Graph

    def to_zep_format(self) -> Dict[str, Any]:
        """Convert to Zep API format for MiroFish ingestion"""
        return {
            "name": self.name,
            "type": self.entity_type,
            "description": self.description,
            "attributes": self.attributes,
            "metadata": {
                "reality_graph_id": self.reality_id,
                "imported_at": datetime.utcnow().isoformat(),
            }
        }


@dataclass
class MiroFishRelation:
    """MiroFish-compatible relationship"""
    source_name: str
    target_name: str
    relation_type: str
    weight: float = 1.0
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_zep_format(self) -> Dict[str, Any]:
        return {
            "source": self.source_name,
            "target": self.target_name,
            "relation": self.relation_type,
            "weight": self.weight,
            "properties": self.properties,
        }


@dataclass
class CrashScenario:
    """Crash scenario for simulation"""
    scenario_id: str
    name: str
    description: str

    # Initial conditions
    shock_entity: str              # Entity that receives initial shock
    shock_magnitude: float         # 0-2 scale (1 = normal, 2 = extreme)
    shock_type: str                # "monetary", "geopolitical", "pandemic", etc.

    # Simulation parameters
    simulation_rounds: int = 48   # Hours to simulate
    agent_count: int = 20         # Number of SimAgents

    # Hawkes process parameters
    mu: float = 0.01              # Background intensity
    alpha: float = 0.5            # Excitation parameter
    beta: float = 0.1             # Decay rate

    # Results (populated after simulation)
    simulated_drawdown: Optional[float] = None
    cascade_depth: Optional[int] = None
    entities_affected: Optional[List[str]] = None
    causal_paths: Optional[List[Dict]] = None


@dataclass
class SimulationResult:
    """Result from MiroFish simulation"""
    simulation_id: str
    scenario: CrashScenario

    # Outcomes
    final_drawdown_pct: float
    peak_intensity: float
    total_actions: int

    # Agent traces
    agent_actions: List[Dict[str, Any]] = field(default_factory=list)

    # Inferred causal chains
    inferred_paths: List[Dict[str, Any]] = field(default_factory=list)

    # Confidence
    confidence: float = 0.8

    # Timestamp
    completed_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


# ─── MIROFISH BRIDGE ──────────────────────────────────────────────────────────

class MiroFishBridge:
    """
    Bridge between Forage Reality Graph and MiroFish simulation engine.

    Architecture:
    ┌────────────────────┐     ┌────────────────────┐
    │   Reality Graph    │────▶│     MiroFish       │
    │   (FalkorDB)       │     │   (Zep + OASIS)    │
    │                    │     │                    │
    │  Company, Asset,   │     │  SimAgent,         │
    │  MacroEvent, etc.  │     │  SimEpisode        │
    └────────────────────┘     └────────────────────┘
            ▲                          │
            │   READS_FROM             │
            │   (immutable)            │
            └──────────────────────────┘
    """

    def __init__(
        self,
        falkordb_url: str = FALKORDB_URL,
        mirofish_url: str = MIROFISH_API_URL,
        graph_name: str = GRAPH_NAME,
    ):
        self.falkordb_url = falkordb_url
        self.mirofish_url = mirofish_url
        self.graph_name = graph_name
        self.redis_client: Optional[redis.Redis] = None

    async def connect(self):
        """Initialize connections to both systems"""
        # Connect to FalkorDB
        self.redis_client = redis.from_url(self.falkordb_url)
        self.redis_client.ping()
        print(f"[BRIDGE] Connected to FalkorDB at {self.falkordb_url}")

    def close(self):
        """Close connections"""
        if self.redis_client:
            self.redis_client.close()

    # ─── EXPORT TO MIROFISH ───────────────────────────────────────────────────

    async def export_ontology(
        self,
        entity_types: Optional[List[str]] = None,
        limit: int = 500
    ) -> Dict[str, Any]:
        """
        Export Reality Graph entities to MiroFish ontology format.

        Returns Zep-compatible ontology for MiroFish graph building.
        """
        if not self.redis_client:
            await self.connect()

        # Default: export crash-relevant entities
        if entity_types is None:
            entity_types = [
                'Corporation', 'FinancialInstitution', 'Asset',
                'MarketCrash', 'CrashFactor', 'CrashMechanism',
                'Industry', 'Location', 'Policy', 'Indicator',
            ]

        entities = await self._fetch_entities(entity_types, limit)
        relations = await self._fetch_relations(entity_types, limit * 2)

        # Convert to MiroFish ontology format
        ontology = self._build_ontology(entities, relations)

        return ontology

    async def export_crash_context(
        self,
        crash_name: str,
        depth: int = 3
    ) -> Tuple[List[MiroFishEntity], List[MiroFishRelation]]:
        """
        Export all entities connected to a crash within N hops.

        This creates a focused subgraph for crash simulation.
        """
        if not self.redis_client:
            await self.connect()

        # Find crash node
        crash_query = f"""
            MATCH (c:Entity)
            WHERE c.name_lower CONTAINS "{crash_name.lower()}"
              AND c.type IN ['MarketCrash', 'SectorCrash', 'FlashCrash']
            RETURN c.id AS crash_id, c.name AS crash_name
            LIMIT 1
        """
        crash_result = self._graph_query(crash_query)

        if not crash_result:
            raise ValueError(f"Crash not found: {crash_name}")

        crash_id = crash_result[0]['crash_id']

        # Get N-hop neighborhood
        neighborhood_query = f"""
            MATCH path = (e:Entity)-[*1..{depth}]-(c:Entity {{id: "{crash_id}"}})
            WITH e, relationships(path) AS rels
            RETURN DISTINCT
                e.id AS entity_id,
                e.name AS entity_name,
                e.type AS entity_type,
                e.properties AS properties
        """

        entities_result = self._graph_query(neighborhood_query)

        # Convert to MiroFish format
        entities = []
        for row in entities_result:
            entities.append(MiroFishEntity(
                name=row['entity_name'],
                entity_type=row['entity_type'],
                attributes=row.get('properties', {}),
                reality_id=row['entity_id'],
            ))

        # Get relations
        relations_query = f"""
            MATCH (a:Entity)-[r:RELATES]-(b:Entity)
            WHERE a.id IN ["{crash_id}"] + [{','.join(f'"{e.reality_id}"' for e in entities)}]
               OR b.id IN ["{crash_id}"] + [{','.join(f'"{e.reality_id}"' for e in entities)}]
            RETURN
                a.name AS source_name,
                b.name AS target_name,
                r.relation AS relation_type,
                r.confidence AS weight
        """

        relations_result = self._graph_query(relations_query)

        relations = []
        for row in relations_result:
            relations.append(MiroFishRelation(
                source_name=row['source_name'],
                target_name=row['target_name'],
                relation_type=row['relation_type'] or 'related_to',
                weight=row.get('weight', 0.5),
            ))

        return entities, relations

    # ─── SIMULATION ───────────────────────────────────────────────────────────

    async def simulate_crash(
        self,
        scenario: CrashScenario,
    ) -> SimulationResult:
        """
        Run a MiroFish simulation on a crash scenario.

        Process:
        1. Export relevant Reality Graph context to MiroFish
        2. Initialize SimAgents based on scenario
        3. Inject shock event
        4. Run simulation for specified rounds
        5. Collect and analyze results
        6. Import results back as SimEpisode nodes
        """
        print(f"[BRIDGE] Starting crash simulation: {scenario.name}")

        # 1. Export context
        entities, relations = await self.export_crash_context(scenario.shock_entity, depth=3)

        # 2. Build MiroFish graph
        graph_id = await self._create_mirofish_graph(
            scenario.scenario_id,
            entities,
            relations
        )

        # 3. Generate agent profiles
        agent_profiles = self._generate_crash_agents(scenario, entities)

        # 4. Run simulation via MiroFish API
        simulation_result = await self._run_mirofish_simulation(
            graph_id,
            scenario,
            agent_profiles
        )

        # 5. Analyze results
        result = self._analyze_simulation(scenario, simulation_result)

        # 6. Import back to Reality Graph
        await self.import_simulation_results(result)

        return result

    async def simulate_counterfactual(
        self,
        crash_name: str,
        intervention: Dict[str, Any]
    ) -> SimulationResult:
        """
        Simulate a counterfactual: "What if X didn't happen?"

        Intervention types:
        - remove_factor: Remove a causal factor from the graph
        - early_intervention: Add circuit breaker at time T
        - limit_exposure: Cap entity exposure to N%
        """
        # Create modified scenario
        scenario = CrashScenario(
            scenario_id=f"cf-{uuid.uuid4().hex[:8]}",
            name=f"Counterfactual: {crash_name}",
            description=f"What if: {intervention.get('description', 'unknown intervention')}",
            shock_entity=intervention.get('target_entity', ''),
            shock_magnitude=intervention.get('shock_magnitude', 0.5),
            shock_type=intervention.get('intervention_type', 'limit_exposure'),
        )

        # Run simulation with modified parameters
        return await self.simulate_crash(scenario)

    # ─── IMPORT RESULTS ───────────────────────────────────────────────────────

    async def import_simulation_results(
        self,
        result: SimulationResult
    ) -> None:
        """
        Import simulation results back to Reality Graph as SimEpisode nodes.

        Creates:
        - SimEpisode node for the simulation run
        - READS_FROM edges to Reality entities observed
        - HYPOTHESIZES edges for inferred causal paths
        """
        if not self.redis_client:
            await self.connect()

        sim_id = f"sim-{result.simulation_id}"

        # Create SimEpisode node
        sim_props = {
            "id": sim_id,
            "type": "SimEpisode",
            "name": result.scenario.name,
            "scenario_id": result.scenario.scenario_id,
            "final_drawdown_pct": result.final_drawdown_pct,
            "peak_intensity": result.peak_intensity,
            "total_actions": result.total_actions,
            "confidence": result.confidence,
            "completed_at": result.completed_at,
        }

        create_sim_query = f"""
            MERGE (s:Entity {{id: "{sim_id}"}})
            SET s += {self._props_to_cypher(sim_props)},
                s.is_simulation = true
        """
        self._graph_query(create_sim_query)

        # Create READS_FROM edges to observed entities
        if result.scenario.entities_affected:
            for entity_name in result.scenario.entities_affected[:50]:  # Limit
                link_query = f"""
                    MATCH (s:Entity {{id: "{sim_id}"}})
                    MATCH (e:Entity)
                    WHERE e.name_lower CONTAINS "{entity_name.lower()}"
                    MERGE (s)-[r:RELATES]->(e)
                    SET r.relation = "READS_FROM",
                        r.simulation_id = "{result.simulation_id}"
                """
                self._graph_query(link_query)

        # Create HYPOTHESIZES edges for inferred paths
        for path in result.inferred_paths[:20]:
            nodes = path.get('nodes', [])
            if len(nodes) >= 2:
                hyp_query = f"""
                    MATCH (a:Entity), (b:Entity)
                    WHERE a.name_lower CONTAINS "{nodes[0].lower()}"
                      AND b.name_lower CONTAINS "{nodes[-1].lower()}"
                    MERGE (a)-[r:RELATES]->(b)
                    SET r.relation = "HYPOTHESIZES",
                        r.inferred_by = "{sim_id}",
                        r.confidence = {path.get('confidence', 0.5)},
                        r.mechanism = "{path.get('mechanism', 'unknown')}"
                """
                self._graph_query(hyp_query)

        print(f"[BRIDGE] Imported simulation {sim_id} with {len(result.inferred_paths)} hypothesized paths")

    # ─── PRIVATE HELPERS ──────────────────────────────────────────────────────

    async def _fetch_entities(
        self,
        entity_types: List[str],
        limit: int
    ) -> List[Dict[str, Any]]:
        """Fetch entities of specified types from Reality Graph"""
        types_str = ', '.join(f'"{t}"' for t in entity_types)

        query = f"""
            MATCH (e:Entity)
            WHERE e.type IN [{types_str}]
            RETURN e.id AS id, e.name AS name, e.type AS type, e.properties AS props
            LIMIT {limit}
        """

        return self._graph_query(query)

    async def _fetch_relations(
        self,
        entity_types: List[str],
        limit: int
    ) -> List[Dict[str, Any]]:
        """Fetch relations between specified entity types"""
        types_str = ', '.join(f'"{t}"' for t in entity_types)

        query = f"""
            MATCH (a:Entity)-[r:RELATES]->(b:Entity)
            WHERE a.type IN [{types_str}] AND b.type IN [{types_str}]
            RETURN
                a.name AS source,
                b.name AS target,
                r.relation AS relation,
                r.confidence AS confidence
            LIMIT {limit}
        """

        return self._graph_query(query)

    def _build_ontology(
        self,
        entities: List[Dict],
        relations: List[Dict]
    ) -> Dict[str, Any]:
        """Build MiroFish/Zep ontology from entities and relations"""

        # Group entities by type
        entity_types = {}
        for e in entities:
            t = e.get('type', 'Entity')
            if t not in entity_types:
                entity_types[t] = {
                    "name": t,
                    "description": f"A {t} entity from Reality Graph",
                    "attributes": []
                }

        # Infer edge types
        edge_types = {}
        for r in relations:
            rel = r.get('relation', 'related_to')
            if rel not in edge_types:
                edge_types[rel] = {
                    "name": rel,
                    "description": f"{rel} relationship",
                    "source_targets": []
                }

        return {
            "entity_types": list(entity_types.values()),
            "edge_types": list(edge_types.values()),
            "metadata": {
                "source": "forage_reality_graph",
                "exported_at": datetime.utcnow().isoformat(),
                "entity_count": len(entities),
                "relation_count": len(relations),
            }
        }

    async def _create_mirofish_graph(
        self,
        graph_id: str,
        entities: List[MiroFishEntity],
        relations: List[MiroFishRelation]
    ) -> str:
        """Create a graph in MiroFish with the exported data"""

        # Prepare text for MiroFish GraphRAG ingestion
        # MiroFish will extract entities from this text
        text_chunks = []

        for entity in entities:
            text_chunks.append(
                f"{entity.name} is a {entity.entity_type}. {entity.description}"
            )

        for relation in relations:
            text_chunks.append(
                f"{relation.source_name} {relation.relation_type} {relation.target_name}."
            )

        combined_text = "\n".join(text_chunks)

        # Call MiroFish API
        try:
            async with aiohttp.ClientSession() as session:
                # Build graph
                async with session.post(
                    f"{self.mirofish_url}/api/graph/build",
                    json={
                        "text": combined_text,
                        "graph_name": graph_id,
                        "ontology": self._build_ontology(
                            [asdict(e) for e in entities],
                            [asdict(r) for r in relations]
                        )
                    }
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get('graph_id', graph_id)
                    else:
                        print(f"[BRIDGE] MiroFish graph build failed: {resp.status}")
                        return graph_id
        except Exception as e:
            print(f"[BRIDGE] MiroFish API error: {e}")
            # Fallback: return ID for offline simulation
            return graph_id

    def _generate_crash_agents(
        self,
        scenario: CrashScenario,
        entities: List[MiroFishEntity]
    ) -> List[Dict[str, Any]]:
        """Generate SimAgent profiles for crash simulation"""

        agents = []

        # Create agents representing different market participants
        agent_types = [
            ("Hedge Fund", "aggressive", 0.8),
            ("Pension Fund", "conservative", 0.3),
            ("Investment Bank", "neutral", 0.6),
            ("Retail Investor", "reactive", 0.5),
            ("Central Bank", "stabilizer", 0.2),
            ("Regulator", "interventionist", 0.1),
        ]

        for i, (agent_name, behavior, aggression) in enumerate(agent_types):
            if i >= scenario.agent_count:
                break

            agents.append({
                "agent_id": i,
                "name": f"{agent_name}_{i}",
                "persona": {
                    "type": agent_name,
                    "behavior_mode": behavior,
                    "aggression_level": aggression,
                    "shock_sensitivity": scenario.alpha * aggression,
                },
                "initial_position": {
                    "entity": scenario.shock_entity,
                    "exposure": 100 * aggression,
                }
            })

        return agents

    async def _run_mirofish_simulation(
        self,
        graph_id: str,
        scenario: CrashScenario,
        agents: List[Dict]
    ) -> Dict[str, Any]:
        """Run the actual MiroFish simulation"""

        try:
            async with aiohttp.ClientSession() as session:
                # Prepare simulation
                async with session.post(
                    f"{self.mirofish_url}/api/simulation/prepare",
                    json={
                        "graph_id": graph_id,
                        "agents": agents,
                        "time_config": {
                            "total_simulation_hours": scenario.simulation_rounds,
                            "minutes_per_round": 30,
                        }
                    }
                ) as resp:
                    if resp.status != 200:
                        return self._mock_simulation(scenario)

                    prep_data = await resp.json()
                    simulation_id = prep_data.get('simulation_id')

                # Start simulation
                async with session.post(
                    f"{self.mirofish_url}/api/simulation/start",
                    json={
                        "simulation_id": simulation_id,
                        "platform": "parallel",  # Twitter + Reddit style
                    }
                ) as resp:
                    if resp.status != 200:
                        return self._mock_simulation(scenario)

                # Wait for completion (poll status)
                for _ in range(120):  # 2 minute timeout
                    await asyncio.sleep(1)

                    async with session.get(
                        f"{self.mirofish_url}/api/simulation/status/{simulation_id}"
                    ) as resp:
                        status = await resp.json()
                        if status.get('runner_status') in ['completed', 'failed']:
                            break

                # Get results
                async with session.get(
                    f"{self.mirofish_url}/api/simulation/actions/{simulation_id}"
                ) as resp:
                    actions = await resp.json()
                    return {
                        "simulation_id": simulation_id,
                        "actions": actions,
                        "status": "completed",
                    }

        except Exception as e:
            print(f"[BRIDGE] Simulation error: {e}")
            return self._mock_simulation(scenario)

    def _mock_simulation(self, scenario: CrashScenario) -> Dict[str, Any]:
        """Fallback mock simulation when MiroFish is unavailable"""

        # Simple cascade model
        base_drawdown = scenario.shock_magnitude * 15  # 15% per unit shock
        cascade_factor = 1 + scenario.alpha * scenario.simulation_rounds / 24

        final_drawdown = base_drawdown * cascade_factor

        return {
            "simulation_id": f"mock-{uuid.uuid4().hex[:8]}",
            "actions": [
                {
                    "round": i,
                    "agent_id": i % 6,
                    "action_type": "SELL" if i < scenario.simulation_rounds / 2 else "HOLD",
                    "intensity": scenario.mu + scenario.alpha * (1 - i / scenario.simulation_rounds),
                }
                for i in range(min(100, scenario.simulation_rounds * 2))
            ],
            "status": "mocked",
            "final_drawdown_pct": round(final_drawdown, 2),
        }

    def _analyze_simulation(
        self,
        scenario: CrashScenario,
        raw_result: Dict[str, Any]
    ) -> SimulationResult:
        """Analyze raw simulation output into structured result"""

        actions = raw_result.get('actions', [])

        # Compute metrics
        total_actions = len(actions)

        # Peak intensity from actions
        intensities = [a.get('intensity', 0) for a in actions if 'intensity' in a]
        peak_intensity = max(intensities) if intensities else scenario.mu

        # Final drawdown
        final_drawdown = raw_result.get('final_drawdown_pct', scenario.shock_magnitude * 10)

        # Infer causal paths from action sequences
        inferred_paths = []
        sell_sequences = []
        current_seq = []

        for action in actions:
            if action.get('action_type') in ['SELL', 'LIQUIDATE']:
                current_seq.append(action.get('agent_id', 0))
            elif current_seq:
                if len(current_seq) >= 2:
                    sell_sequences.append(current_seq)
                current_seq = []

        for seq in sell_sequences[:10]:
            inferred_paths.append({
                "nodes": [f"Agent_{a}" for a in seq],
                "mechanism": "panic_cascade",
                "confidence": 0.6,
            })

        # Entities affected
        affected_agents = set(a.get('agent_id') for a in actions if a.get('action_type') in ['SELL', 'LIQUIDATE'])

        return SimulationResult(
            simulation_id=raw_result.get('simulation_id', str(uuid.uuid4())),
            scenario=scenario,
            final_drawdown_pct=final_drawdown,
            peak_intensity=peak_intensity,
            total_actions=total_actions,
            agent_actions=actions[:100],  # Limit stored actions
            inferred_paths=inferred_paths,
            confidence=0.7 if raw_result.get('status') == 'completed' else 0.4,
        )

    def _graph_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute Cypher query against FalkorDB"""
        if not self.redis_client:
            return []

        try:
            result = self.redis_client.execute_command('GRAPH.QUERY', self.graph_name, query)
            return self._parse_graph_result(result)
        except Exception as e:
            print(f"[BRIDGE] Query error: {e}")
            return []

    def _parse_graph_result(self, raw: Any) -> List[Dict[str, Any]]:
        """Parse FalkorDB result into list of dicts"""
        if not raw or not isinstance(raw, (list, tuple)):
            return []

        headers = raw[0] if raw else []
        data = raw[1] if len(raw) > 1 else []

        if not data or not isinstance(data, (list, tuple)):
            return []

        results = []
        for row in data:
            obj = {}
            for i, h in enumerate(headers):
                if i < len(row):
                    obj[h] = self._parse_element(row[i])
            results.append(obj)

        return results

    def _parse_element(self, elem: Any) -> Any:
        """Parse individual FalkorDB result element"""
        if elem is None:
            return None
        if isinstance(elem, (list, tuple)) and len(elem) == 2:
            return elem[1]  # [type, value] format
        return elem

    def _props_to_cypher(self, props: Dict[str, Any]) -> str:
        """Convert dict to Cypher property map string"""
        pairs = []
        for k, v in props.items():
            if v is None:
                continue
            if isinstance(v, str):
                pairs.append(f'{k}: "{v}"')
            elif isinstance(v, bool):
                pairs.append(f'{k}: {str(v).lower()}')
            elif isinstance(v, (int, float)):
                pairs.append(f'{k}: {v}')
        return '{' + ', '.join(pairs) + '}'


# ─── CLI / MAIN ───────────────────────────────────────────────────────────────

async def main():
    """Example usage"""
    bridge = MiroFishBridge()
    await bridge.connect()

    # Export ontology
    ontology = await bridge.export_ontology()
    print(f"Exported ontology with {ontology['metadata']['entity_count']} entities")

    # Create a crash scenario
    scenario = CrashScenario(
        scenario_id="test-2008-crisis",
        name="2008 Financial Crisis Simulation",
        description="Simulating the 2008 subprime mortgage crisis cascade",
        shock_entity="Lehman Brothers",
        shock_magnitude=1.5,
        shock_type="credit_freeze",
        simulation_rounds=72,
        agent_count=15,
    )

    # Run simulation
    result = await bridge.simulate_crash(scenario)
    print(f"Simulation complete: {result.final_drawdown_pct}% drawdown")

    bridge.close()


if __name__ == "__main__":
    asyncio.run(main())
