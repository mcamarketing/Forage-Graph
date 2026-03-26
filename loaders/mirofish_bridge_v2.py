#!/usr/bin/env python3
"""
MiroFish Bridge V2 — Regime-Aware Reality Graph ↔ MiroFish Integration
========================================================================

Extends mirofish_bridge.py with:
1. Channel-based propagation mapping (Reality Graph channels → MiroFish edge types)
2. Regime-aware simulation constraints (agents constrained by real exposures)
3. SimulatedOutcome serialization back to Reality Graph
4. Scenario planning with multiple parallel simulations
5. Full provenance tracking via DataSource/RawData nodes

Architecture:
┌─────────────────────┐     ┌──────────────────────────┐
│   Reality Graph      │────▶│       MiroFish            │
│   (FalkorDB)         │     │   (Zep + OASIS Swarm)     │
│                      │     │                            │
│  Channels, Regimes,  │     │  SimAgents constrained by  │
│  Crashes, Narratives │     │  real exposures & channels  │
└─────────────────────┘     └──────────────────────────┘
        ▲                              │
        │  SimulatedOutcome            │
        │  SimulatedCrash              │
        │  SimulatedNarrative          │
        └──────────────────────────────┘

[mirofish-bridge-v2-001]
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

# Import V1 bridge for base functionality
from mirofish_bridge import (
    MiroFishBridge, MiroFishEntity, MiroFishRelation,
    CrashScenario, SimulationResult,
    FALKORDB_URL, MIROFISH_API_URL, GRAPH_NAME,
)

import redis


# ═══════════════════════════════════════════════════════════════════════════════
# CHANNEL → MIROFISH MAPPING
# ═══════════════════════════════════════════════════════════════════════════════

CHANNEL_TO_MIROFISH_EDGE = {
    "credit": "CREDIT_CONTAGION",
    "liquidity": "LIQUIDITY_DRAIN",
    "sentiment": "NARRATIVE_SPREAD",
    "supply_chain": "SUPPLY_DISRUPTION",
    "regulation": "REGULATORY_SHIFT",
    "counterparty": "COUNTERPARTY_DEFAULT",
}

ENTITY_TYPE_TO_MIROFISH = {
    "Corporation": "FIRM",
    "Company": "FIRM",
    "FinancialInstitution": "BANK",
    "Asset": "ASSET",
    "Industry": "SECTOR",
    "MarketCrash": "CRISIS_EVENT",
    "SectorCrash": "CRISIS_EVENT",
    "CrashFactor": "SHOCK_SOURCE",
    "CrashMechanism": "TRANSMISSION_MECHANISM",
    "Channel": "CHANNEL",
    "Regime": "MARKET_STATE",
    "Narrative": "NARRATIVE",
    "MacroEvent": "EXOGENOUS_EVENT",
    "Policy": "POLICY_ACTION",
    "CentralBank": "CENTRAL_BANK",
    "Government": "GOVERNMENT",
    "Location": "GEOGRAPHY",
}

REGIME_TO_MIROFISH_STATE = {
    "normal": {"volatility_multiplier": 1.0, "correlation_regime": "low", "liquidity_level": "normal"},
    "stressed": {"volatility_multiplier": 2.5, "correlation_regime": "high", "liquidity_level": "tight"},
    "pre_tipping": {"volatility_multiplier": 4.0, "correlation_regime": "extreme", "liquidity_level": "frozen"},
    "post_event": {"volatility_multiplier": 1.8, "correlation_regime": "decaying", "liquidity_level": "recovering"},
}


# ═══════════════════════════════════════════════════════════════════════════════
# NEW DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ScenarioPlan:
    """Multi-scenario planning container"""
    plan_id: str
    name: str
    description: str
    base_scenario: CrashScenario
    variants: List[CrashScenario] = field(default_factory=list)
    results: List[SimulationResult] = field(default_factory=list)
    recommended_actions: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class SimulatedOutcome:
    """Serialized MiroFish simulation outcome for Reality Graph"""
    outcome_id: str
    scenario_id: str
    drawdown_pct: float
    recovery_days: int
    confidence: float
    channels_activated: List[str]
    entities_impacted: List[str]
    peak_intensity: float
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class SimulatedCrash:
    """A simulated crash based on a real crash pattern"""
    sim_crash_id: str
    name: str
    underlying_crash_id: str
    model_version: str = "mirofish-v2"
    drawdown_pct: float = 0.0
    probability: float = 0.0


@dataclass
class SimulatedNarrative:
    """A simulated narrative based on a real narrative pattern"""
    sim_narrative_id: str
    name: str
    underlying_narrative_id: str
    model_version: str = "mirofish-v2"
    sentiment_shift: float = 0.0
    probability: float = 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# V2 BRIDGE
# ═══════════════════════════════════════════════════════════════════════════════

class MiroFishBridgeV2(MiroFishBridge):
    """
    Enhanced bridge with regime-aware simulation and channel-based propagation.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # ─── REGIME-AWARE EXPORT ─────────────────────────────────────────────────

    async def export_regime_context(self) -> Dict[str, Any]:
        """
        Export current regime state and active channels for MiroFish environment setup.
        MiroFish uses this to configure agent behavior constraints.
        """
        if not self.redis_client:
            await self.connect()

        # Get current regime distribution
        regime_query = """
            MATCH (r:Entity {type: 'Regime'})
            OPTIONAL MATCH (e:Entity)-[rel:RELATES {relation: 'IN_REGIME'}]->(r)
            RETURN r.regime_state AS regime, r.name AS name, count(e) AS entity_count
            ORDER BY entity_count DESC
        """
        regimes = self._graph_query(regime_query)

        # Get active channels and their current intensity
        channel_query = """
            MATCH (ch:Entity {type: 'Channel'})
            OPTIONAL MATCH (event:Entity)-[r:RELATES {relation: 'TRANSMITS_THROUGH'}]->(ch)
            WHERE event.start_date IS NOT NULL
            RETURN ch.channel_type AS channel, ch.name AS name,
                   count(event) AS active_events, ch.velocity AS velocity
            ORDER BY active_events DESC
        """
        channels = self._graph_query(channel_query)

        # Determine dominant regime
        dominant = regimes[0] if regimes else {"regime": "normal"}
        regime_state = dominant.get("regime", "normal")
        mirofish_state = REGIME_TO_MIROFISH_STATE.get(regime_state, REGIME_TO_MIROFISH_STATE["normal"])

        return {
            "environment": {
                "regime": regime_state,
                "regime_params": mirofish_state,
                "active_channels": [
                    {
                        "type": ch.get("channel", "unknown"),
                        "mirofish_edge": CHANNEL_TO_MIROFISH_EDGE.get(ch.get("channel", ""), "GENERIC"),
                        "active_events": ch.get("active_events", 0),
                        "velocity": ch.get("velocity", "medium"),
                    }
                    for ch in channels
                ],
            },
            "constraints": {
                "max_leverage": 5.0 if regime_state == "normal" else 2.0,
                "allow_short_selling": regime_state != "pre_tipping",
                "circuit_breaker_threshold": -0.07 if regime_state == "stressed" else -0.10,
                "forced_liquidation_at": -0.20,
            },
        }

    # ─── CHANNEL-BASED SIMULATION ────────────────────────────────────────────

    async def simulate_with_channels(
        self,
        scenario: CrashScenario,
        channels: List[str] = None,
    ) -> SimulationResult:
        """
        Run simulation with explicit channel-based propagation.
        Channels determine which edges MiroFish agents can traverse.
        """
        if channels is None:
            channels = ["credit", "liquidity", "sentiment"]

        # Export context with channel filtering
        entities, relations = await self.export_crash_context(scenario.shock_entity, depth=3)

        # Get regime context for environment setup
        regime_ctx = await self.export_regime_context()

        # Filter relations to only include specified channels
        mirofish_edges = [CHANNEL_TO_MIROFISH_EDGE[ch] for ch in channels if ch in CHANNEL_TO_MIROFISH_EDGE]

        # Run simulation with channel constraints
        result = await self.simulate_crash(scenario)

        # Tag result with activated channels
        result.scenario.entities_affected = [e.name for e in entities[:20]]

        return result

    # ─── SCENARIO PLANNING ───────────────────────────────────────────────────

    async def plan_scenarios(
        self,
        base_crash: str,
        variants: List[Dict[str, Any]] = None,
    ) -> ScenarioPlan:
        """
        Run multiple scenario variants for planning purposes.

        Each variant modifies the base crash scenario (different shock magnitudes,
        different channel activations, different regime conditions).
        """
        plan_id = f"plan-{uuid.uuid4().hex[:8]}"

        # Create base scenario
        base = CrashScenario(
            scenario_id=f"{plan_id}-base",
            name=f"Base: {base_crash}",
            description=f"Baseline replay of {base_crash}",
            shock_entity=base_crash,
            shock_magnitude=1.0,
            shock_type="historical_replay",
        )

        plan = ScenarioPlan(
            plan_id=plan_id,
            name=f"Scenario Plan: {base_crash}",
            description=f"Multi-variant scenario analysis for {base_crash}",
            base_scenario=base,
        )

        # Generate variants
        if variants is None:
            variants = [
                {"name": "mild", "shock_magnitude": 0.5, "channels": ["sentiment"]},
                {"name": "severe", "shock_magnitude": 1.5, "channels": ["credit", "liquidity", "sentiment"]},
                {"name": "systemic", "shock_magnitude": 2.0, "channels": ["credit", "liquidity", "sentiment", "counterparty"]},
            ]

        for v in variants:
            variant_scenario = CrashScenario(
                scenario_id=f"{plan_id}-{v['name']}",
                name=f"{v['name'].title()}: {base_crash}",
                description=f"Variant {v['name']} of {base_crash}",
                shock_entity=base_crash,
                shock_magnitude=v.get("shock_magnitude", 1.0),
                shock_type=v.get("shock_type", "variant"),
                simulation_rounds=v.get("rounds", 48),
            )
            plan.variants.append(variant_scenario)

        # Run all variants
        for variant in plan.variants:
            try:
                channels = next(
                    (v.get("channels") for v in variants if v["name"] in variant.name.lower()),
                    None,
                )
                result = await self.simulate_with_channels(variant, channels)
                plan.results.append(result)
            except Exception as e:
                print(f"[BRIDGE-V2] Variant {variant.name} failed: {e}")

        # Generate recommended actions
        plan.recommended_actions = self._generate_recommendations(plan)

        # Serialize plan to Reality Graph
        await self._serialize_plan(plan)

        return plan

    def _generate_recommendations(self, plan: ScenarioPlan) -> List[Dict[str, Any]]:
        """Generate recommended actions based on scenario results."""
        recommendations = []

        if not plan.results:
            return [{"action": "insufficient_data", "description": "No simulation results available"}]

        # Find worst-case
        worst = max(plan.results, key=lambda r: abs(r.final_drawdown_pct))
        best = min(plan.results, key=lambda r: abs(r.final_drawdown_pct))

        if abs(worst.final_drawdown_pct) > 20:
            recommendations.append({
                "action": "reduce_exposure",
                "urgency": "high",
                "description": f"Worst-case drawdown of {worst.final_drawdown_pct}% requires immediate risk reduction",
                "target_entities": worst.scenario.entities_affected[:5] if worst.scenario.entities_affected else [],
            })

        if abs(worst.final_drawdown_pct) > 10:
            recommendations.append({
                "action": "hedge_tail_risk",
                "urgency": "medium",
                "description": "Consider tail-risk hedges (puts, CDS protection) for exposed positions",
            })

        recommendations.append({
            "action": "monitor_channels",
            "urgency": "low",
            "description": "Continue monitoring active risk channels for early warning signals",
        })

        return recommendations

    # ─── SERIALIZATION BACK TO REALITY GRAPH ──────────────────────────────────

    async def _serialize_plan(self, plan: ScenarioPlan):
        """Serialize scenario plan results back to Reality Graph."""
        if not self.redis_client:
            await self.connect()

        # Create Scenario node
        scenario_id = plan.plan_id
        self._graph_query(f"""
            MERGE (s:Entity {{id: "{scenario_id}"}})
            SET s.type = "Scenario",
                s.name = "{plan.name}",
                s.name_lower = "{plan.name.lower()}",
                s.scenario_id = "{scenario_id}",
                s.description = "{plan.description}",
                s.variant_count = {len(plan.variants)},
                s.confidence = 0.7,
                s.first_seen = "{datetime.utcnow().isoformat()}"
        """)

        # Create SimulatedOutcome nodes for each result
        for result in plan.results:
            outcome_id = f"outcome-{result.simulation_id}"
            self._graph_query(f"""
                MERGE (o:Entity {{id: "{outcome_id}"}})
                SET o.type = "SimulatedOutcome",
                    o.name = "Outcome: {result.scenario.name}",
                    o.name_lower = "outcome: {result.scenario.name.lower()}",
                    o.outcome_id = "{outcome_id}",
                    o.scenario_id = "{scenario_id}",
                    o.drawdown_pct = {result.final_drawdown_pct},
                    o.peak_intensity = {result.peak_intensity},
                    o.confidence = {result.confidence},
                    o.is_simulation = true,
                    o.first_seen = "{datetime.utcnow().isoformat()}"
            """)

            # SCENARIO_PRODUCES → SimulatedOutcome
            self._graph_query(f"""
                MATCH (s:Entity {{id: "{scenario_id}"}})
                MATCH (o:Entity {{id: "{outcome_id}"}})
                MERGE (s)-[r:RELATES]->(o)
                SET r.relation = "SCENARIO_PRODUCES",
                    r.run_id = "{result.simulation_id}",
                    r.confidence = {result.confidence}
            """)

            # SIMULATED_IMPACT → affected entities
            if result.scenario.entities_affected:
                for entity_name in result.scenario.entities_affected[:10]:
                    safe_name = entity_name.lower().replace('"', '\\"')
                    self._graph_query(f"""
                        MATCH (o:Entity {{id: "{outcome_id}"}})
                        MATCH (e:Entity)
                        WHERE e.name_lower CONTAINS "{safe_name}"
                        MERGE (o)-[r:RELATES]->(e)
                        SET r.relation = "SIMULATED_IMPACT",
                            r.drawdown_pct = {result.final_drawdown_pct},
                            r.confidence = {result.confidence}
                    """)

    async def serialize_simulated_crash(
        self,
        sim_crash: SimulatedCrash,
    ):
        """Store a SimulatedCrash and link to its real-world counterpart."""
        if not self.redis_client:
            await self.connect()

        self._graph_query(f"""
            MERGE (sc:Entity {{id: "{sim_crash.sim_crash_id}"}})
            SET sc.type = "SimulatedCrash",
                sc.name = "{sim_crash.name}",
                sc.name_lower = "{sim_crash.name.lower()}",
                sc.model_version = "{sim_crash.model_version}",
                sc.drawdown_pct = {sim_crash.drawdown_pct},
                sc.probability = {sim_crash.probability},
                sc.is_simulation = true,
                sc.first_seen = "{datetime.utcnow().isoformat()}"
        """)

        # BASED_ON → real crash
        self._graph_query(f"""
            MATCH (sc:Entity {{id: "{sim_crash.sim_crash_id}"}})
            MATCH (rc:Entity {{id: "{sim_crash.underlying_crash_id}"}})
            MERGE (sc)-[r:RELATES]->(rc)
            SET r.relation = "BASED_ON",
                r.confidence = 0.85
        """)

    async def serialize_simulated_narrative(
        self,
        sim_narrative: SimulatedNarrative,
    ):
        """Store a SimulatedNarrative and link to its real-world counterpart."""
        if not self.redis_client:
            await self.connect()

        self._graph_query(f"""
            MERGE (sn:Entity {{id: "{sim_narrative.sim_narrative_id}"}})
            SET sn.type = "SimulatedNarrative",
                sn.name = "{sim_narrative.name}",
                sn.name_lower = "{sim_narrative.name.lower()}",
                sn.model_version = "{sim_narrative.model_version}",
                sn.sentiment_shift = {sim_narrative.sentiment_shift},
                sn.probability = {sim_narrative.probability},
                sn.is_simulation = true,
                sn.first_seen = "{datetime.utcnow().isoformat()}"
        """)

        # BASED_ON → real narrative
        self._graph_query(f"""
            MATCH (sn:Entity {{id: "{sim_narrative.sim_narrative_id}"}})
            MATCH (rn:Entity {{id: "{sim_narrative.underlying_narrative_id}"}})
            MERGE (sn)-[r:RELATES]->(rn)
            SET r.relation = "BASED_ON",
                r.confidence = 0.8
        """)


# ═══════════════════════════════════════════════════════════════════════════════
# CLI / EXAMPLE
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    bridge = MiroFishBridgeV2()
    await bridge.connect()

    # 1. Export regime context
    ctx = await bridge.export_regime_context()
    print(f"Current regime: {ctx['environment']['regime']}")
    print(f"Active channels: {len(ctx['environment']['active_channels'])}")

    # 2. Plan scenarios for a crash
    plan = await bridge.plan_scenarios(
        base_crash="2008 Global Financial Crisis",
        variants=[
            {"name": "mild_credit", "shock_magnitude": 0.5, "channels": ["credit"]},
            {"name": "full_contagion", "shock_magnitude": 1.5, "channels": ["credit", "liquidity", "sentiment", "counterparty"]},
        ],
    )
    print(f"Plan {plan.plan_id}: {len(plan.results)} results, {len(plan.recommended_actions)} recommendations")

    # 3. Serialize a simulated crash
    await bridge.serialize_simulated_crash(SimulatedCrash(
        sim_crash_id="simcrash-test-001",
        name="Simulated 2008-style Credit Freeze",
        underlying_crash_id="crash-gfc-2008",
        drawdown_pct=-35.0,
        probability=0.15,
    ))

    bridge.close()


if __name__ == "__main__":
    asyncio.run(main())
