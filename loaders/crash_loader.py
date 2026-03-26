"""
Historical Crash Data Loader

Loads major historical market crashes into the Reality Graph with:
- Full causal chains (root causes → mechanisms → outcomes)
- Regime classifications
- Transmission channels
- Entity exposures and outcomes

This creates the "causal spine" that agents can query:
- "Why did the 2008 crash happen?"
- "What channels propagated the 1987 crash?"
- "Which entities were most exposed to COVID crash?"

Usage:
    python crash_loader.py                    # Load all crashes
    python crash_loader.py --crash "2008"     # Load specific crash
    python crash_loader.py --dry-run          # Preview without loading

[crash-loader-001]
"""

import os
import sys
import json
import hashlib
import argparse
import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field, asdict

import redis

# ─── CONFIGURATION ────────────────────────────────────────────────────────────

FALKORDB_URL = os.environ.get('FALKORDB_URL', os.environ.get('REDIS_URL', 'redis://localhost:6379'))
GRAPH_NAME = 'forage_v1'


# ─── DATA STRUCTURES ──────────────────────────────────────────────────────────

@dataclass
class HistoricalCrash:
    """Complete historical crash record"""
    crash_id: str
    name: str
    crash_type: str  # MarketCrash, SectorCrash, FlashCrash, etc.

    # Temporal
    start_date: str
    end_date: str
    peak_date: str

    # Indices affected
    primary_index: str
    affected_indices: List[str] = field(default_factory=list)

    # Magnitude
    peak_drawdown_pct: float = 0.0
    recovery_days: int = 0
    volatility_spike: float = 0.0  # VIX or equivalent

    # Regime classification
    regime_before: str = "normal"
    regime_during: str = "stressed"
    regime_after: str = "normal"

    # Root causes (ordered by importance)
    root_causes: List[Dict[str, Any]] = field(default_factory=list)

    # Transmission channels
    channels: List[Dict[str, Any]] = field(default_factory=list)

    # Mechanisms (how it propagated)
    mechanisms: List[Dict[str, Any]] = field(default_factory=list)

    # Entities affected (with exposure data)
    entities_affected: List[Dict[str, Any]] = field(default_factory=list)

    # Interventions (bailouts, circuit breakers, etc.)
    interventions: List[Dict[str, Any]] = field(default_factory=list)

    # Narratives
    narratives: List[str] = field(default_factory=list)

    # Sources/references
    sources: List[str] = field(default_factory=list)


# ─── HISTORICAL CRASH DATABASE ────────────────────────────────────────────────
# Comprehensive causal data for major crashes

HISTORICAL_CRASHES: List[HistoricalCrash] = [

    # ═══════════════════════════════════════════════════════════════════════════
    # 2008 GLOBAL FINANCIAL CRISIS
    # ═══════════════════════════════════════════════════════════════════════════
    HistoricalCrash(
        crash_id="crash-2008-gfc",
        name="2008 Global Financial Crisis",
        crash_type="MarketCrash",
        start_date="2007-10-09",
        end_date="2009-03-09",
        peak_date="2009-03-09",
        primary_index="S&P 500",
        affected_indices=["S&P 500", "DJIA", "FTSE 100", "DAX", "Nikkei 225", "Hang Seng"],
        peak_drawdown_pct=-56.8,
        recovery_days=1480,  # To new highs
        volatility_spike=80.86,  # VIX peak
        regime_before="bubble",
        regime_during="stressed",
        regime_after="risk_off",
        root_causes=[
            {
                "name": "Subprime Mortgage Crisis",
                "type": "CrashFactor",
                "causal_weight": 0.35,
                "description": "Widespread defaults on subprime mortgages triggered MBS collapse",
                "preceded_by_days": 365,
            },
            {
                "name": "Housing Bubble Burst",
                "type": "CrashFactor",
                "causal_weight": 0.25,
                "description": "US housing prices peaked in 2006, declining 30%+ nationally",
                "preceded_by_days": 730,
            },
            {
                "name": "Excessive Leverage",
                "type": "HiddenLeverage",
                "causal_weight": 0.20,
                "description": "Investment banks at 30-40x leverage on MBS positions",
                "preceded_by_days": 1000,
            },
            {
                "name": "CDO Mispricing",
                "type": "CrashFactor",
                "causal_weight": 0.10,
                "description": "Credit rating agencies failed to properly assess CDO risk",
                "preceded_by_days": 1500,
            },
            {
                "name": "Lehman Brothers Failure",
                "type": "BankRun",
                "causal_weight": 0.10,
                "description": "Lehman bankruptcy triggered global credit freeze",
                "preceded_by_days": 0,
            },
        ],
        channels=[
            {"type": "credit", "intensity": 0.95, "description": "Credit markets froze globally"},
            {"type": "liquidity", "intensity": 0.90, "description": "Interbank lending collapsed"},
            {"type": "sentiment", "intensity": 0.85, "description": "Panic selling across all assets"},
            {"type": "counterparty", "intensity": 0.88, "description": "Counterparty risk spiked"},
        ],
        mechanisms=[
            {
                "type": "MarginCall",
                "description": "Forced liquidations across hedge funds and banks",
                "entities_affected": 500,
                "amplification": 3.5,
            },
            {
                "type": "DebtContagion",
                "description": "Credit default swaps triggered cascading losses",
                "entities_affected": 200,
                "amplification": 4.0,
            },
            {
                "type": "PanicSelling",
                "description": "Mass retail and institutional selling",
                "entities_affected": 10000,
                "amplification": 2.0,
            },
        ],
        entities_affected=[
            {"name": "Lehman Brothers", "type": "FinancialInstitution", "outcome": "bankruptcy", "loss_pct": 100},
            {"name": "Bear Stearns", "type": "FinancialInstitution", "outcome": "acquired", "loss_pct": 93},
            {"name": "AIG", "type": "FinancialInstitution", "outcome": "bailout", "loss_pct": 97},
            {"name": "Merrill Lynch", "type": "FinancialInstitution", "outcome": "acquired", "loss_pct": 80},
            {"name": "Washington Mutual", "type": "FinancialInstitution", "outcome": "bankruptcy", "loss_pct": 100},
            {"name": "Citigroup", "type": "FinancialInstitution", "outcome": "bailout", "loss_pct": 77},
            {"name": "Goldman Sachs", "type": "FinancialInstitution", "outcome": "survived", "loss_pct": 45},
            {"name": "Morgan Stanley", "type": "FinancialInstitution", "outcome": "survived", "loss_pct": 55},
        ],
        interventions=[
            {"type": "BAILED_OUT", "entity": "AIG", "amount_usd": 182e9, "date": "2008-09-16"},
            {"type": "BAILED_OUT", "entity": "Citigroup", "amount_usd": 45e9, "date": "2008-11-23"},
            {"type": "INTERVENED_IN", "entity": "Federal Reserve", "action": "QE1", "date": "2008-11-25"},
            {"type": "INTERVENED_IN", "entity": "US Treasury", "action": "TARP", "amount_usd": 700e9, "date": "2008-10-03"},
        ],
        narratives=[
            "Too big to fail",
            "Moral hazard",
            "Wall Street greed",
            "Main Street vs Wall Street",
            "Great Recession",
        ],
        sources=[
            "FCIC Report 2011",
            "Paulson 'On The Brink'",
            "Geithner 'Stress Test'",
            "Bernanke 'The Courage to Act'",
        ],
    ),

    # ═══════════════════════════════════════════════════════════════════════════
    # 2020 COVID CRASH
    # ═══════════════════════════════════════════════════════════════════════════
    HistoricalCrash(
        crash_id="crash-2020-covid",
        name="2020 COVID-19 Market Crash",
        crash_type="MarketCrash",
        start_date="2020-02-19",
        end_date="2020-03-23",
        peak_date="2020-03-23",
        primary_index="S&P 500",
        affected_indices=["S&P 500", "DJIA", "NASDAQ", "FTSE 100", "DAX", "Nikkei 225", "Shanghai Composite"],
        peak_drawdown_pct=-33.9,
        recovery_days=148,  # V-shaped recovery
        volatility_spike=82.69,  # VIX peak
        regime_before="risk_on",
        regime_during="stressed",
        regime_after="risk_on",  # Rapid recovery
        root_causes=[
            {
                "name": "COVID-19 Pandemic",
                "type": "PandemicShock",
                "causal_weight": 0.60,
                "description": "Global pandemic forced economic shutdowns",
                "preceded_by_days": 60,
            },
            {
                "name": "Global Lockdowns",
                "type": "Policy",
                "causal_weight": 0.25,
                "description": "Governments mandated business closures",
                "preceded_by_days": 30,
            },
            {
                "name": "Oil Price War",
                "type": "CrashFactor",
                "causal_weight": 0.10,
                "description": "Saudi-Russia price war crashed oil to negative",
                "preceded_by_days": 14,
            },
            {
                "name": "Travel Ban Fears",
                "type": "GeopoliticalShock",
                "causal_weight": 0.05,
                "description": "Fear of complete travel shutdown",
                "preceded_by_days": 21,
            },
        ],
        channels=[
            {"type": "sentiment", "intensity": 0.95, "description": "Fear of unknown pandemic impact"},
            {"type": "liquidity", "intensity": 0.75, "description": "Dash for cash"},
            {"type": "supply_chain", "intensity": 0.70, "description": "China factory shutdowns"},
            {"type": "credit", "intensity": 0.60, "description": "Corporate bond spreads widened"},
        ],
        mechanisms=[
            {
                "type": "PanicSelling",
                "description": "Fastest bear market in history",
                "entities_affected": 50000,
                "amplification": 5.0,
            },
            {
                "type": "AlgorithmicCascade",
                "description": "Algo selling triggered circuit breakers",
                "entities_affected": 3000,
                "amplification": 2.5,
            },
            {
                "type": "MarginCall",
                "description": "Leveraged positions liquidated",
                "entities_affected": 1000,
                "amplification": 2.0,
            },
        ],
        entities_affected=[
            {"name": "Airlines", "type": "Industry", "outcome": "crashed", "loss_pct": 60},
            {"name": "Cruise Lines", "type": "Industry", "outcome": "crashed", "loss_pct": 75},
            {"name": "Hotels", "type": "Industry", "outcome": "crashed", "loss_pct": 55},
            {"name": "Oil Companies", "type": "Industry", "outcome": "crashed", "loss_pct": 50},
            {"name": "Boeing", "type": "Corporation", "outcome": "bailout", "loss_pct": 70},
            {"name": "Delta Airlines", "type": "Corporation", "outcome": "bailout", "loss_pct": 60},
        ],
        interventions=[
            {"type": "INTERVENED_IN", "entity": "Federal Reserve", "action": "Unlimited QE", "date": "2020-03-23"},
            {"type": "INTERVENED_IN", "entity": "US Congress", "action": "CARES Act", "amount_usd": 2.2e12, "date": "2020-03-27"},
            {"type": "CIRCUIT_BREAKER", "entity": "NYSE", "action": "Trading halts", "date": "2020-03-09"},
            {"type": "CIRCUIT_BREAKER", "entity": "NYSE", "action": "Trading halts", "date": "2020-03-12"},
            {"type": "CIRCUIT_BREAKER", "entity": "NYSE", "action": "Trading halts", "date": "2020-03-16"},
        ],
        narratives=[
            "Black swan event",
            "V-shaped recovery",
            "Fed put",
            "Don't fight the Fed",
            "Stonks only go up",
        ],
        sources=[
            "Fed speeches",
            "WHO reports",
            "IMF WEO",
        ],
    ),

    # ═══════════════════════════════════════════════════════════════════════════
    # 1987 BLACK MONDAY
    # ═══════════════════════════════════════════════════════════════════════════
    HistoricalCrash(
        crash_id="crash-1987-black-monday",
        name="1987 Black Monday",
        crash_type="FlashCrash",
        start_date="1987-10-14",
        end_date="1987-10-19",
        peak_date="1987-10-19",
        primary_index="DJIA",
        affected_indices=["DJIA", "S&P 500", "FTSE 100", "DAX", "Hang Seng", "Nikkei 225"],
        peak_drawdown_pct=-22.6,  # Single day
        recovery_days=435,
        volatility_spike=150.0,  # Implied vol equivalent
        regime_before="bubble",
        regime_during="stressed",
        regime_after="normal",
        root_causes=[
            {
                "name": "Portfolio Insurance",
                "type": "AlgorithmicCascade",
                "causal_weight": 0.40,
                "description": "Automatic hedging programs amplified selling",
                "preceded_by_days": 0,
            },
            {
                "name": "Trade Deficit Fears",
                "type": "GeopoliticalShock",
                "causal_weight": 0.20,
                "description": "US trade deficit widened, dollar weakening fears",
                "preceded_by_days": 14,
            },
            {
                "name": "Rising Interest Rates",
                "type": "MonetaryShock",
                "causal_weight": 0.20,
                "description": "Fed tightening after Louvre Accord",
                "preceded_by_days": 60,
            },
            {
                "name": "Market Overvaluation",
                "type": "CrashFactor",
                "causal_weight": 0.20,
                "description": "Market had risen 44% YTD before crash",
                "preceded_by_days": 300,
            },
        ],
        channels=[
            {"type": "liquidity", "intensity": 0.95, "description": "Market makers withdrew"},
            {"type": "sentiment", "intensity": 0.90, "description": "Global panic"},
            {"type": "algorithmic", "intensity": 0.85, "description": "Program trading cascade"},
        ],
        mechanisms=[
            {
                "type": "AlgorithmicCascade",
                "description": "Portfolio insurance feedback loop",
                "entities_affected": 200,
                "amplification": 10.0,
            },
            {
                "type": "PanicSelling",
                "description": "Human panic compounded algo selling",
                "entities_affected": 5000,
                "amplification": 3.0,
            },
        ],
        entities_affected=[
            {"name": "NYSE", "type": "Market", "outcome": "halted", "loss_pct": 22.6},
            {"name": "Hong Kong Exchange", "type": "Market", "outcome": "closed", "loss_pct": 45.8},
        ],
        interventions=[
            {"type": "INTERVENED_IN", "entity": "Federal Reserve", "action": "Liquidity injection", "date": "1987-10-20"},
            {"type": "CIRCUIT_BREAKER", "entity": "SEC", "action": "Created circuit breakers post-crash", "date": "1988-01-01"},
        ],
        narratives=[
            "Program trading culprit",
            "1929 repeat fears",
            "Market efficiency questioned",
        ],
        sources=[
            "Brady Commission Report",
            "SEC study",
        ],
    ),

    # ═══════════════════════════════════════════════════════════════════════════
    # 2000 DOT-COM CRASH
    # ═══════════════════════════════════════════════════════════════════════════
    HistoricalCrash(
        crash_id="crash-2000-dotcom",
        name="2000 Dot-Com Bubble Burst",
        crash_type="SectorCrash",
        start_date="2000-03-10",
        end_date="2002-10-09",
        peak_date="2002-10-09",
        primary_index="NASDAQ",
        affected_indices=["NASDAQ", "S&P 500", "DJIA"],
        peak_drawdown_pct=-78.4,  # NASDAQ
        recovery_days=5478,  # 15 years to new high for NASDAQ
        volatility_spike=45.0,
        regime_before="bubble",
        regime_during="stressed",
        regime_after="risk_off",
        root_causes=[
            {
                "name": "Tech Stock Overvaluation",
                "type": "CrashFactor",
                "causal_weight": 0.40,
                "description": "P/E ratios of 100+ for unprofitable companies",
                "preceded_by_days": 365,
            },
            {
                "name": "Fed Rate Hikes",
                "type": "MonetaryShock",
                "causal_weight": 0.25,
                "description": "Fed raised rates 6 times in 1999-2000",
                "preceded_by_days": 180,
            },
            {
                "name": "Earnings Reality Check",
                "type": "CrashFactor",
                "causal_weight": 0.20,
                "description": "Companies couldn't deliver on growth promises",
                "preceded_by_days": 90,
            },
            {
                "name": "Y2K Spending End",
                "type": "CrashFactor",
                "causal_weight": 0.15,
                "description": "Enterprise tech spending cliff after Y2K",
                "preceded_by_days": 100,
            },
        ],
        channels=[
            {"type": "sentiment", "intensity": 0.85, "description": "Irrational exuberance reversed"},
            {"type": "credit", "intensity": 0.60, "description": "Venture funding dried up"},
            {"type": "liquidity", "intensity": 0.55, "description": "IPO market frozen"},
        ],
        mechanisms=[
            {
                "type": "PanicSelling",
                "description": "Retail investors panic sold",
                "entities_affected": 100000,
                "amplification": 2.0,
            },
            {
                "type": "HerdBehavior",
                "description": "Momentum reversal",
                "entities_affected": 5000,
                "amplification": 3.0,
            },
        ],
        entities_affected=[
            {"name": "Pets.com", "type": "Corporation", "outcome": "bankruptcy", "loss_pct": 100},
            {"name": "Webvan", "type": "Corporation", "outcome": "bankruptcy", "loss_pct": 100},
            {"name": "WorldCom", "type": "Corporation", "outcome": "bankruptcy", "loss_pct": 100},
            {"name": "Enron", "type": "Corporation", "outcome": "bankruptcy", "loss_pct": 100},
            {"name": "Cisco", "type": "Corporation", "outcome": "survived", "loss_pct": 86},
            {"name": "Amazon", "type": "Corporation", "outcome": "survived", "loss_pct": 94},
            {"name": "Microsoft", "type": "Corporation", "outcome": "survived", "loss_pct": 65},
        ],
        interventions=[
            {"type": "INTERVENED_IN", "entity": "Federal Reserve", "action": "Rate cuts", "date": "2001-01-03"},
        ],
        narratives=[
            "Irrational exuberance",
            "New economy",
            "Eyeballs don't pay bills",
            "Back to fundamentals",
        ],
        sources=[
            "Shiller 'Irrational Exuberance'",
            "SEC filings",
        ],
    ),

    # ═══════════════════════════════════════════════════════════════════════════
    # 2022 CRYPTO CRASH
    # ═══════════════════════════════════════════════════════════════════════════
    HistoricalCrash(
        crash_id="crash-2022-crypto",
        name="2022 Crypto Winter",
        crash_type="SectorCrash",
        start_date="2021-11-10",
        end_date="2022-11-09",
        peak_date="2022-11-09",
        primary_index="BTC",
        affected_indices=["BTC", "ETH", "Total Crypto Market Cap"],
        peak_drawdown_pct=-77.0,  # BTC peak to trough
        recovery_days=500,  # Still recovering
        volatility_spike=100.0,
        regime_before="bubble",
        regime_during="stressed",
        regime_after="risk_off",
        root_causes=[
            {
                "name": "Fed Rate Hikes",
                "type": "MonetaryShock",
                "causal_weight": 0.30,
                "description": "Fed started aggressive tightening cycle",
                "preceded_by_days": 180,
            },
            {
                "name": "Terra Luna Collapse",
                "type": "CrashFactor",
                "causal_weight": 0.25,
                "description": "UST depeg triggered $60B wipeout",
                "preceded_by_days": 180,
            },
            {
                "name": "FTX Fraud",
                "type": "FraudRevelation",
                "causal_weight": 0.25,
                "description": "Second-largest exchange collapsed from fraud",
                "preceded_by_days": 0,
            },
            {
                "name": "3AC Bankruptcy",
                "type": "BankRun",
                "causal_weight": 0.10,
                "description": "Major hedge fund collapse",
                "preceded_by_days": 150,
            },
            {
                "name": "Celsius Freeze",
                "type": "LiquidityCrisis",
                "causal_weight": 0.10,
                "description": "Lending platform froze withdrawals",
                "preceded_by_days": 150,
            },
        ],
        channels=[
            {"type": "credit", "intensity": 0.90, "description": "Crypto lending collapsed"},
            {"type": "sentiment", "intensity": 0.95, "description": "Trust in crypto evaporated"},
            {"type": "liquidity", "intensity": 0.85, "description": "Exchanges restricted withdrawals"},
            {"type": "counterparty", "intensity": 0.88, "description": "Contagion across crypto lenders"},
        ],
        mechanisms=[
            {
                "type": "DebtContagion",
                "description": "Interconnected lending positions collapsed",
                "entities_affected": 100,
                "amplification": 5.0,
            },
            {
                "type": "BankRun",
                "description": "Withdrawal runs on exchanges/lenders",
                "entities_affected": 50,
                "amplification": 4.0,
            },
            {
                "type": "PanicSelling",
                "description": "Mass liquidations",
                "entities_affected": 1000000,
                "amplification": 2.0,
            },
        ],
        entities_affected=[
            {"name": "FTX", "type": "FinancialInstitution", "outcome": "bankruptcy", "loss_pct": 100},
            {"name": "Terra Luna", "type": "Asset", "outcome": "collapsed", "loss_pct": 100},
            {"name": "Celsius", "type": "FinancialInstitution", "outcome": "bankruptcy", "loss_pct": 100},
            {"name": "Three Arrows Capital", "type": "Corporation", "outcome": "bankruptcy", "loss_pct": 100},
            {"name": "Voyager Digital", "type": "FinancialInstitution", "outcome": "bankruptcy", "loss_pct": 100},
            {"name": "BlockFi", "type": "FinancialInstitution", "outcome": "bankruptcy", "loss_pct": 100},
        ],
        interventions=[
            {"type": "INTERVENED_IN", "entity": "SEC", "action": "Enforcement actions", "date": "2022-12-13"},
            {"type": "INTERVENED_IN", "entity": "DOJ", "action": "Criminal charges SBF", "date": "2022-12-12"},
        ],
        narratives=[
            "Crypto winter",
            "Not your keys not your coins",
            "Contagion",
            "Due diligence failure",
        ],
        sources=[
            "Court filings",
            "SEC complaints",
            "Chain analysis",
        ],
    ),
]


# ─── CRASH LOADER ─────────────────────────────────────────────────────────────

class CrashLoader:
    """Load historical crash data into Reality Graph"""

    def __init__(
        self,
        redis_url: str = FALKORDB_URL,
        graph_name: str = GRAPH_NAME,
    ):
        self.redis_url = redis_url
        self.graph_name = graph_name
        self.client: Optional[redis.Redis] = None

    def connect(self):
        """Connect to FalkorDB"""
        self.client = redis.from_url(self.redis_url)
        self.client.ping()
        print(f"[LOADER] Connected to FalkorDB")

    def close(self):
        """Close connection"""
        if self.client:
            self.client.close()

    def load_all(self, dry_run: bool = False) -> Dict[str, int]:
        """Load all historical crashes"""
        stats = {"crashes": 0, "entities": 0, "relationships": 0}

        for crash in HISTORICAL_CRASHES:
            crash_stats = self.load_crash(crash, dry_run=dry_run)
            stats["crashes"] += 1
            stats["entities"] += crash_stats.get("entities", 0)
            stats["relationships"] += crash_stats.get("relationships", 0)

        return stats

    def load_crash(
        self,
        crash: HistoricalCrash,
        dry_run: bool = False
    ) -> Dict[str, int]:
        """Load a single crash with all its causal structure"""

        print(f"\n[LOADER] Loading: {crash.name}")

        entities_created = 0
        relationships_created = 0

        # 1. Create crash node
        crash_props = self._crash_to_props(crash)
        if not dry_run:
            self._create_node(crash.crash_id, crash.crash_type, crash.name, crash_props)
        entities_created += 1

        # 2. Create root cause nodes and TRIGGERED edges
        for cause in crash.root_causes:
            cause_id = self._gen_id(cause["name"])

            if not dry_run:
                self._create_node(
                    cause_id,
                    cause.get("type", "CrashFactor"),
                    cause["name"],
                    {
                        "description": cause.get("description", ""),
                        "preceded_by_days": cause.get("preceded_by_days", 0),
                    }
                )
                self._create_edge(
                    cause_id, crash.crash_id,
                    "TRIGGERED",
                    {
                        "causal_weight": cause["causal_weight"],
                        "mechanism": cause.get("description", ""),
                    }
                )

            entities_created += 1
            relationships_created += 1

        # 3. Create channel nodes and TRANSMITTED_THROUGH edges
        for channel in crash.channels:
            channel_id = f"channel-{channel['type']}"

            if not dry_run:
                self._create_node(
                    channel_id,
                    "RiskChannel",
                    channel["type"].title() + " Channel",
                    {"channel_type": channel["type"]}
                )
                self._create_edge(
                    crash.crash_id, channel_id,
                    "TRANSMITTED_THROUGH",
                    {
                        "intensity": channel["intensity"],
                        "description": channel.get("description", ""),
                    }
                )

            entities_created += 1
            relationships_created += 1

        # 4. Create mechanism nodes and AMPLIFIED_CRASH edges
        for mechanism in crash.mechanisms:
            mech_id = self._gen_id(f"{crash.crash_id}-{mechanism['type']}")

            if not dry_run:
                self._create_node(
                    mech_id,
                    mechanism["type"],
                    mechanism["type"].replace("_", " ").title(),
                    {
                        "description": mechanism.get("description", ""),
                        "amplification_factor": mechanism.get("amplification", 1.0),
                    }
                )
                self._create_edge(
                    mech_id, crash.crash_id,
                    "AMPLIFIED_CRASH",
                    {
                        "entities_affected": mechanism.get("entities_affected", 0),
                        "amplification": mechanism.get("amplification", 1.0),
                    }
                )

            entities_created += 1
            relationships_created += 1

        # 5. Create affected entity nodes and EXPOSED_TO edges
        for entity in crash.entities_affected:
            entity_id = self._gen_id(entity["name"])

            if not dry_run:
                self._create_node(
                    entity_id,
                    entity.get("type", "Corporation"),
                    entity["name"],
                    {"outcome": entity.get("outcome", "unknown")}
                )
                self._create_edge(
                    entity_id, crash.crash_id,
                    "EXPOSED_TO",
                    {
                        "loss_pct": entity.get("loss_pct", 0),
                        "outcome": entity.get("outcome", "unknown"),
                    }
                )

            entities_created += 1
            relationships_created += 1

        # 6. Create intervention edges
        for intervention in crash.interventions:
            intervenor_id = self._gen_id(intervention["entity"])

            if not dry_run:
                # Ensure intervenor exists
                self._create_node(
                    intervenor_id,
                    "LegalEntity",
                    intervention["entity"],
                    {}
                )
                self._create_edge(
                    intervenor_id, crash.crash_id,
                    intervention["type"],
                    {
                        "action": intervention.get("action", ""),
                        "amount_usd": intervention.get("amount_usd", 0),
                        "date": intervention.get("date", ""),
                    }
                )

            relationships_created += 1

        # 7. Create regime node and link
        regime_id = f"regime-{crash.regime_during}"
        if not dry_run:
            self._create_node(
                regime_id,
                "Regime",
                crash.regime_during.replace("_", " ").title(),
                {"regime_type": crash.regime_during}
            )
            self._create_edge(
                crash.crash_id, regime_id,
                "part_of",
                {"start_date": crash.start_date, "end_date": crash.end_date}
            )

        entities_created += 1
        relationships_created += 1

        # 8. Create narrative nodes
        for narrative in crash.narratives:
            narr_id = self._gen_id(f"narrative-{narrative}")
            if not dry_run:
                self._create_node(
                    narr_id,
                    "Narrative",
                    narrative,
                    {"theme": narrative}
                )
                self._create_edge(
                    crash.crash_id, narr_id,
                    "supports",
                    {}
                )

            entities_created += 1
            relationships_created += 1

        print(f"  Created {entities_created} entities, {relationships_created} relationships")

        return {"entities": entities_created, "relationships": relationships_created}

    # ─── PRIVATE HELPERS ──────────────────────────────────────────────────────

    def _crash_to_props(self, crash: HistoricalCrash) -> Dict[str, Any]:
        """Convert crash to property dict"""
        return {
            "start_date": crash.start_date,
            "end_date": crash.end_date,
            "peak_date": crash.peak_date,
            "primary_index": crash.primary_index,
            "peak_drawdown_pct": crash.peak_drawdown_pct,
            "recovery_days": crash.recovery_days,
            "volatility_spike": crash.volatility_spike,
            "regime_before": crash.regime_before,
            "regime_during": crash.regime_during,
            "regime_after": crash.regime_after,
        }

    def _gen_id(self, name: str) -> str:
        """Generate deterministic ID from name"""
        return hashlib.sha3_256(name.lower().encode()).hexdigest()[:16]

    def _create_node(
        self,
        node_id: str,
        node_type: str,
        name: str,
        props: Dict[str, Any]
    ):
        """Create or merge a node in FalkorDB"""
        if not self.client:
            return

        props_str = self._props_to_cypher({
            "type": node_type,
            "name": name,
            "name_lower": name.lower(),
            **props,
        })

        query = f"""
            MERGE (n:Entity {{id: "{node_id}"}})
            SET n += {props_str}
        """

        try:
            self.client.execute_command('GRAPH.QUERY', self.graph_name, query)
        except Exception as e:
            print(f"  [ERROR] Creating node {name}: {e}")

    def _create_edge(
        self,
        from_id: str,
        to_id: str,
        relation: str,
        props: Dict[str, Any]
    ):
        """Create or merge an edge in FalkorDB"""
        if not self.client:
            return

        edge_id = hashlib.sha3_256(f"{from_id}:{relation}:{to_id}".encode()).hexdigest()[:16]

        props_str = self._props_to_cypher({
            "id": edge_id,
            "relation": relation,
            **props,
        })

        query = f"""
            MATCH (a:Entity {{id: "{from_id}"}})
            MATCH (b:Entity {{id: "{to_id}"}})
            MERGE (a)-[r:RELATES]->(b)
            SET r += {props_str}
        """

        try:
            self.client.execute_command('GRAPH.QUERY', self.graph_name, query)
        except Exception as e:
            print(f"  [ERROR] Creating edge {from_id}->{to_id}: {e}")

    def _props_to_cypher(self, props: Dict[str, Any]) -> str:
        """Convert dict to Cypher property map"""
        pairs = []
        for k, v in props.items():
            if v is None:
                continue
            if isinstance(v, str):
                escaped = v.replace('\\', '\\\\').replace('"', '\\"')
                pairs.append(f'{k}: "{escaped}"')
            elif isinstance(v, bool):
                pairs.append(f'{k}: {str(v).lower()}')
            elif isinstance(v, (int, float)):
                pairs.append(f'{k}: {v}')
            elif isinstance(v, list):
                pairs.append(f'{k}: "{json.dumps(v)}"')
        return '{' + ', '.join(pairs) + '}'


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Load historical crash data into Reality Graph")
    parser.add_argument("--crash", type=str, help="Load specific crash by name/ID")
    parser.add_argument("--dry-run", action="store_true", help="Preview without loading")
    parser.add_argument("--list", action="store_true", help="List available crashes")
    args = parser.parse_args()

    if args.list:
        print("\nAvailable crashes:")
        for crash in HISTORICAL_CRASHES:
            print(f"  - {crash.crash_id}: {crash.name} ({crash.peak_drawdown_pct}%)")
        return

    loader = CrashLoader()
    loader.connect()

    try:
        if args.crash:
            # Find specific crash
            found = None
            for crash in HISTORICAL_CRASHES:
                if args.crash.lower() in crash.crash_id.lower() or args.crash.lower() in crash.name.lower():
                    found = crash
                    break

            if found:
                stats = loader.load_crash(found, dry_run=args.dry_run)
                print(f"\nLoaded {found.name}: {stats}")
            else:
                print(f"Crash not found: {args.crash}")
        else:
            stats = loader.load_all(dry_run=args.dry_run)
            print(f"\n=== Summary ===")
            print(f"Crashes loaded: {stats['crashes']}")
            print(f"Entities created: {stats['entities']}")
            print(f"Relationships created: {stats['relationships']}")

    finally:
        loader.close()


if __name__ == "__main__":
    main()
