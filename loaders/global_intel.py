#!/usr/bin/env python3
"""
Forage Global Intel Collector — Comprehensive worldwide data
Extends intel_collector.py with deeper global coverage.

Additional Sources:
  1. UN Data (SDG indicators, population, trade)
  2. IMF (economic outlook, country reports)
  3. CSIS/Think Tank feeds (geopolitical analysis)
  4. GDELT (global events database)
  5. Trade data (WTO, UNCTAD)
  6. Crypto/Financial (CoinGecko, major exchanges)
  7. Climate/Energy (IEA, carbon data)

Usage:
  python global_intel.py --all
  python global_intel.py --un --imf --trade
"""

import os
import sys
import json
import time
import requests
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import base classes from intel_collector
try:
    from intel_collector import Entity, Connection, IntelBatch, post_batches, BATCH_SIZE, HEADERS, GRAPH_URL
except ImportError:
    # Standalone mode - define locally
    GRAPH_URL = os.environ.get("GRAPH_URL", "https://forage-graph-production.up.railway.app")
    GRAPH_SECRET = os.environ.get("GRAPH_SECRET", "6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959")
    BATCH_SIZE = 25
    HEADERS = {"Authorization": f"Bearer {GRAPH_SECRET}", "Content-Type": "application/json"}

    @dataclass
    class Entity:
        type: str
        name: str
        properties: Dict[str, Any]
        source: str
        confidence: float = 0.75

    @dataclass
    class Connection:
        from_type: str
        from_name: str
        to_type: str
        to_name: str
        relation: str
        properties: Dict[str, Any]
        source: str
        confidence: float = 0.8

    @dataclass
    class IntelBatch:
        entities: List[Entity]
        connections: List[Connection]
        source: str

        def to_dict(self) -> Dict:
            return {
                "entities": [asdict(e) for e in self.entities],
                "connections": [asdict(c) for c in self.connections]
            }

    def post_batches(batches, source):
        total_e, total_c, errors = 0, 0, []
        for i, batch in enumerate(batches):
            try:
                resp = requests.post(f"{GRAPH_URL}/ingest/bulk", headers=HEADERS, json=batch.to_dict(), timeout=30)
                if resp.status_code == 201:
                    r = resp.json()
                    total_e += r.get("entities", {}).get("added", 0) + r.get("entities", {}).get("merged", 0)
                    total_c += r.get("connections", {}).get("added", 0) + r.get("connections", {}).get("merged", 0)
            except Exception as e:
                errors.append(str(e))
        return {"source": source, "entities": total_e, "connections": total_c, "errors": errors}

USER_AGENT = "ForageGlobalIntel/1.0 (https://forage.ai)"

# ─── UN DATA COLLECTOR ─────────────────────────────────────────────────────────

UN_DATA_API = "https://data.un.org/ws/rest/data"
UN_COUNTRY_API = "https://restcountries.com/v3.1/all"

def collect_un_countries() -> List[IntelBatch]:
    """Collect comprehensive country data from REST Countries API."""
    print("[UN/Countries] Starting collection...")
    batches = []
    entities = []
    connections = []

    try:
        print("  [Countries] Fetching all countries...")
        resp = requests.get(UN_COUNTRY_API, headers={"User-Agent": USER_AGENT}, timeout=30)
        resp.raise_for_status()
        countries = resp.json()

        for c in countries:
            name = c.get("name", {}).get("common", "")
            official = c.get("name", {}).get("official", "")

            if not name:
                continue

            # Main country entity
            entities.append(Entity(
                type="Jurisdiction",
                name=name,
                properties={
                    "official_name": official,
                    "iso2": c.get("cca2"),
                    "iso3": c.get("cca3"),
                    "un_code": c.get("ccn3"),
                    "population": c.get("population"),
                    "area_km2": c.get("area"),
                    "region": c.get("region"),
                    "subregion": c.get("subregion"),
                    "capital": c.get("capital", [None])[0] if c.get("capital") else None,
                    "languages": list(c.get("languages", {}).values()),
                    "currencies": list(c.get("currencies", {}).keys()),
                    "timezones": c.get("timezones"),
                    "tld": c.get("tld"),
                    "flag_emoji": c.get("flag"),
                    "driving_side": c.get("car", {}).get("side"),
                    "un_member": c.get("unMember", False),
                    "landlocked": c.get("landlocked", False),
                    "independent": c.get("independent", True),
                },
                source="restcountries",
                confidence=0.95
            ))

            # Capital city
            capitals = c.get("capital", [])
            for cap in capitals:
                if cap:
                    entities.append(Entity(
                        type="Location",
                        name=cap,
                        properties={"type": "capital_city", "country": name},
                        source="restcountries"
                    ))
                    connections.append(Connection(
                        from_type="Location", from_name=cap,
                        to_type="Jurisdiction", to_name=name,
                        relation="located_in",
                        properties={"role": "capital"},
                        source="restcountries"
                    ))

            # Region hierarchy
            region = c.get("region")
            subregion = c.get("subregion")
            if region:
                entities.append(Entity(
                    type="Location",
                    name=region,
                    properties={"type": "continent_region"},
                    source="restcountries"
                ))
                connections.append(Connection(
                    from_type="Jurisdiction", from_name=name,
                    to_type="Location", to_name=region,
                    relation="part_of",
                    properties={},
                    source="restcountries"
                ))

            if subregion:
                entities.append(Entity(
                    type="Location",
                    name=subregion,
                    properties={"type": "subregion", "parent_region": region},
                    source="restcountries"
                ))
                connections.append(Connection(
                    from_type="Jurisdiction", from_name=name,
                    to_type="Location", to_name=subregion,
                    relation="part_of",
                    properties={},
                    source="restcountries"
                ))

            # Borders
            borders = c.get("borders", [])
            for border_code in borders:
                # We'll create a relationship using ISO3 code - will resolve on merge
                connections.append(Connection(
                    from_type="Jurisdiction", from_name=name,
                    to_type="Jurisdiction", to_name=border_code,  # ISO3 - will need resolution
                    relation="related_to",
                    properties={"type": "shares_border"},
                    source="restcountries"
                ))

            # Currencies
            for currency_code, currency_info in c.get("currencies", {}).items():
                currency_name = currency_info.get("name", currency_code)
                entities.append(Entity(
                    type="FinancialInstrument",
                    name=currency_name,
                    properties={
                        "code": currency_code,
                        "symbol": currency_info.get("symbol"),
                        "type": "currency"
                    },
                    source="restcountries"
                ))
                connections.append(Connection(
                    from_type="Jurisdiction", from_name=name,
                    to_type="FinancialInstrument", to_name=currency_name,
                    relation="issues",
                    properties={"type": "national_currency"},
                    source="restcountries"
                ))

        print(f"  [Countries] Collected {len(entities)} entities, {len(connections)} connections")

    except Exception as e:
        print(f"  [Countries Error] {e}")

    # Batch
    for i in range(0, max(len(entities), len(connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=entities[i:i+BATCH_SIZE],
            connections=connections[i:i+BATCH_SIZE],
            source="un_countries"
        ))

    return batches

# ─── IMF DATA COLLECTOR ────────────────────────────────────────────────────────

IMF_API = "https://www.imf.org/external/datamapper/api/v1"

def collect_imf() -> List[IntelBatch]:
    """Collect economic indicators from IMF."""
    print("[IMF] Starting collection...")
    batches = []
    entities = []
    connections = []

    try:
        # Get available indicators
        print("  [IMF] Fetching economic indicators...")
        indicators_resp = requests.get(f"{IMF_API}/indicators", timeout=30)

        if indicators_resp.status_code == 200:
            indicators = indicators_resp.json().get("indicators", {})

            for code, info in list(indicators.items())[:20]:  # Top 20 indicators
                entities.append(Entity(
                    type="Indicator",
                    name=info.get("label", code),
                    properties={
                        "imf_code": code,
                        "description": info.get("description", ""),
                        "unit": info.get("unit", ""),
                        "source": "IMF"
                    },
                    source="imf",
                    confidence=0.90
                ))

        # Get country data for key indicators
        key_indicators = ["NGDP_RPCH", "PCPIPCH", "LUR", "BCA_NGDPD"]  # GDP growth, inflation, unemployment, current account
        for ind in key_indicators:
            try:
                data_resp = requests.get(f"{IMF_API}/{ind}", timeout=30)
                if data_resp.status_code == 200:
                    data = data_resp.json().get("values", {}).get(ind, {})

                    for country_code, years in list(data.items())[:50]:
                        if years:
                            latest_year = max(years.keys())
                            latest_value = years[latest_year]

                            # Create signal/trend
                            connections.append(Connection(
                                from_type="Indicator", from_name=ind,
                                to_type="Jurisdiction", to_name=country_code,
                                relation="impacts",
                                properties={
                                    "year": latest_year,
                                    "value": latest_value,
                                    "unit": "percent"
                                },
                                source="imf"
                            ))
            except:
                pass

        print(f"  [IMF] Collected {len(entities)} indicators, {len(connections)} country links")

    except Exception as e:
        print(f"  [IMF Error] {e}")

    # Batch
    for i in range(0, max(len(entities), len(connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=entities[i:i+BATCH_SIZE],
            connections=connections[i:i+BATCH_SIZE],
            source="imf"
        ))

    return batches

# ─── GDELT EVENTS COLLECTOR ────────────────────────────────────────────────────

GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"

def collect_gdelt(limit: int = 500) -> List[IntelBatch]:
    """Collect global events from GDELT."""
    print("[GDELT] Starting collection...")
    batches = []
    entities = []
    connections = []

    try:
        # Query recent news with geopolitical themes
        themes = [
            "WB_2734_TRADE_POLICY",
            "TAX_FNCACT_SANCTIONS",
            "GENERAL_GOVERNMENT",
            "WB_635_CONFLICT_AND_VIOLENCE"
        ]

        for theme in themes:
            print(f"  [GDELT] Fetching {theme}...")
            try:
                params = {
                    "query": theme,
                    "mode": "artlist",
                    "maxrecords": 100,
                    "format": "json"
                }
                resp = requests.get(GDELT_API, params=params, timeout=30, headers={"User-Agent": USER_AGENT})

                if resp.status_code == 200:
                    articles = resp.json().get("articles", [])

                    for article in articles:
                        title = article.get("title", "")[:200]
                        if not title:
                            continue

                        entities.append(Entity(
                            type="Event",
                            name=title,
                            properties={
                                "url": article.get("url"),
                                "source_country": article.get("sourcecountry"),
                                "language": article.get("language"),
                                "domain": article.get("domain"),
                                "seendate": article.get("seendate"),
                                "theme": theme,
                                "tone": article.get("tone"),
                                "socialimage": article.get("socialimage")
                            },
                            source="gdelt",
                            confidence=0.70
                        ))

                        # Source connection
                        domain = article.get("domain", "")
                        if domain:
                            entities.append(Entity(
                                type="InformationSource",
                                name=domain,
                                properties={
                                    "type": "news_outlet",
                                    "country": article.get("sourcecountry")
                                },
                                source="gdelt"
                            ))
                            connections.append(Connection(
                                from_type="InformationSource", from_name=domain,
                                to_type="Event", to_name=title,
                                relation="related_to",
                                properties={"type": "reports"},
                                source="gdelt"
                            ))
            except Exception as e:
                print(f"    [GDELT] Theme {theme} error: {e}")

        print(f"  [GDELT] Collected {len(entities)} entities, {len(connections)} connections")

    except Exception as e:
        print(f"  [GDELT Error] {e}")

    # Batch
    for i in range(0, max(len(entities), len(connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=entities[i:i+BATCH_SIZE],
            connections=connections[i:i+BATCH_SIZE],
            source="gdelt"
        ))

    return batches

# ─── CRYPTO/FINANCIAL COLLECTOR ────────────────────────────────────────────────

COINGECKO_API = "https://api.coingecko.com/api/v3"

def collect_crypto() -> List[IntelBatch]:
    """Collect top cryptocurrencies and exchanges."""
    print("[Crypto] Starting collection...")
    batches = []
    entities = []
    connections = []

    try:
        # Top coins
        print("  [Crypto] Fetching top cryptocurrencies...")
        resp = requests.get(
            f"{COINGECKO_API}/coins/markets",
            params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100},
            timeout=30
        )

        if resp.status_code == 200:
            coins = resp.json()

            for coin in coins:
                entities.append(Entity(
                    type="FinancialInstrument",
                    name=coin.get("name", ""),
                    properties={
                        "symbol": coin.get("symbol", "").upper(),
                        "type": "cryptocurrency",
                        "market_cap_usd": coin.get("market_cap"),
                        "current_price_usd": coin.get("current_price"),
                        "price_change_24h": coin.get("price_change_percentage_24h"),
                        "total_volume": coin.get("total_volume"),
                        "circulating_supply": coin.get("circulating_supply"),
                        "ath": coin.get("ath"),
                        "ath_date": coin.get("ath_date"),
                        "image": coin.get("image")
                    },
                    source="coingecko",
                    confidence=0.90
                ))

        # Exchanges
        print("  [Crypto] Fetching exchanges...")
        resp = requests.get(f"{COINGECKO_API}/exchanges", params={"per_page": 50}, timeout=30)

        if resp.status_code == 200:
            exchanges = resp.json()

            for ex in exchanges:
                ex_name = ex.get("name", "")
                entities.append(Entity(
                    type="FinancialInstitution",
                    name=ex_name,
                    properties={
                        "type": "crypto_exchange",
                        "country": ex.get("country"),
                        "year_established": ex.get("year_established"),
                        "trust_score": ex.get("trust_score"),
                        "trust_score_rank": ex.get("trust_score_rank"),
                        "trade_volume_24h_btc": ex.get("trade_volume_24h_btc"),
                        "url": ex.get("url")
                    },
                    source="coingecko",
                    confidence=0.85
                ))

                # Location
                country = ex.get("country")
                if country:
                    connections.append(Connection(
                        from_type="FinancialInstitution", from_name=ex_name,
                        to_type="Jurisdiction", to_name=country,
                        relation="headquartered_in",
                        properties={},
                        source="coingecko"
                    ))

        print(f"  [Crypto] Collected {len(entities)} entities, {len(connections)} connections")

    except Exception as e:
        print(f"  [Crypto Error] {e}")

    # Batch
    for i in range(0, max(len(entities), len(connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=entities[i:i+BATCH_SIZE],
            connections=connections[i:i+BATCH_SIZE],
            source="crypto"
        ))

    return batches

# ─── TRADE DATA COLLECTOR ──────────────────────────────────────────────────────

COMTRADE_API = "https://comtradeapi.un.org/public/v1/preview"

def collect_trade() -> List[IntelBatch]:
    """Collect international trade relationships."""
    print("[Trade] Starting collection...")
    batches = []
    entities = []
    connections = []

    try:
        # Use World Bank trade data as fallback (Comtrade needs API key)
        print("  [Trade] Fetching trade indicators from World Bank...")

        indicators = [
            ("NE.EXP.GNFS.ZS", "exports_pct_gdp"),
            ("NE.IMP.GNFS.ZS", "imports_pct_gdp"),
            ("TG.VAL.TOTL.GD.ZS", "merchandise_trade_pct_gdp"),
        ]

        for ind_code, ind_name in indicators:
            url = f"https://api.worldbank.org/v2/country/all/indicator/{ind_code}?format=json&per_page=500&date=2022"
            resp = requests.get(url, timeout=30)

            if resp.status_code == 200:
                data = resp.json()
                if len(data) > 1:
                    for entry in data[1] or []:
                        country = entry.get("country", {}).get("value")
                        value = entry.get("value")

                        if country and value:
                            entities.append(Entity(
                                type="Indicator",
                                name=ind_name,
                                properties={
                                    "wb_code": ind_code,
                                    "type": "trade_indicator"
                                },
                                source="worldbank_trade"
                            ))
                            connections.append(Connection(
                                from_type="Indicator", from_name=ind_name,
                                to_type="Jurisdiction", to_name=country,
                                relation="impacts",
                                properties={
                                    "value": value,
                                    "year": 2022,
                                    "unit": "percent_gdp"
                                },
                                source="worldbank_trade"
                            ))

        print(f"  [Trade] Collected {len(entities)} entities, {len(connections)} connections")

    except Exception as e:
        print(f"  [Trade Error] {e}")

    # Batch
    for i in range(0, max(len(entities), len(connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=entities[i:i+BATCH_SIZE],
            connections=connections[i:i+BATCH_SIZE],
            source="trade"
        ))

    return batches

# ─── THINK TANK / GEOPOLITICAL FEEDS ───────────────────────────────────────────

GEOPOLITICAL_FEEDS = [
    ("https://www.cfr.org/rss/backgrounders", "Council on Foreign Relations"),
    ("https://carnegieendowment.org/rss/solr/?fa=solr_search_rss", "Carnegie Endowment"),
    ("https://www.brookings.edu/feed/", "Brookings Institution"),
    ("https://www.rand.org/feeds/rand_review.xml", "RAND Corporation"),
    ("https://www.chathamhouse.org/rss.xml", "Chatham House"),
]

def collect_geopolitical() -> List[IntelBatch]:
    """Collect geopolitical analysis from think tanks."""
    print("[Geopolitical] Starting collection...")
    batches = []
    entities = []
    connections = []
    import xml.etree.ElementTree as ET

    for url, source_name in GEOPOLITICAL_FEEDS:
        print(f"  [Geopolitical] Fetching {source_name}...")
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
            if resp.status_code != 200:
                continue

            root = ET.fromstring(resp.content)
            items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")

            # Source entity
            entities.append(Entity(
                type="InformationSource",
                name=source_name,
                properties={"type": "think_tank", "url": url},
                source="geopolitical"
            ))

            for item in items[:30]:
                title = item.findtext("title") or item.findtext("{http://www.w3.org/2005/Atom}title", "")
                link = item.findtext("link") or ""
                pub_date = item.findtext("pubDate") or item.findtext("{http://www.w3.org/2005/Atom}published", "")
                desc = item.findtext("description") or item.findtext("{http://www.w3.org/2005/Atom}summary", "")

                if not title:
                    continue

                entities.append(Entity(
                    type="Event",
                    name=title[:200],
                    properties={
                        "type": "analysis",
                        "url": link,
                        "published": pub_date,
                        "description": (desc or "")[:500],
                        "source": source_name
                    },
                    source=f"geopolitical:{source_name.lower().replace(' ', '_')}",
                    confidence=0.80
                ))

                connections.append(Connection(
                    from_type="InformationSource", from_name=source_name,
                    to_type="Event", to_name=title[:200],
                    relation="related_to",
                    properties={"type": "publishes"},
                    source="geopolitical"
                ))

        except Exception as e:
            print(f"    [Geopolitical] {source_name} error: {e}")

    print(f"  [Geopolitical] Collected {len(entities)} entities, {len(connections)} connections")

    # Batch
    for i in range(0, max(len(entities), len(connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=entities[i:i+BATCH_SIZE],
            connections=connections[i:i+BATCH_SIZE],
            source="geopolitical"
        ))

    return batches

# ─── ENERGY/CLIMATE DATA ───────────────────────────────────────────────────────

def collect_energy() -> List[IntelBatch]:
    """Collect energy and climate data."""
    print("[Energy] Starting collection...")
    batches = []
    entities = []
    connections = []

    try:
        # World Bank energy indicators
        print("  [Energy] Fetching energy indicators...")
        indicators = [
            ("EG.USE.PCAP.KG.OE", "energy_use_per_capita"),
            ("EG.ELC.ACCS.ZS", "electricity_access_pct"),
            ("EN.ATM.CO2E.PC", "co2_emissions_per_capita"),
            ("EG.FEC.RNEW.ZS", "renewable_energy_pct"),
        ]

        for ind_code, ind_name in indicators:
            entities.append(Entity(
                type="Indicator",
                name=ind_name,
                properties={"wb_code": ind_code, "type": "energy_climate"},
                source="worldbank_energy"
            ))

            url = f"https://api.worldbank.org/v2/country/all/indicator/{ind_code}?format=json&per_page=300&date=2020"
            resp = requests.get(url, timeout=30)

            if resp.status_code == 200:
                data = resp.json()
                if len(data) > 1:
                    for entry in (data[1] or [])[:100]:
                        country = entry.get("country", {}).get("value")
                        value = entry.get("value")

                        if country and value:
                            connections.append(Connection(
                                from_type="Indicator", from_name=ind_name,
                                to_type="Jurisdiction", to_name=country,
                                relation="impacts",
                                properties={"value": value, "year": 2020},
                                source="worldbank_energy"
                            ))

        print(f"  [Energy] Collected {len(entities)} entities, {len(connections)} connections")

    except Exception as e:
        print(f"  [Energy Error] {e}")

    # Batch
    for i in range(0, max(len(entities), len(connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=entities[i:i+BATCH_SIZE],
            connections=connections[i:i+BATCH_SIZE],
            source="energy"
        ))

    return batches

# ─── MAJOR TECH COMPANIES ──────────────────────────────────────────────────────

TECH_COMPANIES = [
    ("Apple", {"ticker": "AAPL", "sector": "technology", "hq": "Cupertino"}),
    ("Microsoft", {"ticker": "MSFT", "sector": "technology", "hq": "Redmond"}),
    ("Alphabet", {"ticker": "GOOGL", "sector": "technology", "hq": "Mountain View"}),
    ("Amazon", {"ticker": "AMZN", "sector": "e-commerce", "hq": "Seattle"}),
    ("Meta", {"ticker": "META", "sector": "social_media", "hq": "Menlo Park"}),
    ("NVIDIA", {"ticker": "NVDA", "sector": "semiconductors", "hq": "Santa Clara"}),
    ("Tesla", {"ticker": "TSLA", "sector": "automotive", "hq": "Austin"}),
    ("Berkshire Hathaway", {"ticker": "BRK.A", "sector": "conglomerate", "hq": "Omaha"}),
    ("Taiwan Semiconductor", {"ticker": "TSM", "sector": "semiconductors", "hq": "Hsinchu"}),
    ("Samsung Electronics", {"ticker": "005930.KS", "sector": "electronics", "hq": "Seoul"}),
    ("Tencent", {"ticker": "0700.HK", "sector": "technology", "hq": "Shenzhen"}),
    ("Alibaba", {"ticker": "BABA", "sector": "e-commerce", "hq": "Hangzhou"}),
    ("ASML", {"ticker": "ASML", "sector": "semiconductors", "hq": "Veldhoven"}),
    ("JPMorgan Chase", {"ticker": "JPM", "sector": "banking", "hq": "New York"}),
    ("Visa", {"ticker": "V", "sector": "payments", "hq": "San Francisco"}),
    ("Johnson & Johnson", {"ticker": "JNJ", "sector": "healthcare", "hq": "New Brunswick"}),
    ("ExxonMobil", {"ticker": "XOM", "sector": "energy", "hq": "Spring"}),
    ("Walmart", {"ticker": "WMT", "sector": "retail", "hq": "Bentonville"}),
    ("Nestle", {"ticker": "NESN.SW", "sector": "consumer_goods", "hq": "Vevey"}),
    ("LVMH", {"ticker": "MC.PA", "sector": "luxury", "hq": "Paris"}),
]

def collect_major_companies() -> List[IntelBatch]:
    """Add major global companies with relationships."""
    print("[Major Companies] Starting collection...")
    batches = []
    entities = []
    connections = []

    for name, props in TECH_COMPANIES:
        entities.append(Entity(
            type="Corporation",
            name=name,
            properties={
                "ticker": props["ticker"],
                "sector": props["sector"],
                "type": "public_company",
                "fortune_500": True
            },
            source="curated",
            confidence=0.95
        ))

        # Industry
        entities.append(Entity(
            type="Industry",
            name=props["sector"],
            properties={},
            source="curated"
        ))
        connections.append(Connection(
            from_type="Corporation", from_name=name,
            to_type="Industry", to_name=props["sector"],
            relation="operates_in",
            properties={},
            source="curated"
        ))

        # HQ
        connections.append(Connection(
            from_type="Corporation", from_name=name,
            to_type="Location", to_name=props["hq"],
            relation="headquartered_in",
            properties={},
            source="curated"
        ))

    print(f"  [Major Companies] Collected {len(entities)} entities, {len(connections)} connections")

    batches.append(IntelBatch(entities=entities, connections=connections, source="major_companies"))
    return batches

# ─── MAIN ──────────────────────────────────────────────────────────────────────

def run_global_collectors() -> Dict:
    """Run all global collectors."""
    print("=" * 60)
    print("FORAGE GLOBAL INTEL COLLECTOR")
    print("=" * 60)
    start = time.time()

    results = {"sources": {}, "totals": {"entities": 0, "connections": 0, "errors": []}}

    collectors = [
        ("un_countries", collect_un_countries),
        ("imf", collect_imf),
        ("gdelt", collect_gdelt),
        ("crypto", collect_crypto),
        ("trade", collect_trade),
        ("geopolitical", collect_geopolitical),
        ("energy", collect_energy),
        ("major_companies", collect_major_companies),
    ]

    for source, fn in collectors:
        print()
        try:
            batches = fn()
            if batches:
                result = post_batches(batches, source)
                results["sources"][source] = result
                results["totals"]["entities"] += result.get("entities", 0)
                results["totals"]["connections"] += result.get("connections", 0)
        except Exception as e:
            results["sources"][source] = {"error": str(e)}
            results["totals"]["errors"].append(f"{source}: {e}")

    elapsed = time.time() - start
    print()
    print("=" * 60)
    print(f"Duration: {elapsed:.1f}s")
    print(f"Total Entities: {results['totals']['entities']}")
    print(f"Total Connections: {results['totals']['connections']}")
    return results

def main():
    parser = argparse.ArgumentParser(description="Forage Global Intel Collector")
    parser.add_argument("--all", action="store_true", help="Run all collectors")
    parser.add_argument("--countries", action="store_true", help="UN countries data")
    parser.add_argument("--imf", action="store_true", help="IMF economic data")
    parser.add_argument("--gdelt", action="store_true", help="GDELT events")
    parser.add_argument("--crypto", action="store_true", help="Crypto markets")
    parser.add_argument("--trade", action="store_true", help="Trade data")
    parser.add_argument("--geopolitical", action="store_true", help="Think tank feeds")
    parser.add_argument("--energy", action="store_true", help="Energy/climate")
    parser.add_argument("--companies", action="store_true", help="Major companies")
    parser.add_argument("--output", type=str, help="Output JSON file")

    args = parser.parse_args()

    if not any(vars(args).values()):
        args.all = True

    if args.all:
        results = run_global_collectors()
    else:
        results = {"sources": {}}
        if args.countries:
            batches = collect_un_countries()
            if batches: results["sources"]["un_countries"] = post_batches(batches, "un_countries")
        if args.imf:
            batches = collect_imf()
            if batches: results["sources"]["imf"] = post_batches(batches, "imf")
        if args.gdelt:
            batches = collect_gdelt()
            if batches: results["sources"]["gdelt"] = post_batches(batches, "gdelt")
        if args.crypto:
            batches = collect_crypto()
            if batches: results["sources"]["crypto"] = post_batches(batches, "crypto")
        if args.trade:
            batches = collect_trade()
            if batches: results["sources"]["trade"] = post_batches(batches, "trade")
        if args.geopolitical:
            batches = collect_geopolitical()
            if batches: results["sources"]["geopolitical"] = post_batches(batches, "geopolitical")
        if args.energy:
            batches = collect_energy()
            if batches: results["sources"]["energy"] = post_batches(batches, "energy")
        if args.companies:
            batches = collect_major_companies()
            if batches: results["sources"]["major_companies"] = post_batches(batches, "major_companies")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved to {args.output}")

    return results

if __name__ == "__main__":
    main()
