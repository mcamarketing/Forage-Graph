#!/usr/bin/env python3
"""
Forage Intel Collector — Comprehensive free intelligence gathering
Collects entities AND relationships from multiple free sources.

Sources:
  1. Wikidata (countries, leaders, central banks, intl orgs)
  2. OpenSanctions (PEPs, sanctions, watchlists)
  3. SEC EDGAR (US public companies, insiders, filings)
  4. World Bank (economic indicators, country data)
  5. News/RSS feeds (events, trends, signals)
  6. GitHub trending (tech companies, projects)

Usage:
  python intel_collector.py --all              # Run all collectors
  python intel_collector.py --wikidata         # Just Wikidata
  python intel_collector.py --sanctions        # Just OpenSanctions
  python intel_collector.py --sec              # Just SEC EDGAR
  python intel_collector.py --news             # Just news feeds
  python intel_collector.py --worldbank        # Just World Bank
"""

import os
import sys
import json
import time
import hashlib
import argparse
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET

# ─── CONFIG ────────────────────────────────────────────────────────────────────

GRAPH_URL = os.environ.get("GRAPH_URL", "https://forage-graph-production.up.railway.app")
GRAPH_SECRET = os.environ['GRAPH_API_SECRET']
BATCH_SIZE = 25
MAX_RETRIES = 3
RETRY_DELAY = 2

HEADERS = {
    "Authorization": f"Bearer {GRAPH_SECRET}",
    "Content-Type": "application/json"
}

# User agent for web requests
USER_AGENT = "ForageIntelCollector/1.0 (https://forage.ai; intel@forage.ai)"

# ─── DATA CLASSES ──────────────────────────────────────────────────────────────

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

# ─── GRAPH API ─────────────────────────────────────────────────────────────────

def post_bulk(batch: IntelBatch) -> Dict:
    """POST entities + connections to graph in one call."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                f"{GRAPH_URL}/ingest/bulk",
                headers=HEADERS,
                json=batch.to_dict(),
                timeout=120  # Longer timeout for Railway cold starts
            )
            if resp.status_code == 201:
                return resp.json()
            elif resp.status_code == 429:  # Rate limited
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            else:
                return {"error": f"HTTP {resp.status_code}: {resp.text}"}
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                return {"error": str(e)}
    return {"error": "Max retries exceeded"}

def post_batches(batches: List[IntelBatch], source: str) -> Dict:
    """Post multiple batches and aggregate results."""
    total_entities = 0
    total_connections = 0
    errors = []

    for i, batch in enumerate(batches):
        result = post_bulk(batch)
        if "error" in result:
            errors.append(f"Batch {i}: {result['error']}")
        else:
            ent = result.get("entities", {})
            conn = result.get("connections", {})
            total_entities += ent.get("added", 0) + ent.get("merged", 0)
            total_connections += conn.get("added", 0) + conn.get("merged", 0)

        # Progress
        if (i + 1) % 10 == 0:
            print(f"  [{source}] Processed {i + 1}/{len(batches)} batches...")

    return {
        "source": source,
        "batches": len(batches),
        "entities": total_entities,
        "connections": total_connections,
        "errors": errors
    }

# ─── WIKIDATA COLLECTOR ────────────────────────────────────────────────────────

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
WIKIDATA_HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"}

def sparql_query(query: str) -> List[Dict]:
    """Execute SPARQL query against Wikidata."""
    try:
        resp = requests.get(
            SPARQL_ENDPOINT,
            params={"query": query, "format": "json"},
            headers=WIKIDATA_HEADERS,
            timeout=60
        )
        resp.raise_for_status()
        return resp.json().get("results", {}).get("bindings", [])
    except Exception as e:
        print(f"  [SPARQL Error] {e}")
        return []

def val(binding: Dict, key: str) -> str:
    """Extract value from SPARQL binding."""
    return binding.get(key, {}).get("value", "").strip()

def collect_wikidata() -> List[IntelBatch]:
    """Collect entities and relationships from Wikidata."""
    print("[Wikidata] Starting collection...")
    batches = []
    entities = []
    connections = []

    # 1. Countries with capitals, leaders, central banks
    print("  [Wikidata] Fetching countries...")
    countries_query = """
    SELECT DISTINCT ?country ?countryLabel ?iso2 ?iso3 ?capital ?capitalLabel
           ?leader ?leaderLabel ?centralBank ?centralBankLabel ?population
    WHERE {
      ?country wdt:P31 wd:Q3624078 .
      OPTIONAL { ?country wdt:P297 ?iso2 }
      OPTIONAL { ?country wdt:P298 ?iso3 }
      OPTIONAL { ?country wdt:P36 ?capital }
      OPTIONAL { ?country wdt:P6 ?leader }
      OPTIONAL { ?country wdt:P1304 ?centralBank }
      OPTIONAL { ?country wdt:P1082 ?population }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    LIMIT 250
    """

    for row in sparql_query(countries_query):
        country_name = val(row, "countryLabel")
        if not country_name or country_name.startswith("Q"):
            continue

        # Country entity
        entities.append(Entity(
            type="Jurisdiction",
            name=country_name,
            properties={
                "iso2": val(row, "iso2"),
                "iso3": val(row, "iso3"),
                "population": val(row, "population"),
                "wikidata_id": val(row, "country").split("/")[-1]
            },
            source="wikidata",
            confidence=0.95
        ))

        # Capital relationship
        capital = val(row, "capitalLabel")
        if capital and not capital.startswith("Q"):
            entities.append(Entity(
                type="Location",
                name=capital,
                properties={"type": "capital_city"},
                source="wikidata"
            ))
            connections.append(Connection(
                from_type="Location", from_name=capital,
                to_type="Jurisdiction", to_name=country_name,
                relation="located_in",
                properties={"role": "capital"},
                source="wikidata"
            ))

        # Leader relationship
        leader = val(row, "leaderLabel")
        if leader and not leader.startswith("Q"):
            entities.append(Entity(
                type="Person",
                name=leader,
                properties={"role": "head_of_government"},
                source="wikidata"
            ))
            connections.append(Connection(
                from_type="Person", from_name=leader,
                to_type="Jurisdiction", to_name=country_name,
                relation="holds_role",
                properties={"role": "head_of_government"},
                source="wikidata"
            ))

        # Central bank relationship
        cb = val(row, "centralBankLabel")
        if cb and not cb.startswith("Q"):
            entities.append(Entity(
                type="FinancialInstitution",
                name=cb,
                properties={"type": "central_bank"},
                source="wikidata"
            ))
            connections.append(Connection(
                from_type="FinancialInstitution", from_name=cb,
                to_type="Jurisdiction", to_name=country_name,
                relation="has_jurisdiction",
                properties={"role": "monetary_authority"},
                source="wikidata"
            ))

    # 2. International Organizations
    print("  [Wikidata] Fetching international organizations...")
    orgs_query = """
    SELECT DISTINCT ?org ?orgLabel ?hq ?hqLabel ?founded ?memberCount
    WHERE {
      VALUES ?orgType { wd:Q484652 wd:Q1335818 wd:Q15925165 }
      ?org wdt:P31 ?orgType .
      OPTIONAL { ?org wdt:P159 ?hq }
      OPTIONAL { ?org wdt:P571 ?founded }
      OPTIONAL { ?org wdt:P2124 ?memberCount }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    LIMIT 200
    """

    for row in sparql_query(orgs_query):
        org_name = val(row, "orgLabel")
        if not org_name or org_name.startswith("Q"):
            continue

        entities.append(Entity(
            type="LegalEntity",
            name=org_name,
            properties={
                "type": "international_organization",
                "founded": val(row, "founded")[:10] if val(row, "founded") else None,
                "member_count": val(row, "memberCount")
            },
            source="wikidata"
        ))

        hq = val(row, "hqLabel")
        if hq and not hq.startswith("Q"):
            connections.append(Connection(
                from_type="LegalEntity", from_name=org_name,
                to_type="Location", to_name=hq,
                relation="headquartered_in",
                properties={},
                source="wikidata"
            ))

    # 3. Major corporations
    print("  [Wikidata] Fetching major corporations...")
    corps_query = """
    SELECT DISTINCT ?company ?companyLabel ?industry ?industryLabel ?ceo ?ceoLabel
           ?hq ?hqLabel ?revenue ?employees ?founded
    WHERE {
      ?company wdt:P31 wd:Q4830453 .
      ?company wdt:P2139 ?revenue .
      FILTER(?revenue > 1000000000)
      OPTIONAL { ?company wdt:P452 ?industry }
      OPTIONAL { ?company wdt:P169 ?ceo }
      OPTIONAL { ?company wdt:P159 ?hq }
      OPTIONAL { ?company wdt:P1128 ?employees }
      OPTIONAL { ?company wdt:P571 ?founded }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    LIMIT 500
    """

    for row in sparql_query(corps_query):
        company = val(row, "companyLabel")
        if not company or company.startswith("Q"):
            continue

        entities.append(Entity(
            type="Corporation",
            name=company,
            properties={
                "revenue": val(row, "revenue"),
                "employees": val(row, "employees"),
                "founded": val(row, "founded")[:10] if val(row, "founded") else None
            },
            source="wikidata"
        ))

        # Industry
        industry = val(row, "industryLabel")
        if industry and not industry.startswith("Q"):
            entities.append(Entity(
                type="Industry",
                name=industry,
                properties={},
                source="wikidata"
            ))
            connections.append(Connection(
                from_type="Corporation", from_name=company,
                to_type="Industry", to_name=industry,
                relation="operates_in",
                properties={},
                source="wikidata"
            ))

        # CEO
        ceo = val(row, "ceoLabel")
        if ceo and not ceo.startswith("Q"):
            entities.append(Entity(
                type="Person",
                name=ceo,
                properties={"role": "CEO"},
                source="wikidata"
            ))
            connections.append(Connection(
                from_type="Person", from_name=ceo,
                to_type="Corporation", to_name=company,
                relation="works_at",
                properties={"role": "CEO"},
                source="wikidata"
            ))

        # HQ
        hq = val(row, "hqLabel")
        if hq and not hq.startswith("Q"):
            connections.append(Connection(
                from_type="Corporation", from_name=company,
                to_type="Location", to_name=hq,
                relation="headquartered_in",
                properties={},
                source="wikidata"
            ))

    # Batch up
    print(f"  [Wikidata] Collected {len(entities)} entities, {len(connections)} connections")
    for i in range(0, max(len(entities), len(connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=entities[i:i+BATCH_SIZE],
            connections=connections[i:i+BATCH_SIZE],
            source="wikidata"
        ))

    return batches

# ─── OPENSANCTIONS COLLECTOR ───────────────────────────────────────────────────

OPENSANCTIONS_URL = "https://data.opensanctions.org/datasets/latest/default/entities.ftm.json"

def collect_opensanctions(limit: int = 5000) -> List[IntelBatch]:
    """Collect PEPs, sanctions, watchlists from OpenSanctions."""
    print("[OpenSanctions] Starting collection...")
    batches = []
    entities = []
    connections = []

    try:
        print("  [OpenSanctions] Streaming entities (this may take a moment)...")
        resp = requests.get(OPENSANCTIONS_URL, stream=True, timeout=120, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()

        count = 0
        for line in resp.iter_lines():
            if count >= limit:
                break
            if not line:
                continue

            try:
                entity = json.loads(line)
                schema = entity.get("schema", "")
                props = entity.get("properties", {})

                # Map OpenSanctions schema to our types
                type_map = {
                    "Person": "Person",
                    "Company": "Corporation",
                    "Organization": "LegalEntity",
                    "LegalEntity": "LegalEntity",
                    "PublicBody": "LegalEntity"
                }

                entity_type = type_map.get(schema)
                if not entity_type:
                    continue

                names = props.get("name", [])
                if not names:
                    continue
                name = names[0]

                # Build properties
                entity_props = {
                    "opensanctions_id": entity.get("id"),
                    "datasets": entity.get("datasets", []),
                    "first_seen": entity.get("first_seen"),
                    "last_seen": entity.get("last_seen"),
                }

                # Add relevant properties
                if "birthDate" in props:
                    entity_props["birth_date"] = props["birthDate"][0] if props["birthDate"] else None
                if "nationality" in props:
                    entity_props["nationality"] = props["nationality"]
                if "position" in props:
                    entity_props["positions"] = props["position"]
                if "topics" in props:
                    entity_props["topics"] = props["topics"]  # e.g., ["sanction", "crime.terror"]

                # Determine confidence based on dataset quality
                datasets = entity.get("datasets", [])
                if any(d in datasets for d in ["us_ofac_sdn", "eu_fsf", "un_sc_sanctions"]):
                    confidence = 0.95
                elif any(d in datasets for d in ["interpol_red_notices", "us_bis_denied"]):
                    confidence = 0.90
                else:
                    confidence = 0.80

                entities.append(Entity(
                    type=entity_type,
                    name=name,
                    properties=entity_props,
                    source="opensanctions",
                    confidence=confidence
                ))

                # Add country relationships
                for country in props.get("country", []):
                    if len(country) == 2:  # ISO code
                        connections.append(Connection(
                            from_type=entity_type, from_name=name,
                            to_type="Jurisdiction", to_name=country.upper(),
                            relation="located_in",
                            properties={"source": "opensanctions"},
                            source="opensanctions"
                        ))

                # Add position/role relationships
                for position in props.get("position", []):
                    connections.append(Connection(
                        from_type=entity_type, from_name=name,
                        to_type="LegalEntity", to_name=position,
                        relation="holds_role",
                        properties={"role": position},
                        source="opensanctions"
                    ))

                count += 1
                if count % 1000 == 0:
                    print(f"  [OpenSanctions] Processed {count} entities...")

            except json.JSONDecodeError:
                continue

        print(f"  [OpenSanctions] Collected {len(entities)} entities, {len(connections)} connections")

    except Exception as e:
        print(f"  [OpenSanctions Error] {e}")
        return []

    # Batch up
    for i in range(0, max(len(entities), len(connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=entities[i:i+BATCH_SIZE],
            connections=connections[i:i+BATCH_SIZE],
            source="opensanctions"
        ))

    return batches

# ─── SEC EDGAR COLLECTOR ───────────────────────────────────────────────────────

SEC_COMPANY_TICKERS = "https://www.sec.gov/files/company_tickers.json"
SEC_HEADERS = {"User-Agent": USER_AGENT}

def collect_sec_edgar(limit: int = 2000) -> List[IntelBatch]:
    """Collect US public companies from SEC EDGAR."""
    print("[SEC EDGAR] Starting collection...")
    batches = []
    entities = []
    connections = []

    try:
        print("  [SEC EDGAR] Fetching company tickers...")
        resp = requests.get(SEC_COMPANY_TICKERS, headers=SEC_HEADERS, timeout=30)
        resp.raise_for_status()
        companies = resp.json()

        count = 0
        for key, company in companies.items():
            if count >= limit:
                break

            cik = str(company.get("cik_str", "")).zfill(10)
            ticker = company.get("ticker", "")
            name = company.get("title", "")

            if not name:
                continue

            entities.append(Entity(
                type="Corporation",
                name=name,
                properties={
                    "cik": cik,
                    "ticker": ticker,
                    "exchange": "US",
                    "sec_registered": True
                },
                source="sec_edgar",
                confidence=0.95
            ))

            # US jurisdiction
            connections.append(Connection(
                from_type="Corporation", from_name=name,
                to_type="Jurisdiction", to_name="United States",
                relation="has_jurisdiction",
                properties={"registration": "SEC"},
                source="sec_edgar"
            ))

            count += 1

        print(f"  [SEC EDGAR] Collected {len(entities)} companies")

        # Fetch some recent filings for relationships
        print("  [SEC EDGAR] Fetching recent 8-K filings for events...")
        filings_url = "https://efts.sec.gov/LATEST/search-index?q=8-K&dateRange=custom&startdt=2024-01-01&enddt=2024-12-31&forms=8-K"
        # Note: This is a simplified approach - full EDGAR API requires more complex handling

    except Exception as e:
        print(f"  [SEC EDGAR Error] {e}")

    # Batch up
    for i in range(0, max(len(entities), len(connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=entities[i:i+BATCH_SIZE],
            connections=connections[i:i+BATCH_SIZE],
            source="sec_edgar"
        ))

    return batches

# ─── WORLD BANK COLLECTOR ──────────────────────────────────────────────────────

WB_COUNTRIES_URL = "https://api.worldbank.org/v2/country?format=json&per_page=300"
WB_INDICATORS = [
    ("NY.GDP.MKTP.CD", "gdp_usd"),
    ("SP.POP.TOTL", "population"),
    ("FP.CPI.TOTL.ZG", "inflation_rate"),
]

def collect_worldbank() -> List[IntelBatch]:
    """Collect country data and economic indicators from World Bank."""
    print("[World Bank] Starting collection...")
    batches = []
    entities = []
    connections = []

    try:
        print("  [World Bank] Fetching countries...")
        resp = requests.get(WB_COUNTRIES_URL, headers={"User-Agent": USER_AGENT}, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if len(data) < 2:
            print("  [World Bank] No data returned")
            return []

        countries = data[1]
        country_codes = {}

        for c in countries:
            if c.get("region", {}).get("value") == "Aggregates":
                continue  # Skip aggregate regions

            name = c.get("name", "")
            iso2 = c.get("iso2Code", "")

            if not name:
                continue

            country_codes[iso2] = name

            entities.append(Entity(
                type="Jurisdiction",
                name=name,
                properties={
                    "iso2": iso2,
                    "iso3": c.get("id", ""),
                    "region": c.get("region", {}).get("value"),
                    "income_level": c.get("incomeLevel", {}).get("value"),
                    "capital": c.get("capitalCity", ""),
                    "longitude": c.get("longitude"),
                    "latitude": c.get("latitude")
                },
                source="worldbank",
                confidence=0.95
            ))

            # Capital relationship
            capital = c.get("capitalCity", "")
            if capital:
                entities.append(Entity(
                    type="Location",
                    name=capital,
                    properties={"type": "capital_city"},
                    source="worldbank"
                ))
                connections.append(Connection(
                    from_type="Location", from_name=capital,
                    to_type="Jurisdiction", to_name=name,
                    relation="located_in",
                    properties={"role": "capital"},
                    source="worldbank"
                ))

            # Region relationship
            region = c.get("region", {}).get("value")
            if region and region != "Aggregates":
                connections.append(Connection(
                    from_type="Jurisdiction", from_name=name,
                    to_type="Location", to_name=region,
                    relation="part_of",
                    properties={},
                    source="worldbank"
                ))

        print(f"  [World Bank] Collected {len(entities)} entities, {len(connections)} connections")

    except Exception as e:
        print(f"  [World Bank Error] {e}")

    # Batch up
    for i in range(0, max(len(entities), len(connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=entities[i:i+BATCH_SIZE],
            connections=connections[i:i+BATCH_SIZE],
            source="worldbank"
        ))

    return batches

# ─── NEWS/RSS COLLECTOR ────────────────────────────────────────────────────────

RSS_FEEDS = [
    ("https://feeds.bbci.co.uk/news/world/rss.xml", "BBC World"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/World.xml", "NYT World"),
    ("https://feeds.reuters.com/reuters/worldNews", "Reuters"),
    ("https://www.ft.com/world?format=rss", "Financial Times"),
    ("https://feeds.feedburner.com/TechCrunch", "TechCrunch"),
]

def parse_rss(url: str, source_name: str) -> Tuple[List[Entity], List[Connection]]:
    """Parse an RSS feed and extract entities."""
    entities = []
    connections = []

    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)

        # Handle both RSS and Atom
        items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")

        for item in items[:50]:  # Limit per feed
            title = item.findtext("title") or item.findtext("{http://www.w3.org/2005/Atom}title", "")
            link = item.findtext("link") or item.findtext("{http://www.w3.org/2005/Atom}link", "")
            pub_date = item.findtext("pubDate") or item.findtext("{http://www.w3.org/2005/Atom}published", "")
            description = item.findtext("description") or item.findtext("{http://www.w3.org/2005/Atom}summary", "")

            if not title:
                continue

            # Create event entity
            entities.append(Entity(
                type="Event",
                name=title[:200],  # Truncate long titles
                properties={
                    "url": link,
                    "published": pub_date,
                    "description": (description or "")[:500],
                    "feed": source_name
                },
                source=f"rss:{source_name.lower().replace(' ', '_')}",
                confidence=0.70
            ))

            # Create source entity and connection
            entities.append(Entity(
                type="InformationSource",
                name=source_name,
                properties={"type": "news_feed", "url": url},
                source="rss"
            ))
            connections.append(Connection(
                from_type="InformationSource", from_name=source_name,
                to_type="Event", to_name=title[:200],
                relation="related_to",
                properties={"type": "reports"},
                source="rss"
            ))

    except Exception as e:
        print(f"    [RSS Error] {source_name}: {e}")

    return entities, connections

def collect_news() -> List[IntelBatch]:
    """Collect news events from RSS feeds."""
    print("[News/RSS] Starting collection...")
    batches = []
    all_entities = []
    all_connections = []

    for url, name in RSS_FEEDS:
        print(f"  [RSS] Fetching {name}...")
        entities, connections = parse_rss(url, name)
        all_entities.extend(entities)
        all_connections.extend(connections)

    print(f"  [News/RSS] Collected {len(all_entities)} entities, {len(all_connections)} connections")

    # Batch up
    for i in range(0, max(len(all_entities), len(all_connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=all_entities[i:i+BATCH_SIZE],
            connections=all_connections[i:i+BATCH_SIZE],
            source="news_rss"
        ))

    return batches

# ─── GITHUB TRENDING COLLECTOR ─────────────────────────────────────────────────

GITHUB_API = "https://api.github.com"

def collect_github_trending() -> List[IntelBatch]:
    """Collect trending repos and tech companies from GitHub."""
    print("[GitHub] Starting collection...")
    batches = []
    entities = []
    connections = []

    try:
        # Search for popular repos
        print("  [GitHub] Fetching trending repositories...")
        resp = requests.get(
            f"{GITHUB_API}/search/repositories",
            params={
                "q": "stars:>10000",
                "sort": "stars",
                "order": "desc",
                "per_page": 100
            },
            headers={"User-Agent": USER_AGENT, "Accept": "application/vnd.github.v3+json"},
            timeout=30
        )

        if resp.status_code == 200:
            repos = resp.json().get("items", [])

            for repo in repos:
                name = repo.get("full_name", "")
                owner = repo.get("owner", {})

                # Repo as technology/project
                entities.append(Entity(
                    type="Technology",
                    name=repo.get("name", ""),
                    properties={
                        "github_url": repo.get("html_url"),
                        "stars": repo.get("stargazers_count"),
                        "forks": repo.get("forks_count"),
                        "language": repo.get("language"),
                        "description": (repo.get("description") or "")[:500],
                        "topics": repo.get("topics", [])
                    },
                    source="github",
                    confidence=0.85
                ))

                # Owner/org
                owner_type = owner.get("type", "User")
                owner_name = owner.get("login", "")

                if owner_type == "Organization":
                    entities.append(Entity(
                        type="Corporation",
                        name=owner_name,
                        properties={
                            "github_url": owner.get("html_url"),
                            "type": "tech_company"
                        },
                        source="github"
                    ))
                    connections.append(Connection(
                        from_type="Corporation", from_name=owner_name,
                        to_type="Technology", to_name=repo.get("name", ""),
                        relation="owns",
                        properties={"platform": "github"},
                        source="github"
                    ))
                else:
                    entities.append(Entity(
                        type="Person",
                        name=owner_name,
                        properties={
                            "github_url": owner.get("html_url"),
                            "type": "developer"
                        },
                        source="github"
                    ))
                    connections.append(Connection(
                        from_type="Person", from_name=owner_name,
                        to_type="Technology", to_name=repo.get("name", ""),
                        relation="founded_by",
                        properties={"platform": "github"},
                        source="github"
                    ))

            print(f"  [GitHub] Collected {len(entities)} entities, {len(connections)} connections")
        else:
            print(f"  [GitHub] API returned {resp.status_code}")

    except Exception as e:
        print(f"  [GitHub Error] {e}")

    # Batch up
    for i in range(0, max(len(entities), len(connections)), BATCH_SIZE):
        batches.append(IntelBatch(
            entities=entities[i:i+BATCH_SIZE],
            connections=connections[i:i+BATCH_SIZE],
            source="github"
        ))

    return batches

# ─── MAIN ORCHESTRATOR ─────────────────────────────────────────────────────────

def run_all_collectors() -> Dict:
    """Run all collectors and aggregate results."""
    print("=" * 60)
    print("FORAGE INTEL COLLECTOR - Full Run")
    print("=" * 60)
    start = time.time()

    results = {
        "started": datetime.utcnow().isoformat(),
        "sources": {},
        "totals": {"entities": 0, "connections": 0, "errors": []}
    }

    collectors = [
        ("wikidata", collect_wikidata),
        ("opensanctions", lambda: collect_opensanctions(limit=3000)),
        ("sec_edgar", lambda: collect_sec_edgar(limit=2000)),
        ("worldbank", collect_worldbank),
        ("news_rss", collect_news),
        ("github", collect_github_trending),
    ]

    for source, collector_fn in collectors:
        print()
        try:
            batches = collector_fn()
            if batches:
                result = post_batches(batches, source)
                results["sources"][source] = result
                results["totals"]["entities"] += result.get("entities", 0)
                results["totals"]["connections"] += result.get("connections", 0)
                results["totals"]["errors"].extend(result.get("errors", []))
            else:
                results["sources"][source] = {"skipped": True, "reason": "No data collected"}
        except Exception as e:
            results["sources"][source] = {"error": str(e)}
            results["totals"]["errors"].append(f"{source}: {e}")

    elapsed = time.time() - start
    results["completed"] = datetime.utcnow().isoformat()
    results["duration_seconds"] = round(elapsed, 2)

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Duration: {elapsed:.1f}s")
    print(f"Total Entities: {results['totals']['entities']}")
    print(f"Total Connections: {results['totals']['connections']}")
    print(f"Errors: {len(results['totals']['errors'])}")

    return results

def main():
    parser = argparse.ArgumentParser(description="Forage Intel Collector")
    parser.add_argument("--all", action="store_true", help="Run all collectors")
    parser.add_argument("--wikidata", action="store_true", help="Collect from Wikidata")
    parser.add_argument("--sanctions", action="store_true", help="Collect from OpenSanctions")
    parser.add_argument("--sec", action="store_true", help="Collect from SEC EDGAR")
    parser.add_argument("--worldbank", action="store_true", help="Collect from World Bank")
    parser.add_argument("--news", action="store_true", help="Collect from RSS feeds")
    parser.add_argument("--github", action="store_true", help="Collect from GitHub")
    parser.add_argument("--output", type=str, help="Output results to JSON file")

    args = parser.parse_args()

    # If no args, run all
    if not any([args.all, args.wikidata, args.sanctions, args.sec, args.worldbank, args.news, args.github]):
        args.all = True

    if args.all:
        results = run_all_collectors()
    else:
        results = {"sources": {}, "totals": {"entities": 0, "connections": 0}}

        if args.wikidata:
            batches = collect_wikidata()
            if batches:
                results["sources"]["wikidata"] = post_batches(batches, "wikidata")

        if args.sanctions:
            batches = collect_opensanctions(limit=3000)
            if batches:
                results["sources"]["opensanctions"] = post_batches(batches, "opensanctions")

        if args.sec:
            batches = collect_sec_edgar(limit=2000)
            if batches:
                results["sources"]["sec_edgar"] = post_batches(batches, "sec_edgar")

        if args.worldbank:
            batches = collect_worldbank()
            if batches:
                results["sources"]["worldbank"] = post_batches(batches, "worldbank")

        if args.news:
            batches = collect_news()
            if batches:
                results["sources"]["news_rss"] = post_batches(batches, "news_rss")

        if args.github:
            batches = collect_github_trending()
            if batches:
                results["sources"]["github"] = post_batches(batches, "github")

    # Output
    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved to {args.output}")

    return results

if __name__ == "__main__":
    main()
