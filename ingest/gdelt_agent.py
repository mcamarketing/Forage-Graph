#!/usr/bin/env python3
"""
gdelt_agent.py

GDELT polling agent.
Polls GDELT GKG endpoint every 5 minutes for new events.
Writes cross-border sentiment signals to Forage graph.

Requirements:
  httpx
  python-dotenv

Env vars:
  APIFY_TOKEN      - Forage API token
  FORAGE_ENDPOINT  - Default: https://ernesta-labs--forage.apify.actor
  GDELT_INTERVAL   - Polling interval in seconds (default: 300)
  GDELT_THEME      - Optional: filter by theme (e.g., "TAX_FNC")
  GDELT_COUNTRY    - Optional: filter by country (e.g., "Russia")
"""

import os
import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Optional

import httpx
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────

APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")
FORAGE_ENDPOINT = os.environ.get(
    "FORAGE_ENDPOINT", "https://ernesta-labs--forage.apify.actor"
)
GDELT_INTERVAL = int(os.environ.get("GDELT_INTERVAL", "300"))  # 5 minutes
GDELT_THEME = os.environ.get("GDELT_THEME", "")
GDELT_COUNTRY = os.environ.get("GDELT_COUNTRY", "")

# ─── FORAGE CLIENT ───────────────────────────────────────────────────────────


class ForageClient:
    """Simple Forage MCP client."""

    def __init__(self, token: str, endpoint: str):
        self.token = token
        self.endpoint = endpoint

    async def call_tool(self, tool_name: str, params: dict) -> Optional[dict]:
        """Call a Forage MCP tool."""
        if not self.token:
            return None

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    f"{self.endpoint}/mcp",
                    headers={
                        "Authorization": f"Bearer {self.token}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "jsonrpc": "2.0",
                        "id": f"gdelt-{int(time.time())}",
                        "method": "tools/call",
                        "params": {"name": tool_name, "arguments": params},
                    },
                )

                if response.status_code == 200:
                    data = response.json()
                    text = data.get("result", {}).get("content", [{}])[0].get("text")
                    return json.loads(text) if text else None
        except Exception as e:
            print(f"[FORAGE] Error calling {tool_name}: {e}")

        return None

    async def add_signal(self, entity: str, metric: str, value: float):
        """Add signal to graph."""
        return await self.call_tool(
            "add_signal",
            {
                "entity": entity,
                "metric": metric,
                "value": value,
                "timestamp": int(time.time() * 1000),
            },
        )

    async def add_claim(
        self,
        entity: str,
        relation: str,
        target: str,
        assertion: str,
        source: str,
        confidence: float,
    ):
        """Add claim to graph."""
        return await self.call_tool(
            "add_claim",
            {
                "entity": entity,
                "relation": relation,
                "target": target,
                "assertion": assertion,
                "source_url": source,
                "confidence": confidence,
            },
        )


# ─── GDELT CLIENT ───────────────────────────────────────────────────────────


class GDELTClient:
    """GDELT GKG API client."""

    BASE_URL = "https://api.gdeltproject.org/api/v2/gkggraph/gkggraph"

    async def fetch_recent(
        self, minutes: int = 15, theme: str = "", country: str = ""
    ) -> list:
        """Fetch recent GKG records."""
        # Calculate time range
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=minutes)

        # Build URL
        params = {
            "format": "json",
            "mode": "artlist",
            "maxrecs": "1000",
            "sort": "DateDesc",
        }

        # Add filters
        if theme:
            params["theme"] = theme
        if country:
            params["countrycode"] = country

        # Add date filter
        params["startdatetime"] = start_time.strftime("%Y%m%dT%H%M%S")
        params["enddatetime"] = end_time.strftime("%Y%m%dT%H%M%S")

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(self.BASE_URL, params=params)

                if response.status_code == 200:
                    lines = response.text.strip().split("\n")
                    records = []
                    for line in lines:
                        if line.strip():
                            try:
                                records.append(json.loads(line))
                            except:
                                continue
                    return records
        except Exception as e:
            print(f"[GDELT] Fetch error: {e}")

        return []


# ─── MAIN LOOP ───────────────────────────────────────────────────────────────


async def main():
    print(f"[GDELT-AGENT] Starting with {GDELT_INTERVAL}s interval")
    if GDELT_THEME:
        print(f"[GDELT-AGENT] Theme filter: {GDELT_THEME}")
    if GDELT_COUNTRY:
        print(f"[GDELT-AGENT] Country filter: {GDELT_COUNTRY}")

    forage = ForageClient(APIFY_TOKEN, FORAGE_ENDPOINT)
    gdelt = GDELTClient()

    # Rate limiting
    last_request_time = 0
    min_interval = 1.0  # 1 second between requests

    while True:
        try:
            # Rate limit
            now = time.time()
            if now - last_request_time < min_interval:
                await asyncio.sleep(min_interval - (now - last_request_time))
            last_request_time = time.time()

            # Fetch recent GKG records
            records = await gdelt.fetch_recent(
                minutes=15, theme=GDELT_THEME, country=GDELT_COUNTRY
            )

            print(f"[GDELT-AGENT] Fetched {len(records)} records")

            # Process each record
            events_ingested = 0
            signals_written = 0

            for record in records:
                # Extract actors and themes
                # GDELT format: fields separated by tabs
                # See: https://blog.gdeltproject.org/the-global-knowledge-graph-building-a-free-open-database-of-global-human-society/

                try:
                    # Parse record
                    # Common fields: se, url, title, domain, translations, themes, persons, orgs, locations
                    themes = (
                        record.get("themes", []).split(",")
                        if record.get("themes")
                        else []
                    )
                    persons = (
                        record.get("persons", "").split(",")
                        if record.get("persons")
                        else []
                    )
                    orgs = (
                        record.get("orgs", "").split(",") if record.get("orgs") else []
                    )

                    # Extract tone (sentiment)
                    tone = float(record.get("tone", 0))

                    # Process actors
                    all_actors = [p.strip() for p in persons if p.strip()] + [
                        o.strip() for o in orgs if o.strip()
                    ]

                    for actor in all_actors[:5]:  # Limit to 5 actors per record
                        if len(actor) < 2:
                            continue

                        # Write sentiment signal
                        # Normalize tone from GDELT (-10 to 10) to (-1 to 1)
                        normalized_tone = max(-1, min(1, tone / 10))

                        await forage.add_signal(
                            actor, "cross_border_sentiment", normalized_tone
                        )
                        signals_written += 1

                        # Write claim for evidence
                        await forage.add_claim(
                            actor,
                            "GDELT_EVENT",
                            record.get("url", "unknown"),
                            json.dumps(
                                {
                                    "title": record.get("title", ""),
                                    "tone": tone,
                                    "themes": themes[:3],
                                }
                            ),
                            "gdelt",
                            0.7,
                        )

                        events_ingested += 1

                except Exception as e:
                    print(f"[GDELT] Record processing error: {e}")
                    continue

            print(
                f"[GDELT-AGENT] Wrote {signals_written} signals, {events_ingested} claims"
            )

        except Exception as e:
            print(f"[GDELT-AGENT] Error: {e}")
            # Exponential backoff on failure (M5 fix)
            backoff = min(120, GDELT_INTERVAL * 2)
            print(f"[GDELT-AGENT] Backing off for {backoff}s")
            await asyncio.sleep(backoff)
            continue

        # Wait for next interval
        await asyncio.sleep(GDELT_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
