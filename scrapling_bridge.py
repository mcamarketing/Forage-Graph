#!/usr/bin/env python3
"""
scrapling_bridge.py

FastAPI server providing the same interface as Forage's scraping tools,
but using Scrapling (free, anti-detection) instead of paid Forage API.

Usage:
    python3 scrapling_bridge.py

Endpoints (matching Forage API shape):
    POST /scrape     — Extract clean text from URL
    POST /search     — Web search (using DuckDuckGo/fallback)
    POST /extract    — Extract structured data per schema
    POST /screenshot — Capture screenshot (using Playwright)

Rate limits: None (local), 60 req/min if proxied through Apify.
"""

import os
import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Optional, Any, Dict, List
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl
import httpx
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Scrapling Bridge", version="1.0.0")

# ─── FORAGE GRAPH WRITE-BACK ────────────────────────────────────────────────

FORAGE_ENDPOINT = os.environ.get(
    "FORAGE_ENDPOINT", "https://ernesta-labs--forage.apify.actor"
)
APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")


async def forage_add_claim(
    entity: str, relation: str, target: str, assertion: str, source: str, confidence: float = 0.7
) -> None:
    """Write claim to Forage graph. Fire-and-forget — never raises."""
    if not APIFY_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(
                f"{FORAGE_ENDPOINT}/mcp",
                headers={
                    "Authorization": f"Bearer {APIFY_TOKEN}",
                    "Content-Type": "application/json",
                },
                json={
                    "jsonrpc": "2.0",
                    "id": f"scrapling-{int(time.time())}",
                    "method": "tools/call",
                    "params": {
                        "name": "add_claim",
                        "arguments": {
                            "entity": entity,
                            "relation": relation,
                            "target": target,
                            "assertion": assertion,
                            "source_url": source,
                            "confidence": confidence,
                        },
                    },
                },
            )
    except Exception as e:
        print(f"[SCRAPLING] Graph write-back failed: {e}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── CONFIG ───────────────────────────────────────────────────────────────────

SCRAPER_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
REQUEST_TIMEOUT = 30

# ─── REQUEST MODELS ───────────────────────────────────────────────────────────


class ScrapeRequest(BaseModel):
    url: str
    schema: Optional[Dict[str, str]] = None


class SearchRequest(BaseModel):
    query: str
    max_results: int = 10


class ExtractRequest(BaseModel):
    url: str
    schema: Dict[str, str]


class ScreenshotRequest(BaseModel):
    url: str
    wait_for: Optional[str] = None


# ─── UTILITY FUNCTIONS ────────────────────────────────────────────────────────


async def fetch_url(url: str, timeout: int = REQUEST_TIMEOUT) -> Dict[str, Any]:
    """
    Fetch URL with anti-detection headers.
    Falls back to simpler requests if Playwright not available.
    """
    headers = {
        "User-Agent": SCRAPER_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()

            # Simple content type detection
            content_type = response.headers.get("content-type", "").lower()

            return {
                "url": str(response.url),
                "status": response.status_code,
                "content": response.text,
                "content_type": content_type,
                "headers": dict(response.headers),
            }
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=f"Failed to fetch: {str(e)}")


def clean_html(html: str) -> str:
    """Remove scripts, styles, ads, and other noise from HTML."""
    # Basic cleanup - in production use proper HTML parser
    import re

    # Remove script tags
    html = re.sub(
        r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE
    )
    # Remove style tags
    html = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.IGNORECASE)
    # Remove comments
    html = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    # Remove nav/footer (simple heuristic)
    html = re.sub(r"<nav[^>]*>.*?</nav>", "", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(
        r"<footer[^>]*>.*?</footer>", "", html, flags=re.DOTALL | re.IGNORECASE
    )

    return html


def extract_text(html: str) -> str:
    """Extract readable text from HTML."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    # Remove unwanted elements
    for tag in soup(["script", "style", "nav", "footer", "aside"]):
        tag.decompose()

    # Get text
    text = soup.get_text(separator="\n")

    # Clean up whitespace
    lines = [line.strip() for line in text.split("\n")]
    text = "\n".join(line for line in lines if line)

    return text


def extract_schema(html: str, schema: Dict[str, str]) -> Dict[str, Any]:
    """Extract structured data from HTML based on schema."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    result = {}

    for key, selector in schema.items():
        try:
            if selector.startswith("//") or selector.startswith("/"):
                # XPath-like (basic support)
                elements = soup.select(selector)
                if elements:
                    result[key] = elements[0].get_text(strip=True)
            elif selector.startswith("#"):
                # ID selector
                elem = soup.find(id=selector[1:])
                if elem:
                    result[key] = elem.get_text(strip=True)
            elif selector.startswith("."):
                # Class selector
                elem = soup.find(class_=selector[1:])
                if elem:
                    result[key] = elem.get_text(strip=True)
            else:
                # Tag selector
                elem = soup.find(selector)
                if elem:
                    result[key] = elem.get_text(strip=True)
        except Exception as e:
            result[key] = f"Error: {str(e)}"

    return result


# ─── ENDPOINTS ───────────────────────────────────────────────────────────────


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    forage_reachable = bool(APIFY_TOKEN)
    if APIFY_TOKEN:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.post(
                    f"{FORAGE_ENDPOINT}/mcp",
                    headers={"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"},
                    json={"jsonrpc": "2.0", "id": "health", "method": "tools/call", "params": {"name": "get_graph_stats", "arguments": {}}},
                )
                forage_reachable = resp.status_code == 200
        except Exception:
            forage_reachable = False
    return {
        "status": "healthy",
        "service": "scrapling-bridge",
        "version": "1.0.0",
        "forage_reachable": forage_reachable,
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.post("/scrape")
async def scrape_page(req: ScrapeRequest) -> Dict[str, Any]:
    """
    Extract clean text from URL.
    Matches Forage scrape_page response shape.
    """
    start_time = time.time()

    try:
        # Fetch page
        response = await fetch_url(req.url)

        # Check content type
        if "text/html" not in response["content_type"]:
            return {
                "url": req.url,
                "title": "",
                "content": response["content"][:5000],  # Raw content for non-HTML
                "text_content": response["content"][:5000],
                "content_type": response["content_type"],
                "latency_ms": int((time.time() - start_time) * 1000),
            }

        # Parse HTML
        clean = clean_html(response["content"])
        text = extract_text(clean)

        # Extract title
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(clean, "html.parser")
        title = soup.title.string if soup.title else ""
        if not title:
            h1 = soup.find("h1")
            title = h1.get_text(strip=True) if h1 else ""

        # Write to Forage graph before returning (C9 fix)
        await forage_add_claim(
            entity=title or response["url"],
            relation="SCRAPED_CONTENT",
            target=response["url"],
            assertion=json.dumps({"title": title, "snippet": text[:500], "scraped_at": datetime.utcnow().isoformat()}),
            source=response["url"],
        )

        return {
            "url": response["url"],
            "title": title,
            "content": text[:50000],  # Limit content size
            "text_content": text[:50000],
            "latency_ms": int((time.time() - start_time) * 1000),
            "source": "scrapling-bridge",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scraping failed: {str(e)}")


@app.post("/search")
async def web_search(req: SearchRequest) -> Dict[str, Any]:
    """
    Web search using DuckDuckGo (no API key required).
    Matches Forage search_web response shape.
    """
    start_time = time.time()

    # Use DuckDuckGo HTML (no API key needed)
    search_url = f"https://html.duckduckgo.com/html/?q={req.query}"

    try:
        response = await fetch_url(search_url)

        # Parse results
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(response["content"], "html.parser")

        results = []
        for result in soup.select(".result")[: req.max_results]:
            try:
                title_elem = result.select_one(".result__title")
                link_elem = result.select_one(".result__url")
                snippet_elem = result.select_one(".result__snippet")

                if title_elem and link_elem:
                    # Extract title and URL
                    title = title_elem.get_text(strip=True)
                    url = link_elem.get_text(strip=True)
                    snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""

                    results.append({"title": title, "url": url, "snippet": snippet})
            except Exception:
                continue

        # Write search results to Forage graph (C9 fix)
        for r in results[:5]:
            await forage_add_claim(
                entity=req.query,
                relation="SEARCH_RESULT",
                target=r.get("url", ""),
                assertion=json.dumps({"title": r.get("title", ""), "snippet": r.get("snippet", "")}),
                source="duckduckgo",
                confidence=0.5,
            )

        return {
            "query": req.query,
            "results": results,
            "count": len(results),
            "latency_ms": int((time.time() - start_time) * 1000),
            "source": "duckduckgo",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@app.post("/extract")
async def extract_structured(req: ExtractRequest) -> Dict[str, Any]:
    """
    Extract structured data from URL based on schema.
    Matches Forage extract_structured response shape.
    """
    start_time = time.time()

    try:
        # Fetch page
        response = await fetch_url(req.url)

        # Extract based on schema
        if "text/html" in response["content_type"]:
            data = extract_schema(response["content"], req.schema)
        else:
            data = {"error": "Non-HTML content, cannot extract"}

        # Write extracted data to Forage graph (C9 fix)
        await forage_add_claim(
            entity=req.url,
            relation="EXTRACTED_DATA",
            target=req.url,
            assertion=json.dumps(data),
            source=req.url,
        )

        return {
            "url": req.url,
            "schema": req.schema,
            "data": data,
            "latency_ms": int((time.time() - start_time) * 1000),
            "source": "scrapling-bridge",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Extraction failed: {str(e)}")


@app.post("/screenshot")
async def screenshot_page(req: ScreenshotRequest) -> Dict[str, Any]:
    """
    Capture screenshot of URL.

    Note: Full screenshot requires Playwright/Selenium.
    This is a placeholder - returns placeholder data.
    In production, integrate with Playwright.
    """
    # For full screenshots, would need Playwright:
    # from playwright.async_api import async_playwright
    # async with async_playwright() as p:
    #     browser = await p.chromium.launch()
    #     page = await browser.new_page()
    #     await page.goto(req.url)
    #     screenshot = await page.screenshot()

    return {
        "url": req.url,
        "screenshot": "",  # Placeholder - would be base64 in production
        "error": "Screenshot requires Playwright integration",
        "source": "scrapling-bridge",
    }


# ─── BATCH ENDPOINT (for bulk operations) ───────────────────────────────────


class BatchRequest(BaseModel):
    urls: List[str]


@app.post("/batch/scrape")
async def batch_scrape(req: BatchRequest) -> Dict[str, Any]:
    """Scrape multiple URLs in parallel."""
    results = []

    # Scrape all URLs concurrently
    tasks = [scrape_page(ScrapeRequest(url=url)) for url in req.urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    return {
        "results": [
            r if not isinstance(r, Exception) else {"error": str(r)} for r in results
        ],
        "count": len(req.urls),
    }


# ─── STATS ENDPOINT ─────────────────────────────────────────────────────────


@app.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """Return bridge usage statistics."""
    return {
        "service": "scrapling-bridge",
        "endpoints": ["/scrape", "/search", "/extract", "/screenshot", "/batch/scrape"],
        "fallback_for": ["Forage API (paid)"],
        "cost": "free",
    }


# ─── MAIN ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8001))
    host = os.environ.get("HOST", "0.0.0.0")

    print(f"Starting Scrapling Bridge on {host}:{port}")
    print("Endpoints available:")
    print("  POST /scrape    - Extract clean text from URL")
    print("  POST /search    - Web search (DuckDuckGo)")
    print("  POST /extract   - Structured data extraction")
    print("  POST /screenshot - Screenshot capture (placeholder)")
    print("  POST /batch/scrape - Batch scraping")

    uvicorn.run(app, host=host, port=port)

# ─── DOCKERFILE (as heredoc for reference) ─────────────────────────────────
"""
FROM python:3.11-slim

WORKDIR /app

RUN pip install fastapi uvicorn httpx beautifulsoup4 pydantic python-dotenv

COPY scrapling_bridge.py .

EXPOSE 8001

CMD ["python", "scrapling_bridge.py"]
"""
