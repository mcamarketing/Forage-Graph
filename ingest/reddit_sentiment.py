#!/usr/bin/env python3
"""
reddit_sentiment.py

Reddit sentiment tracking agent.
Polls configured subreddits every 5 minutes for sentiment analysis.
Writes narrative velocity signals to Forage graph.

Requirements:
  httpx
  python-dotenv
  vaderSentiment
  langdetect

Env vars:
  APIFY_TOKEN      - Forage API token
  FORAGE_ENDPOINT  - Default: https://ernesta-labs--forage.apify.actor
  REDDIT_SUBREDDITS - Comma-separated list (e.g., "russia,ukraine,tatarstan")
  REDDIT_LIMIT     - Posts per subreddit (default: 25)
  POLL_INTERVAL    - Polling interval in seconds (default: 300)
"""

import os
import asyncio
import json
import time
from datetime import datetime
from typing import List, Optional

import httpx
from dotenv import load_dotenv

# VADER for sentiment, langdetect for language
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    from langdetect import detect, LangDetectException

    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False
    print(
        "[REDDIT] Warning: vaderSentiment/langdetect not installed, using basic sentiment"
    )

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────

APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")
FORAGE_ENDPOINT = os.environ.get(
    "FORAGE_ENDPOINT", "https://ernesta-labs--forage.apify.actor"
)
REDDIT_SUBREDDITS = os.environ.get("REDDIT_SUBREDDITS", "russia,ukraine").split(",")
REDDIT_LIMIT = int(os.environ.get("REDDIT_LIMIT", "25"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "300"))  # 5 minutes

# ─── FORAGE CLIENT ───────────────────────────────────────────────────────────


class ForageClient:
    """Simple Forage MCP client."""

    def __init__(self, token: str, endpoint: str):
        self.token = token
        self.endpoint = endpoint

    async def call_tool(self, tool_name: str, params: dict) -> Optional[dict]:
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
                        "id": f"reddit-{int(time.time())}",
                        "method": "tools/call",
                        "params": {"name": tool_name, "arguments": params},
                    },
                )

                if response.status_code == 200:
                    data = response.json()
                    text = data.get("result", {}).get("content", [{}])[0].get("text")
                    return json.loads(text) if text else None
        except Exception as e:
            print(f"[FORAGE] Error: {e}")

        return None

    async def add_signal(self, entity: str, metric: str, value: float):
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


# ─── REDDIT CLIENT ───────────────────────────────────────────────────────────


class RedditClient:
    """Reddit JSON API client."""

    BASE_URL = "https://www.reddit.com"

    async def get_subreddit_posts(self, subreddit: str, limit: int = 25) -> List[dict]:
        """Get posts from a subreddit."""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                # Use .json endpoint
                response = await client.get(
                    f"{self.BASE_URL}/r/{subreddit}/hot.json",
                    params={"limit": min(limit, 100), "raw_json": 1},
                    headers={"User-Agent": "PerceptionGraph/1.0"},
                )

                if response.status_code == 200:
                    data = response.json()
                    children = data.get("data", {}).get("children", [])

                    posts = []
                    for item in children:
                        post = item.get("data", {})
                        posts.append(
                            {
                                "subreddit": subreddit,
                                "title": post.get("title", ""),
                                "selftext": post.get("selftext", ""),
                                "score": post.get("score", 0),
                                "num_comments": post.get("num_comments", 0),
                                "url": post.get("url", ""),
                                "id": post.get("id", ""),
                            }
                        )

                    return posts
        except Exception as e:
            print(f"[REDDIT] Error fetching r/{subreddit}: {e}")

        return []


# ─── SENTIMENT ANALYZER ─────────────────────────────────────────────────────


class SentimentAnalyzer:
    """VADER-based sentiment analysis with language detection."""

    def __init__(self):
        self.analyzer = SentimentIntensityAnalyzer() if VADER_AVAILABLE else None

    def analyze(self, text: str) -> dict:
        """Analyze text sentiment."""
        if not self.analyzer or not text:
            return {"compound": 0, "pos": 0, "neg": 0, "neu": 1, "language": "unknown"}

        # Get VADER scores
        scores = self.analyzer.polarity_scores(text)

        # Detect language
        try:
            lang = detect(text)
        except LangDetectException:
            lang = "unknown"

        return {
            "compound": scores["compound"],  # -1 to 1
            "pos": scores["pos"],
            "neg": scores["neg"],
            "neu": scores["neu"],
            "language": lang,
        }

    def mean_sentiment(self, sentiments: List[dict]) -> dict:
        """Calculate mean sentiment across multiple texts."""
        if not sentiments:
            return {"compound": 0, "pos": 0, "neg": 0, "neu": 1}

        n = len(sentiments)
        return {
            "compound": sum(s["compound"] for s in sentiments) / n,
            "pos": sum(s["pos"] for s in sentiments) / n,
            "neg": sum(s["neg"] for s in sentiments) / n,
            "neu": sum(s["neu"] for s in sentiments) / n,
        }


# ─── MAIN LOOP ───────────────────────────────────────────────────────────────


async def main():
    print(f"[REDDIT-AGENT] Starting with {POLL_INTERVAL}s interval")
    print(f"[REDDIT-AGENT] Monitoring: {REDDIT_SUBREDDITS}")

    forage = ForageClient(APIFY_TOKEN, FORAGE_ENDPOINT)
    reddit = RedditClient()
    analyzer = SentimentAnalyzer() if VADER_AVAILABLE else None

    if not VADER_AVAILABLE:
        print("[REDDIT] Warning: Running without sentiment analysis")

    while True:
        try:
            posts_processed = 0
            signals_written = 0

            for subreddit in REDDIT_SUBREDDITS:
                subreddit = subreddit.strip()
                if not subreddit:
                    continue

                # Fetch posts
                posts = await reddit.get_subreddit_posts(subreddit, REDDIT_LIMIT)
                print(f"[REDDIT] r/{subreddit}: {len(posts)} posts")

                # Analyze each post
                sentiments = []
                for post in posts:
                    text = f"{post['title']} {post['selftext']}"
                    if not text.strip():
                        continue

                    posts_processed += 1

                    if analyzer:
                        sentiment = analyzer.analyze(text)
                        sentiments.append(sentiment)

                # Calculate aggregate sentiment for subreddit
                if sentiments:
                    mean_sent = analyzer.mean_sentiment(sentiments)
                    velocity = (mean_sent["compound"] + 1) / 2  # Convert -1..1 to 0..1
                    reach = min(1.0, len(posts) / 100)  # Normalize by post count

                    # Write signal to graph
                    await forage.add_signal(
                        f"r/{subreddit}", "narrative_velocity", velocity
                    )

                    # Write claim for evidence
                    await forage.add_claim(
                        f"r/{subreddit}",
                        "REDDIT_SENTIMENT",
                        f"{len(posts)} posts",
                        json.dumps(
                            {
                                "compound": mean_sent["compound"],
                                "positive": mean_sent["pos"],
                                "negative": mean_sent["neg"],
                                "posts_analyzed": len(sentiments),
                            }
                        ),
                        f"reddit.com/r/{subreddit}",
                        0.7,
                    )

                    signals_written += 1
                    print(
                        f"[REDDIT] r/{subreddit}: velocity={velocity:.2f}, reach={reach:.2f}"
                    )

            print(
                f"[REDDIT-AGENT] Processed {posts_processed} posts, wrote {signals_written} signals"
            )

        except Exception as e:
            print(f"[REDDIT-AGENT] Error: {e}")
            # Exponential backoff on failure (M5 fix)
            backoff = min(120, POLL_INTERVAL * 2)
            print(f"[REDDIT-AGENT] Backing off for {backoff}s")
            await asyncio.sleep(backoff)
            continue

        # Wait for next interval
        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
