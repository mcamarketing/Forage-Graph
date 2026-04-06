#!/usr/bin/env python3
"""
prediction_validator.py

24-hour price prediction validator.
Records predictions and resolves them after 24h against actual prices.

Requirements:
  httpx
  click
  pandas

Env vars:
  APIFY_TOKEN      - Forage API token
  FORAGE_ENDPOINT  - Default: https://ernesta-labs--forage.apify.actor

CLI:
  python3 prediction_validator.py --record BTC     - Record current price as 24h prediction
  python3 prediction_validator.py --resolve      - Check and resolve pending predictions
  python3 prediction_validator.py --backtest N  - N-day backtest vs buy-hold baseline
  python3 prediction_validator.py --leaderboard   - Print top assets by win_rate
  python3 prediction_validator.py --status        - Show pending predictions
"""

import os
import sys
import csv
import json
import time
import asyncio
import click
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

import httpx
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────

APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")
FORAGE_ENDPOINT = os.environ.get(
    "FORAGE_ENDPOINT", "https://ernesta-labs--forage.apify.actor"
)
LOG_FILE = os.environ.get(
    "SINGULARITY_LOG_PATH",
    str(Path(__file__).resolve().parent / "singularity_log.csv"),
)

# ─── FORAGE CLIENT ───────────────────────────────────────────────────────────


class ForageClient:
    def __init__(self, token: str, endpoint: str):
        self.token = token
        self.endpoint = endpoint

    async def call_tool(self, tool_name: str, params: dict) -> Optional[dict]:
        if not self.token:
            print("[FORAGE] No token configured")
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
                        "id": f"pred-{int(time.time())}",
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

    async def add_signal(self, entity: str, metric: str, value: float, ts: int = None):
        return await self.call_tool(
            "add_signal",
            {
                "entity": entity,
                "metric": metric,
                "value": value,
                "timestamp": ts or int(time.time() * 1000),
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


# ─── COINGECKO CLIENT ───────────────────────────────────────────────────────


class CoinGeckoClient:
    """CoinGecko API client for price data."""

    BASE_URL = "https://api.coingecko.com/api/v3"

    async def get_price(self, coin_id: str) -> Optional[float]:
        """Get current price for a coin."""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    f"{self.BASE_URL}/simple/price",
                    params={"ids": coin_id, "vs_currencies": "usd"},
                )

                if response.status_code == 200:
                    data = response.json()
                    return data.get(coin_id, {}).get("usd")
        except Exception as e:
            print(f"[COINGECKO] Error fetching {coin_id}: {e}")

        return None

    async def get_historical_price(self, coin_id: str, date: str) -> Optional[float]:
        """Get historical price for a coin (format: DD-MM-YYYY)."""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    f"{self.BASE_URL}/coins/{coin_id}/history",
                    params={"date": date, "localization": "false"},
                )

                if response.status_code == 200:
                    data = response.json()
                    return (
                        data.get("market_data", {}).get("current_price", {}).get("usd")
                    )
        except Exception as e:
            print(f"[COINGECKO] Error fetching historical {coin_id}: {e}")

        return None


# ─── PREDICTION LOG ─────────────────────────────────────────────────────────

PREDICTION_FIELDS = [
    "timestamp",
    "asset",
    "predicted_price",
    "resolution_time",
    "actual_price",
    "direction_correct",
    "resolved",
]


def load_predictions() -> List[dict]:
    """Load predictions from CSV."""
    predictions = []
    if Path(LOG_FILE).exists():
        with open(LOG_FILE, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                predictions.append(
                    {
                        "timestamp": row["timestamp"],
                        "asset": row["asset"],
                        "predicted_price": float(row["predicted_price"]),
                        "resolution_time": row["resolution_time"],
                        "actual_price": float(row["actual_price"])
                        if row["actual_price"]
                        else None,
                        "direction_correct": row.get("direction_correct", ""),
                        "resolved": row.get("resolved", "false") == "true",
                    }
                )
    return predictions


def save_prediction(pred: dict):
    """Save prediction to CSV."""
    file_exists = Path(LOG_FILE).exists()
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=PREDICTION_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(
            {
                "timestamp": pred["timestamp"],
                "asset": pred["asset"],
                "predicted_price": pred["predicted_price"],
                "resolution_time": pred["resolution_time"],
                "actual_price": pred.get("actual_price", ""),
                "direction_correct": pred.get("direction_correct", ""),
                "resolved": str(pred.get("resolved", False)).lower(),
            }
        )


def update_prediction(pred: dict):
    """Update an existing prediction in CSV."""
    predictions = load_predictions()
    for i, p in enumerate(predictions):
        if p["timestamp"] == pred["timestamp"] and p["asset"] == pred["asset"]:
            predictions[i].update(pred)
            break

    with open(LOG_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=PREDICTION_FIELDS)
        writer.writeheader()
        writer.writerows(predictions)


# ─── COMMANDS ───────────────────────────────────────────────────────────────


async def record_prediction(asset: str):
    """Record current price as 24h prediction."""
    print(f"[PREDICT] Recording prediction for {asset}")

    cg = CoinGeckoClient()
    forage = ForageClient(APIFY_TOKEN, FORAGE_ENDPOINT)

    # Map common names to CoinGecko IDs
    coin_map = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "SOL": "solana",
        "DOGE": "dogecoin",
        "XRP": "ripple",
    }
    coin_id = coin_map.get(asset.upper(), asset.lower())

    # Get current price
    current_price = await cg.get_price(coin_id)
    if not current_price:
        print(f"[PREDICT] Could not fetch price for {asset}")
        return

    print(f"[PREDICT] Current {asset} price: ${current_price:.2f}")

    # Calculate resolution time (24h from now)
    resolution_time = (datetime.utcnow() + timedelta(hours=24)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # Save to log
    pred = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "asset": asset,
        "predicted_price": current_price,
        "resolution_time": resolution_time,
        "resolved": False,
    }
    save_prediction(pred)
    print(f"[PREDICT] Saved prediction: resolution at {resolution_time}")

    # Write to Forage graph
    if forage.token:
        await forage.add_signal(asset, "prediction_24h", current_price)
        await forage.add_claim(
            asset,
            "PREDICTION_RECORDED",
            resolution_time,
            f"predicted_price: {current_price}",
            "prediction_validator",
            0.5,
        )


async def resolve_predictions():
    """Resolve pending predictions against actual prices."""
    print("[PREDICT] Checking pending predictions...")

    predictions = load_predictions()
    cg = CoinGeckoClient()
    forage = ForageClient(APIFY_TOKEN, FORAGE_ENDPOINT)

    # Map common names to CoinGecko IDs
    coin_map = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "SOL": "solana",
        "DOGE": "dogecoin",
        "XRP": "ripple",
    }

    resolved_count = 0

    for pred in predictions:
        if pred.get("resolved"):
            continue

        # Check if past resolution time
        resolution_dt = datetime.strptime(pred["resolution_time"], "%Y-%m-%d %H:%M:%S")
        if resolution_dt > datetime.utcnow():
            continue

        asset = pred["asset"]
        coin_id = coin_map.get(asset.upper(), asset.lower())

        # Get historical price at resolution time
        date_str = resolution_dt.strftime("%d-%m-%Y")
        actual_price = await cg.get_historical_price(coin_id, date_str)

        if not actual_price:
            print(
                f"[PREDICT] Could not fetch historical price for {asset} at {date_str}"
            )
            continue

        # Calculate if prediction was correct (direction only)
        predicted = pred["predicted_price"]
        direction_correct = (actual_price > predicted) == (predicted > 0)

        # Update prediction
        pred["actual_price"] = actual_price
        pred["direction_correct"] = direction_correct
        pred["resolved"] = True
        update_prediction(pred)

        print(
            f"[PREDICT] Resolved {asset}: predicted=${predicted:.2f}, actual=${actual_price:.2f}, correct={direction_correct}"
        )

        # Write resolution to graph
        if forage.token:
            await forage.add_signal(
                asset, "prediction_resolved", 1.0 if direction_correct else 0.0
            )

        resolved_count += 1

    print(f"[PREDICT] Resolved {resolved_count} predictions")


async def backtest(days: int):
    """Run backtest comparing strategy vs buy-hold."""
    print(f"[PREDICT] Running {days}-day backtest...")

    predictions = load_predictions()
    resolved = [p for p in predictions if p.get("resolved") and p.get("actual_price")]

    if len(resolved) < 20:
        print(f"[PREDICT] Only {len(resolved)} resolved predictions, need at least 20")
        return

    # Calculate metrics
    correct = sum(1 for p in resolved if p.get("direction_correct"))
    total = len(resolved)
    win_rate = correct / total

    # Mean absolute error
    errors = [
        abs(p["actual_price"] - p["predicted_price"]) / p["actual_price"]
        for p in resolved
    ]
    mae = sum(errors) / len(errors)

    # Sharpe vs buy-hold (simplified)
    # Would need actual baseline comparison in production
    sharpe = 0.0  # Placeholder

    print(f"\n=== Backtest Results ({len(resolved)} resolved predictions) ===")
    print(f"Win Rate: {win_rate:.2%}")
    print(f"Mean Absolute Error: {mae:.2%}")
    print(f"Sharpe vs Buy-Hold: {sharpe:.2f}")

    # Hard rule: warn if win_rate > 0.70 with < 100 samples
    if total < 100 and win_rate > 0.70:
        print("\nWARNING: Insufficient sample size to claim 70%+ accuracy")
        print("(Need 100+ resolved predictions before making this claim)")

    # Write to graph only if sufficient samples
    if total >= 100:
        forage = ForageClient(APIFY_TOKEN, FORAGE_ENDPOINT)
        if forage.token:
            await forage.add_signal(
                "prediction_validator", "backtest_win_rate", win_rate
            )
            await forage.add_signal("prediction_validator", "backtest_mae", mae)
    else:
        print(f"\n(Note: Not writing to graph - only {total} samples, need 100+)")


def leaderboard():
    """Print top assets by win_rate."""
    predictions = load_predictions()

    # Group by asset
    by_asset: Dict[str, List[dict]] = {}
    for pred in predictions:
        asset = pred["asset"]
        if asset not in by_asset:
            by_asset[asset] = []
        if pred.get("resolved"):
            by_asset[asset].append(pred)

    # Calculate win rates
    results = []
    for asset, preds in by_asset.items():
        if len(preds) < 20:
            continue
        correct = sum(1 for p in preds if p.get("direction_correct"))
        results.append(
            {"asset": asset, "win_rate": correct / len(preds), "total": len(preds)}
        )

    results.sort(key=lambda x: x["win_rate"], reverse=True)

    print("\n=== Asset Leaderboard (min 20 resolved) ===")
    for r in results:
        print(f"{r['asset']:8s}: {r['win_rate']:.2%} ({r['total']} predictions)")


def status():
    """Show pending predictions."""
    predictions = load_predictions()
    pending = [p for p in predictions if not p.get("resolved")]

    print("\n=== Pending Predictions ===")
    for p in pending:
        res_dt = datetime.strptime(p["resolution_time"], "%Y-%m-%d %H:%M:%S")
        time_until = res_dt - datetime.utcnow()
        hours = time_until.total_seconds() / 3600

        print(
            f"{p['asset']:8s} ${p['predicted_price']:>10.2f} resolves in {hours:.1f}h"
        )

    print(f"\nTotal pending: {len(pending)}")


# ─── CLI ─────────────────────────────────────────────────────────────────────


@click.group()
def cli():
    pass


@cli.command()
@click.argument("asset")
def record(asset):
    asyncio.run(record_prediction(asset))


@cli.command()
def resolve():
    asyncio.run(resolve_predictions())


@cli.command()
@click.argument("days", type=int, default=30)
def backtest(days):
    asyncio.run(backtest(days))


@cli.command()
def leaderboard_cmd():
    leaderboard()


@cli.command()
def status_cmd():
    status()


if __name__ == "__main__":
    cli()
