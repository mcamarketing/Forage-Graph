#!/usr/bin/env python3
"""
Forage Intel Daemon — Continuous non-stop intelligence collection
Runs forever, cycling through all collectors on configurable schedules.

Usage:
  python intel_daemon.py                    # Run with defaults
  python intel_daemon.py --interval 30      # Run every 30 minutes
  python intel_daemon.py --fast             # Quick cycle (15 min)
  python intel_daemon.py --aggressive       # Very fast (5 min)

Features:
  - Runs continuously until stopped (Ctrl+C)
  - Automatic retry on failures
  - Staggered collection to avoid rate limits
  - Health monitoring and auto-recovery
  - Logs everything to file
"""

import os
import sys
import time
import json
import signal
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import collectors
try:
    from intel_collector import (
        collect_wikidata, collect_opensanctions, collect_sec_edgar,
        collect_worldbank, collect_news, collect_github_trending,
        post_batches
    )
    from global_intel import (
        collect_un_countries, collect_imf, collect_gdelt, collect_crypto,
        collect_trade, collect_geopolitical, collect_energy, collect_major_companies
    )
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure intel_collector.py and global_intel.py are in the same directory")
    sys.exit(1)

# ─── CONFIG ────────────────────────────────────────────────────────────────────

GRAPH_URL = os.environ.get("GRAPH_URL", "https://forage-graph-production.up.railway.app")
GRAPH_SECRET = os.environ.get("GRAPH_SECRET", "6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959")
LOG_FILE = os.environ.get("INTEL_LOG_FILE", "intel_daemon.log")

# Default intervals (minutes)
DEFAULT_INTERVALS = {
    "news_rss": 15,           # News: every 15 min
    "gdelt": 30,              # GDELT: every 30 min
    "crypto": 60,             # Crypto: every hour
    "github": 120,            # GitHub: every 2 hours
    "opensanctions": 360,     # Sanctions: every 6 hours
    "sec_edgar": 720,         # SEC: every 12 hours
    "wikidata": 1440,         # Wikidata: daily
    "un_countries": 1440,     # Countries: daily
    "worldbank": 1440,        # World Bank: daily
    "imf": 1440,              # IMF: daily
    "trade": 1440,            # Trade: daily
    "geopolitical": 240,      # Think tanks: every 4 hours
    "energy": 720,            # Energy: every 12 hours
    "major_companies": 2880,  # Companies: every 2 days
}

# ─── LOGGING ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("intel_daemon")

# ─── COLLECTOR REGISTRY ────────────────────────────────────────────────────────

COLLECTORS = {
    # From intel_collector.py
    "wikidata": collect_wikidata,
    "opensanctions": lambda: collect_opensanctions(limit=3000),
    "sec_edgar": lambda: collect_sec_edgar(limit=2000),
    "worldbank": collect_worldbank,
    "news_rss": collect_news,
    "github": collect_github_trending,
    # From global_intel.py
    "un_countries": collect_un_countries,
    "imf": collect_imf,
    "gdelt": collect_gdelt,
    "crypto": collect_crypto,
    "trade": collect_trade,
    "geopolitical": collect_geopolitical,
    "energy": collect_energy,
    "major_companies": collect_major_companies,
}

# ─── DAEMON STATE ──────────────────────────────────────────────────────────────

class IntelDaemon:
    def __init__(self, interval_multiplier: float = 1.0):
        self.running = True
        self.stop_event = Event()
        self.last_run: Dict[str, datetime] = {}
        self.run_counts: Dict[str, int] = {k: 0 for k in COLLECTORS}
        self.error_counts: Dict[str, int] = {k: 0 for k in COLLECTORS}
        self.total_entities = 0
        self.total_connections = 0
        self.interval_multiplier = interval_multiplier
        self.start_time = datetime.utcnow()

        # Adjust intervals
        self.intervals = {
            k: max(1, int(v * interval_multiplier))
            for k, v in DEFAULT_INTERVALS.items()
        }

    def should_run(self, collector_name: str) -> bool:
        """Check if a collector should run based on its interval."""
        if collector_name not in self.last_run:
            return True

        elapsed = (datetime.utcnow() - self.last_run[collector_name]).total_seconds() / 60
        return elapsed >= self.intervals.get(collector_name, 60)

    def run_collector(self, name: str) -> Dict:
        """Run a single collector and return results."""
        if name not in COLLECTORS:
            return {"error": f"Unknown collector: {name}"}

        logger.info(f"[{name}] Starting collection...")
        start = time.time()

        try:
            collector_fn = COLLECTORS[name]
            batches = collector_fn()

            if not batches:
                logger.warning(f"[{name}] No data collected")
                return {"entities": 0, "connections": 0, "skipped": True}

            result = post_batches(batches, name)
            elapsed = time.time() - start

            entities = result.get("entities", 0)
            connections = result.get("connections", 0)

            self.total_entities += entities
            self.total_connections += connections
            self.run_counts[name] += 1
            self.last_run[name] = datetime.utcnow()

            logger.info(f"[{name}] Completed in {elapsed:.1f}s: {entities} entities, {connections} connections")
            return result

        except Exception as e:
            self.error_counts[name] += 1
            logger.error(f"[{name}] Error: {e}")
            return {"error": str(e)}

    def run_cycle(self) -> Dict:
        """Run one full cycle of all due collectors."""
        results = {}
        due_collectors = [name for name in COLLECTORS if self.should_run(name)]

        if not due_collectors:
            return {"message": "No collectors due"}

        logger.info(f"Running {len(due_collectors)} collectors: {', '.join(due_collectors)}")

        for name in due_collectors:
            if not self.running:
                break

            results[name] = self.run_collector(name)

            # Small delay between collectors to be nice to APIs
            if self.running:
                time.sleep(2)

        return results

    def get_status(self) -> Dict:
        """Get current daemon status."""
        uptime = (datetime.utcnow() - self.start_time).total_seconds()
        return {
            "running": self.running,
            "uptime_seconds": int(uptime),
            "uptime_human": str(timedelta(seconds=int(uptime))),
            "total_entities": self.total_entities,
            "total_connections": self.total_connections,
            "run_counts": self.run_counts,
            "error_counts": self.error_counts,
            "last_run": {k: v.isoformat() for k, v in self.last_run.items()},
            "intervals": self.intervals,
        }

    def run_forever(self, check_interval: int = 60):
        """Run daemon forever, checking for due collectors."""
        logger.info("=" * 60)
        logger.info("FORAGE INTEL DAEMON STARTED")
        logger.info(f"Intervals: {self.intervals}")
        logger.info("=" * 60)

        # Initial full run
        logger.info("Running initial full collection...")
        self.run_cycle()

        while self.running:
            try:
                # Wait for check interval
                self.stop_event.wait(timeout=check_interval)

                if not self.running:
                    break

                # Check and run due collectors
                self.run_cycle()

                # Log status periodically
                status = self.get_status()
                logger.info(f"Status: {status['total_entities']} entities, {status['total_connections']} connections, uptime: {status['uptime_human']}")

            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt")
                break
            except Exception as e:
                logger.error(f"Daemon error: {e}")
                time.sleep(30)  # Wait before retry

        logger.info("Daemon stopped")

    def stop(self):
        """Stop the daemon gracefully."""
        logger.info("Stopping daemon...")
        self.running = False
        self.stop_event.set()


# ─── SIGNAL HANDLERS ───────────────────────────────────────────────────────────

daemon = None

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global daemon
    logger.info(f"Received signal {signum}")
    if daemon:
        daemon.stop()

# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    global daemon

    parser = argparse.ArgumentParser(description="Forage Intel Daemon - Continuous Collection")
    parser.add_argument("--interval", type=int, default=60, help="Check interval in seconds (default: 60)")
    parser.add_argument("--fast", action="store_true", help="Fast mode: 0.25x intervals")
    parser.add_argument("--aggressive", action="store_true", help="Aggressive mode: 0.1x intervals")
    parser.add_argument("--slow", action="store_true", help="Slow mode: 2x intervals")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--status", action="store_true", help="Show status and exit")

    args = parser.parse_args()

    # Determine interval multiplier
    multiplier = 1.0
    if args.fast:
        multiplier = 0.25
        logger.info("FAST MODE: Running at 4x speed")
    elif args.aggressive:
        multiplier = 0.1
        logger.info("AGGRESSIVE MODE: Running at 10x speed")
    elif args.slow:
        multiplier = 2.0
        logger.info("SLOW MODE: Running at 0.5x speed")

    # Create daemon
    daemon = IntelDaemon(interval_multiplier=multiplier)

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if args.once:
        # Single run mode
        logger.info("Running single collection cycle...")
        results = daemon.run_cycle()
        print(json.dumps(results, indent=2, default=str))
    elif args.status:
        print(json.dumps(daemon.get_status(), indent=2))
    else:
        # Continuous mode
        try:
            daemon.run_forever(check_interval=args.interval)
        except KeyboardInterrupt:
            daemon.stop()
        finally:
            # Final status
            status = daemon.get_status()
            logger.info("=" * 60)
            logger.info("FINAL STATUS")
            logger.info(f"Uptime: {status['uptime_human']}")
            logger.info(f"Total Entities: {status['total_entities']}")
            logger.info(f"Total Connections: {status['total_connections']}")
            logger.info(f"Run Counts: {status['run_counts']}")
            logger.info(f"Error Counts: {status['error_counts']}")
            logger.info("=" * 60)

if __name__ == "__main__":
    main()
