#!/bin/bash
# Forage Intel Daemon - Continuous Runner (Unix/Mac)
# Runs non-stop until Ctrl+C
#
# Usage:
#   ./run_daemon.sh              - Normal mode
#   ./run_daemon.sh fast         - 4x faster
#   ./run_daemon.sh aggressive   - 10x faster

set -e
cd "$(dirname "$0")"

echo "============================================"
echo "  FORAGE INTEL DAEMON - CONTINUOUS MODE"
echo "  Press Ctrl+C to stop"
echo "============================================"
echo

# Check Python
command -v python3 &> /dev/null || { echo "Python3 required"; exit 1; }

# Install deps
python3 -c "import requests" 2>/dev/null || pip3 install requests

# Run daemon
case "$1" in
    fast)
        echo "Starting FAST mode (4x speed)..."
        python3 intel_daemon.py --fast
        ;;
    aggressive)
        echo "Starting AGGRESSIVE mode (10x speed)..."
        python3 intel_daemon.py --aggressive
        ;;
    slow)
        echo "Starting SLOW mode (0.5x speed)..."
        python3 intel_daemon.py --slow
        ;;
    *)
        echo "Starting NORMAL mode..."
        python3 intel_daemon.py
        ;;
esac
