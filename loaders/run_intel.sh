#!/bin/bash
# Forage Intel Collector - Unix/Mac Runner
# Run: ./run_intel.sh [all|wikidata|sanctions|sec|worldbank|news|github]

set -e
cd "$(dirname "$0")"

echo "============================================"
echo "  FORAGE INTEL COLLECTOR"
echo "============================================"
echo

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python3 not found. Please install Python 3.8+"
    exit 1
fi

# Install dependencies if needed
if ! python3 -c "import requests" 2>/dev/null; then
    echo "Installing dependencies..."
    pip3 install requests
fi

# Run collector
if [ -z "$1" ]; then
    echo "Running ALL collectors..."
    python3 intel_collector.py --all --output intel_results.json
else
    echo "Running $1 collector..."
    python3 intel_collector.py --$1 --output intel_results.json
fi

echo
echo "Done! Check intel_results.json for details."
