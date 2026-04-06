#!/usr/bin/env python3
"""
Direct loader for FalkorDB Cloud.
Uses the same APIs as Railway but points to FalkorDB Cloud.
"""

import os
import json
import time
import requests
import redis

# Configure for your FalkorDB Cloud instance
CLOUD_INSTANCE_ID = "instance-dyps0zi57"
CLOUD_PASSWORD = os.environ.get("FALKORDB_PASSWORD", "")

CLOUD_GRAPH_URL = f"https://api.falkordb.cloud/v1/graphs/{CLOUD_INSTANCE_ID}"
HEADERS = {
    "Authorization": f"Bearer {CLOUD_PASSWORD}",
    "Content-Type": "application/json",
}

def main():
    print("FalkorDB Cloud Loader")
    print(f"Instance: {CLOUD_INSTANCE_ID}")
    print("\nConfigure CLOUD_PASSWORD env var and run bulk loaders.")
    print("Point GRAPH_URL to FalkorDB Cloud API.")

if __name__ == "__main__":
    main()
