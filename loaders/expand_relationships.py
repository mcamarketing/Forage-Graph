#!/usr/bin/env python3
"""Expand relationships to increase edge density."""
import requests
import time

API = 'https://forage-graph-production.up.railway.app'
SECRET = '6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959'
headers = {'Authorization': f'Bearer {SECRET}', 'Content-Type': 'application/json'}

# More comprehensive competitor relationships
COMPETITORS = [
    # Big Tech
    ('Apple', 'Google', 'competes_with'),
    ('Apple', 'Microsoft', 'competes_with'),
    ('Apple', 'Samsung', 'competes_with'),
    ('Apple', 'Meta', 'competes_with'),
    ('Google', 'Microsoft', 'competes_with'),
    ('Google', 'Meta', 'competes_with'),
    ('Google', 'Amazon', 'competes_with'),
    ('Microsoft', 'Amazon', 'competes_with'),
    ('Microsoft', 'Salesforce', 'competes_with'),
    ('Microsoft', 'Oracle', 'competes_with'),

    # Semiconductors
    ('NVIDIA', 'AMD', 'competes_with'),
    ('NVIDIA', 'Intel', 'competes_with'),
    ('NVIDIA', 'Qualcomm', 'competes_with'),
    ('AMD', 'Intel', 'competes_with'),
    ('TSMC', 'Samsung', 'competes_with'),
    ('TSMC', 'Intel', 'competes_with'),
    ('Qualcomm', 'MediaTek', 'competes_with'),
    ('Qualcomm', 'Apple', 'competes_with'),
    ('Broadcom', 'Qualcomm', 'competes_with'),
    ('ASML', 'Applied Materials', 'competes_with'),

    # Streaming
    ('Netflix', 'Disney', 'competes_with'),
    ('Netflix', 'Amazon Prime', 'competes_with'),
    ('Netflix', 'HBO Max', 'competes_with'),
    ('Netflix', 'Apple TV', 'competes_with'),
    ('Disney', 'Warner Bros', 'competes_with'),
    ('Spotify', 'Apple Music', 'competes_with'),
    ('Spotify', 'Amazon Music', 'competes_with'),

    # Social
    ('Meta', 'TikTok', 'competes_with'),
    ('Meta', 'Twitter', 'competes_with'),
    ('Meta', 'Snap', 'competes_with'),
    ('TikTok', 'YouTube', 'competes_with'),
    ('Twitter', 'Threads', 'competes_with'),

    # E-Commerce
    ('Amazon', 'Walmart', 'competes_with'),
    ('Amazon', 'Alibaba', 'competes_with'),
    ('Amazon', 'Shopify', 'competes_with'),
    ('Alibaba', 'JD.com', 'competes_with'),
    ('Alibaba', 'PDD', 'competes_with'),
    ('eBay', 'Amazon', 'competes_with'),

    # Ride-hailing
    ('Uber', 'Lyft', 'competes_with'),
    ('Uber', 'Didi', 'competes_with'),
    ('Uber', 'Grab', 'competes_with'),

    # Payments
    ('Visa', 'Mastercard', 'competes_with'),
    ('Visa', 'PayPal', 'competes_with'),
    ('PayPal', 'Square', 'competes_with'),
    ('PayPal', 'Stripe', 'competes_with'),
    ('Stripe', 'Adyen', 'competes_with'),

    # Banks
    ('JPMorgan', 'Goldman Sachs', 'competes_with'),
    ('JPMorgan', 'Morgan Stanley', 'competes_with'),
    ('JPMorgan', 'Bank of America', 'competes_with'),
    ('JPMorgan', 'Citigroup', 'competes_with'),
    ('Goldman Sachs', 'Morgan Stanley', 'competes_with'),
    ('BlackRock', 'Vanguard', 'competes_with'),
    ('BlackRock', 'State Street', 'competes_with'),

    # Auto
    ('Tesla', 'Toyota', 'competes_with'),
    ('Tesla', 'Volkswagen', 'competes_with'),
    ('Tesla', 'BYD', 'competes_with'),
    ('Tesla', 'Ford', 'competes_with'),
    ('Tesla', 'GM', 'competes_with'),
    ('Tesla', 'Rivian', 'competes_with'),
    ('Toyota', 'Honda', 'competes_with'),
    ('Toyota', 'Ford', 'competes_with'),
    ('Ford', 'GM', 'competes_with'),
    ('Volkswagen', 'BMW', 'competes_with'),
    ('Mercedes', 'BMW', 'competes_with'),

    # Aerospace
    ('Boeing', 'Airbus', 'competes_with'),
    ('Lockheed Martin', 'Northrop Grumman', 'competes_with'),
    ('Lockheed Martin', 'Raytheon', 'competes_with'),
    ('SpaceX', 'Blue Origin', 'competes_with'),
    ('SpaceX', 'ULA', 'competes_with'),

    # Pharma
    ('Pfizer', 'Moderna', 'competes_with'),
    ('Pfizer', 'Merck', 'competes_with'),
    ('Pfizer', 'Johnson & Johnson', 'competes_with'),
    ('Eli Lilly', 'Novo Nordisk', 'competes_with'),
    ('AbbVie', 'Bristol-Myers', 'competes_with'),

    # Energy
    ('ExxonMobil', 'Chevron', 'competes_with'),
    ('ExxonMobil', 'Shell', 'competes_with'),
    ('ExxonMobil', 'BP', 'competes_with'),
    ('Shell', 'BP', 'competes_with'),
    ('TotalEnergies', 'Shell', 'competes_with'),

    # Consumer
    ('Coca-Cola', 'PepsiCo', 'competes_with'),
    ('Nike', 'Adidas', 'competes_with'),
    ('Nike', 'Under Armour', 'competes_with'),
    ('McDonalds', 'Starbucks', 'competes_with'),
    ('Walmart', 'Target', 'competes_with'),
    ('Costco', 'Walmart', 'competes_with'),
]

# Partnership relationships
PARTNERSHIPS = [
    ('Apple', 'TSMC', 'partner_of'),
    ('Apple', 'Qualcomm', 'partner_of'),
    ('Google', 'Samsung', 'partner_of'),
    ('Microsoft', 'OpenAI', 'partner_of'),
    ('Microsoft', 'SAP', 'partner_of'),
    ('Amazon', 'Anthropic', 'partner_of'),
    ('NVIDIA', 'Microsoft', 'partner_of'),
    ('NVIDIA', 'Google', 'partner_of'),
    ('NVIDIA', 'Amazon', 'partner_of'),
    ('NVIDIA', 'Meta', 'partner_of'),
    ('Intel', 'Microsoft', 'partner_of'),
    ('AMD', 'Microsoft', 'partner_of'),
    ('Oracle', 'Microsoft', 'partner_of'),
    ('Salesforce', 'Google', 'partner_of'),
    ('Tesla', 'Panasonic', 'partner_of'),
    ('SpaceX', 'NASA', 'partner_of'),
    ('Boeing', 'NASA', 'partner_of'),
]

# Supply relationships
SUPPLY = [
    ('TSMC', 'Apple', 'supplies_to'),
    ('TSMC', 'NVIDIA', 'supplies_to'),
    ('TSMC', 'AMD', 'supplies_to'),
    ('TSMC', 'Qualcomm', 'supplies_to'),
    ('Samsung', 'Apple', 'supplies_to'),
    ('Samsung', 'Google', 'supplies_to'),
    ('Foxconn', 'Apple', 'supplies_to'),
    ('Foxconn', 'Amazon', 'supplies_to'),
    ('ASML', 'TSMC', 'supplies_to'),
    ('ASML', 'Samsung', 'supplies_to'),
    ('ASML', 'Intel', 'supplies_to'),
    ('Panasonic', 'Tesla', 'supplies_to'),
    ('CATL', 'Tesla', 'supplies_to'),
    ('LG Energy', 'Tesla', 'supplies_to'),
    ('LG Energy', 'GM', 'supplies_to'),
    ('Corning', 'Apple', 'supplies_to'),
    ('SK Hynix', 'Apple', 'supplies_to'),
    ('Micron', 'Apple', 'supplies_to'),
]

# Cross-industry dependencies
DEPENDENCIES = [
    ('Semiconductors', 'AI', 'enables'),
    ('Semiconductors', 'Automotive', 'enables'),
    ('Semiconductors', 'Consumer Electronics', 'enables'),
    ('Cloud Computing', 'AI', 'enables'),
    ('Cloud Computing', 'E-Commerce', 'enables'),
    ('Energy', 'Manufacturing', 'enables'),
    ('Logistics', 'E-Commerce', 'enables'),
    ('5G', 'IoT', 'enables'),
]

all_connections = []
for from_name, to_name, rel_type in COMPETITORS:
    all_connections.append({
        'from_name': from_name,
        'to_name': to_name,
        'relationship': rel_type,
        'confidence': 0.95,
        'source': 'competitor_analysis'
    })

for from_name, to_name, rel_type in PARTNERSHIPS:
    all_connections.append({
        'from_name': from_name,
        'to_name': to_name,
        'relationship': rel_type,
        'confidence': 0.9,
        'source': 'partnership_analysis'
    })

for from_name, to_name, rel_type in SUPPLY:
    all_connections.append({
        'from_name': from_name,
        'to_name': to_name,
        'relationship': rel_type,
        'confidence': 0.95,
        'source': 'supply_chain_analysis'
    })

for from_name, to_name, rel_type in DEPENDENCIES:
    all_connections.append({
        'from_name': from_name,
        'to_name': to_name,
        'relationship': rel_type,
        'confidence': 0.85,
        'source': 'industry_analysis'
    })

print(f"Total connections to add: {len(all_connections)}")
print(f"  Competitors: {len(COMPETITORS)}")
print(f"  Partnerships: {len(PARTNERSHIPS)}")
print(f"  Supply chain: {len(SUPPLY)}")
print(f"  Dependencies: {len(DEPENDENCIES)}")

# Get current stats
resp = requests.get(f'{API}/stats', headers=headers)
stats = resp.json()
print(f"\nBefore: {stats['total_entities']} entities, {stats['total_relationships']} relationships")
print(f"Ratio: {stats['total_relationships']/stats['total_entities']:.2f}")

# Load in chunks
print("\nLoading...")
total_added = 0
chunk_size = 10
for i in range(0, len(all_connections), chunk_size):
    chunk = all_connections[i:i+chunk_size]
    payload = {'connections': chunk}

    for attempt in range(3):
        try:
            resp = requests.post(f'{API}/ingest/bulk', headers=headers, json=payload, timeout=60)
            if resp.status_code == 503:
                time.sleep(5)
                continue
            if resp.status_code in (200, 201):
                data = resp.json()
                added = data.get('connections', {}).get('added', 0)
                total_added += added
                print(f"  Chunk {i//chunk_size + 1}: +{added}")
            break
        except Exception as e:
            time.sleep(3)
    time.sleep(1)

# Final stats
resp = requests.get(f'{API}/stats', headers=headers)
stats = resp.json()
print(f"\nAfter: {stats['total_entities']} entities, {stats['total_relationships']} relationships")
print(f"Ratio: {stats['total_relationships']/stats['total_entities']:.2f} edges/node")
print(f"Total added: {total_added}")
