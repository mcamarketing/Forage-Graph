#!/usr/bin/env python3
"""Bulk add connections to the knowledge graph."""
import requests
import time

API = 'https://forage-graph-production.up.railway.app'
SECRET = '6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959'
headers = {'Authorization': f'Bearer {SECRET}', 'Content-Type': 'application/json'}

# All relationships to add
relationships = [
    # Tech supply chain
    ('Apple', 'TSMC', 'procures_from'),
    ('Apple', 'Samsung', 'procures_from'),
    ('Apple', 'Foxconn', 'manufactured_by'),
    ('NVIDIA', 'TSMC', 'manufactured_by'),
    ('AMD', 'TSMC', 'manufactured_by'),
    ('Google', 'NVIDIA', 'procures_from'),
    ('Microsoft', 'NVIDIA', 'procures_from'),
    ('Amazon', 'NVIDIA', 'procures_from'),
    ('Meta', 'NVIDIA', 'procures_from'),
    ('Tesla', 'NVIDIA', 'procures_from'),
    ('Tesla', 'Panasonic', 'procures_from'),

    # Competitors
    ('Apple', 'Microsoft', 'competes_with'),
    ('Apple', 'Google', 'competes_with'),
    ('Apple', 'Samsung', 'competes_with'),
    ('Microsoft', 'Google', 'competes_with'),
    ('NVIDIA', 'AMD', 'competes_with'),
    ('Intel', 'AMD', 'competes_with'),
    ('Netflix', 'Disney', 'competes_with'),
    ('Netflix', 'Amazon', 'competes_with'),
    ('Uber', 'Lyft', 'competes_with'),
    ('Coca-Cola', 'PepsiCo', 'competes_with'),
    ('Boeing', 'Airbus', 'competes_with'),
    ('Toyota', 'Tesla', 'competes_with'),
    ('Walmart', 'Amazon', 'competes_with'),
    ('Goldman Sachs', 'Morgan Stanley', 'competes_with'),
    ('JPMorgan', 'Bank of America', 'competes_with'),
    ('Visa', 'Mastercard', 'competes_with'),

    # Investments & Acquisitions
    ('Microsoft', 'OpenAI', 'invests_in'),
    ('Google', 'DeepMind', 'owns'),
    ('Amazon', 'Anthropic', 'invests_in'),
    ('Microsoft', 'LinkedIn', 'owns'),
    ('Microsoft', 'GitHub', 'owns'),
    ('Google', 'YouTube', 'owns'),
    ('Meta', 'Instagram', 'owns'),
    ('Meta', 'WhatsApp', 'owns'),
    ('Amazon', 'Twitch', 'owns'),
    ('Amazon', 'MGM', 'owns'),
    ('Disney', 'Marvel', 'owns'),
    ('Disney', 'Pixar', 'owns'),
    ('Salesforce', 'Slack', 'owns'),
    ('Berkshire Hathaway', 'Apple', 'invests_in'),
    ('Berkshire Hathaway', 'Coca-Cola', 'invests_in'),
    ('Adobe', 'Figma', 'owns'),

    # Partnerships
    ('Apple', 'Goldman Sachs', 'partner_of'),
    ('Google', 'Samsung', 'partner_of'),
    ('Microsoft', 'SAP', 'partner_of'),

    # HQ Locations
    ('Apple', 'California', 'headquartered_in'),
    ('Google', 'California', 'headquartered_in'),
    ('Meta', 'California', 'headquartered_in'),
    ('NVIDIA', 'California', 'headquartered_in'),
    ('Microsoft', 'Washington', 'headquartered_in'),
    ('Amazon', 'Washington', 'headquartered_in'),
    ('Tesla', 'Texas', 'headquartered_in'),
    ('Toyota', 'Japan', 'headquartered_in'),
    ('Samsung', 'South Korea', 'headquartered_in'),
    ('TSMC', 'Taiwan', 'headquartered_in'),
    ('Alibaba', 'China', 'headquartered_in'),
    ('Volkswagen', 'Germany', 'headquartered_in'),

    # Industry affiliations
    ('Apple', 'Technology', 'operates_in'),
    ('NVIDIA', 'Semiconductors', 'operates_in'),
    ('Intel', 'Semiconductors', 'operates_in'),
    ('AMD', 'Semiconductors', 'operates_in'),
    ('Goldman Sachs', 'Finance', 'operates_in'),
    ('JPMorgan', 'Finance', 'operates_in'),
    ('Toyota', 'Automotive', 'operates_in'),
    ('Tesla', 'Automotive', 'operates_in'),
    ('Boeing', 'Aerospace', 'operates_in'),
    ('Pfizer', 'Healthcare', 'operates_in'),
    ('ExxonMobil', 'Energy', 'operates_in'),

    # Regulatory
    ('Google', 'DOJ', 'investigated_by'),
    ('Meta', 'FTC', 'investigated_by'),
    ('Apple', 'EU', 'regulated_by'),
    ('Amazon', 'EU', 'regulated_by'),
]

print(f'Adding {len(relationships)} connections...\n')

total_added = 0
total_merged = 0

# Process in chunks of 5 with delays
chunk_size = 5
for i in range(0, len(relationships), chunk_size):
    chunk = relationships[i:i+chunk_size]
    connections = []
    for from_name, to_name, rel_type in chunk:
        connections.append({
            'from_name': from_name,
            'to_name': to_name,
            'relationship': rel_type,
            'confidence': 0.95,
            'source': 'industry_knowledge'
        })

    payload = {'connections': connections}

    for attempt in range(3):
        try:
            resp = requests.post(f'{API}/ingest/bulk', headers=headers, json=payload, timeout=60)
            if resp.status_code == 503:
                print(f'Chunk {i//chunk_size + 1}: 503 - retry {attempt+1}/3')
                time.sleep(5)
                continue

            data = resp.json()
            added = data.get('connections', {}).get('added', 0)
            merged = data.get('connections', {}).get('merged', 0)
            total_added += added
            total_merged += merged

            # Print what was added
            for from_name, to_name, rel_type in chunk:
                print(f'  {from_name} --[{rel_type}]--> {to_name}')
            print(f'  Chunk {i//chunk_size + 1}: +{added} new, {merged} merged')
            break
        except Exception as e:
            print(f'Chunk {i//chunk_size + 1} error: {e}')
            time.sleep(3)

    time.sleep(2)

print(f'\n{"="*50}')
print(f'Total added: {total_added}')
print(f'Total merged: {total_merged}')

# Final stats
try:
    resp = requests.get(f'{API}/stats', headers=headers)
    data = resp.json()
    print(f'\nGraph stats:')
    print(f'  Entities: {data["total_entities"]}')
    print(f'  Relationships: {data["total_relationships"]}')
except Exception as e:
    print(f'Stats error: {e}')
