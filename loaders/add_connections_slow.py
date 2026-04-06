#!/usr/bin/env python3
"""Add connections one at a time with longer delays."""
import os
import requests
import time

API = os.environ.get('GRAPH_API_URL', 'https://forage-graph-production.up.railway.app')
SECRET = os.environ['GRAPH_API_SECRET']
headers = {'Authorization': f'Bearer {SECRET}', 'Content-Type': 'application/json'}

# Key high-value relationships
connections = [
    # Supply chain
    ('Apple', 'TSMC', 'procures_from'),
    ('Apple', 'Foxconn', 'manufactured_by'),
    ('NVIDIA', 'TSMC', 'manufactured_by'),
    ('Google', 'NVIDIA', 'procures_from'),
    ('Microsoft', 'NVIDIA', 'procures_from'),
    ('Tesla', 'NVIDIA', 'procures_from'),
    # Acquisitions
    ('Microsoft', 'OpenAI', 'invests_in'),
    ('Google', 'DeepMind', 'owns'),
    ('Amazon', 'Anthropic', 'invests_in'),
    ('Microsoft', 'LinkedIn', 'owns'),
    ('Microsoft', 'GitHub', 'owns'),
    ('Google', 'YouTube', 'owns'),
    ('Meta', 'Instagram', 'owns'),
    ('Meta', 'WhatsApp', 'owns'),
    ('Amazon', 'Twitch', 'owns'),
    ('Disney', 'Marvel', 'owns'),
    ('Disney', 'Pixar', 'owns'),
    ('Salesforce', 'Slack', 'owns'),
    # Competitors
    ('Apple', 'Microsoft', 'competes_with'),
    ('Apple', 'Google', 'competes_with'),
    ('Apple', 'Samsung', 'competes_with'),
    ('Microsoft', 'Google', 'competes_with'),
    ('NVIDIA', 'AMD', 'competes_with'),
    ('Netflix', 'Disney', 'competes_with'),
    ('Uber', 'Lyft', 'competes_with'),
    ('Coca-Cola', 'PepsiCo', 'competes_with'),
    ('Boeing', 'Airbus', 'competes_with'),
    ('Toyota', 'Tesla', 'competes_with'),
    ('Nike', 'Adidas', 'competes_with'),
    ('Walmart', 'Amazon', 'competes_with'),
    # Industry affiliations
    ('Apple', 'Technology', 'operates_in'),
    ('NVIDIA', 'Semiconductors', 'operates_in'),
    ('Goldman Sachs', 'Finance', 'operates_in'),
    ('Toyota', 'Automotive', 'operates_in'),
    ('Boeing', 'Aerospace', 'operates_in'),
    # HQ locations
    ('Apple', 'California', 'headquartered_in'),
    ('Microsoft', 'Washington', 'headquartered_in'),
    ('Toyota', 'Japan', 'headquartered_in'),
    ('Samsung', 'South Korea', 'headquartered_in'),
    ('TSMC', 'Taiwan', 'headquartered_in'),
]

added = 0
failed = 0

print(f'Adding {len(connections)} priority connections...')
print('One at a time with 3s delays...\n')

for from_name, to_name, rel_type in connections:
    conn = {
        'from_name': from_name,
        'to_name': to_name,
        'relationship': rel_type,
        'confidence': 0.95,
        'source': 'industry_knowledge'
    }
    payload = {'connections': [conn]}

    success = False
    for attempt in range(5):
        try:
            resp = requests.post(f'{API}/ingest/bulk', headers=headers, json=payload, timeout=30)
            if resp.status_code == 503:
                print(f'{from_name} -> {to_name}: 503, retry {attempt+1}/5...')
                time.sleep(5)
                continue
            if resp.status_code in (200, 201):
                data = resp.json()
                n = data.get('connections_added', 0)
                if n > 0:
                    added += 1
                    print(f'+ {from_name} --[{rel_type}]--> {to_name}')
                else:
                    print(f'  {from_name} --[{rel_type}]--> {to_name} (already exists)')
                success = True
                break
            else:
                print(f'{from_name} -> {to_name}: {resp.status_code}')
                break
        except Exception as e:
            print(f'{from_name} -> {to_name} error: {e}')
            time.sleep(3)

    if not success:
        failed += 1

    time.sleep(3)

print(f'\n{"="*50}')
print(f'Added: {added}')
print(f'Failed: {failed}')

# Final stats
try:
    resp = requests.get(f'{API}/stats', headers=headers)
    data = resp.json()
    print(f'Total entities: {data["total_entities"]}')
    print(f'Total relationships: {data["total_relationships"]}')
except Exception as e:
    print(f'Stats error: {e}')
