#!/usr/bin/env python3
"""Add massive batch of relationships to the knowledge graph."""
import requests
import json
import time

API = 'https://forage-graph-production.up.railway.app'
SECRET = '6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959'
headers = {'Authorization': f'Bearer {SECRET}', 'Content-Type': 'application/json'}

# Supply chain relationships
supply_chain = [
    ('Apple', 'TSMC', 'procures_from'),
    ('Apple', 'Samsung', 'procures_from'),
    ('Apple', 'Foxconn', 'manufactured_by'),
    ('NVIDIA', 'TSMC', 'manufactured_by'),
    ('AMD', 'TSMC', 'manufactured_by'),
    ('Intel', 'Samsung', 'competes_with'),
    ('NVIDIA', 'AMD', 'competes_with'),
    ('Google', 'NVIDIA', 'procures_from'),
    ('Microsoft', 'NVIDIA', 'procures_from'),
    ('Amazon', 'NVIDIA', 'procures_from'),
    ('Meta', 'NVIDIA', 'procures_from'),
    ('Tesla', 'NVIDIA', 'procures_from'),
    ('Tesla', 'Panasonic', 'procures_from'),
    ('Tesla', 'LG', 'procures_from'),
    ('Apple', 'Corning', 'procures_from'),
    ('Samsung', 'Qualcomm', 'procures_from'),
]

# Financial relationships
financial = [
    ('Goldman Sachs', 'Morgan Stanley', 'competes_with'),
    ('JPMorgan', 'Bank of America', 'competes_with'),
    ('BlackRock', 'Vanguard', 'competes_with'),
    ('Visa', 'Mastercard', 'competes_with'),
    ('PayPal', 'Square', 'competes_with'),
    ('Coinbase', 'Binance', 'competes_with'),
    ('Goldman Sachs', 'Apple', 'partner_of'),
    ('Berkshire Hathaway', 'Apple', 'invests_in'),
    ('Berkshire Hathaway', 'Coca-Cola', 'invests_in'),
    ('Berkshire Hathaway', 'Bank of America', 'invests_in'),
]

# Tech partnerships and acquisitions
partnerships = [
    ('Microsoft', 'OpenAI', 'invests_in'),
    ('Google', 'DeepMind', 'owns'),
    ('Amazon', 'Anthropic', 'invests_in'),
    ('Apple', 'Goldman Sachs', 'partner_of'),
    ('Google', 'Samsung', 'partner_of'),
    ('Microsoft', 'SAP', 'partner_of'),
    ('Salesforce', 'Slack', 'owns'),
    ('Microsoft', 'LinkedIn', 'owns'),
    ('Microsoft', 'GitHub', 'owns'),
    ('Google', 'YouTube', 'owns'),
    ('Meta', 'Instagram', 'owns'),
    ('Meta', 'WhatsApp', 'owns'),
    ('Amazon', 'Twitch', 'owns'),
    ('Amazon', 'MGM', 'owns'),
    ('Disney', 'Marvel', 'owns'),
    ('Disney', 'Lucasfilm', 'owns'),
    ('Disney', 'Pixar', 'owns'),
    ('Sony', 'PlayStation', 'owns'),
    ('Microsoft', 'Xbox', 'owns'),
    ('Adobe', 'Figma', 'owns'),
    ('Salesforce', 'Tableau', 'owns'),
    ('Intel', 'Mobileye', 'owns'),
    ('NVIDIA', 'Mellanox', 'owns'),
    ('AMD', 'Xilinx', 'owns'),
    ('Broadcom', 'VMware', 'owns'),
]

# Industry affiliations
industries = [
    ('Apple', 'Technology', 'operates_in'),
    ('Microsoft', 'Technology', 'operates_in'),
    ('Google', 'Technology', 'operates_in'),
    ('Amazon', 'E-Commerce', 'operates_in'),
    ('Amazon', 'Cloud Computing', 'operates_in'),
    ('Netflix', 'Entertainment', 'operates_in'),
    ('Disney', 'Entertainment', 'operates_in'),
    ('Goldman Sachs', 'Finance', 'operates_in'),
    ('JPMorgan', 'Finance', 'operates_in'),
    ('Pfizer', 'Healthcare', 'operates_in'),
    ('Johnson & Johnson', 'Healthcare', 'operates_in'),
    ('ExxonMobil', 'Energy', 'operates_in'),
    ('Chevron', 'Energy', 'operates_in'),
    ('Toyota', 'Automotive', 'operates_in'),
    ('Tesla', 'Automotive', 'operates_in'),
    ('Boeing', 'Aerospace', 'operates_in'),
    ('Lockheed Martin', 'Defense', 'operates_in'),
    ('NVIDIA', 'Semiconductors', 'operates_in'),
    ('Intel', 'Semiconductors', 'operates_in'),
    ('AMD', 'Semiconductors', 'operates_in'),
    ('TSMC', 'Semiconductors', 'operates_in'),
    ('Samsung', 'Semiconductors', 'operates_in'),
    ('Qualcomm', 'Semiconductors', 'operates_in'),
    ('Broadcom', 'Semiconductors', 'operates_in'),
]

# Competitor relationships
competitors = [
    ('Apple', 'Microsoft', 'competes_with'),
    ('Apple', 'Google', 'competes_with'),
    ('Apple', 'Samsung', 'competes_with'),
    ('Microsoft', 'Google', 'competes_with'),
    ('Amazon', 'Microsoft', 'competes_with'),
    ('Amazon', 'Google', 'competes_with'),
    ('Netflix', 'Disney', 'competes_with'),
    ('Netflix', 'Amazon', 'competes_with'),
    ('Uber', 'Lyft', 'competes_with'),
    ('Airbnb', 'Booking Holdings', 'competes_with'),
    ('Spotify', 'Apple', 'competes_with'),
    ('Boeing', 'Airbus', 'competes_with'),
    ('Toyota', 'Volkswagen', 'competes_with'),
    ('Toyota', 'Tesla', 'competes_with'),
    ('Coca-Cola', 'PepsiCo', 'competes_with'),
    ('McDonalds', 'Burger King', 'competes_with'),
    ('Nike', 'Adidas', 'competes_with'),
    ('Walmart', 'Target', 'competes_with'),
    ('Walmart', 'Amazon', 'competes_with'),
    ('Oracle', 'SAP', 'competes_with'),
    ('Oracle', 'Salesforce', 'competes_with'),
    ('Salesforce', 'Microsoft', 'competes_with'),
    ('AWS', 'Azure', 'competes_with'),
    ('AWS', 'Google Cloud', 'competes_with'),
]

# Geographic relationships
locations = [
    ('Apple', 'California', 'headquartered_in'),
    ('Microsoft', 'Washington', 'headquartered_in'),
    ('Amazon', 'Washington', 'headquartered_in'),
    ('Google', 'California', 'headquartered_in'),
    ('Meta', 'California', 'headquartered_in'),
    ('Tesla', 'Texas', 'headquartered_in'),
    ('NVIDIA', 'California', 'headquartered_in'),
    ('Intel', 'California', 'headquartered_in'),
    ('Toyota', 'Japan', 'headquartered_in'),
    ('Samsung', 'South Korea', 'headquartered_in'),
    ('TSMC', 'Taiwan', 'headquartered_in'),
    ('Alibaba', 'China', 'headquartered_in'),
    ('Tencent', 'China', 'headquartered_in'),
    ('Volkswagen', 'Germany', 'headquartered_in'),
    ('Airbus', 'France', 'headquartered_in'),
]

# Regulatory and legal
regulatory = [
    ('Google', 'DOJ', 'investigated_by'),
    ('Meta', 'FTC', 'investigated_by'),
    ('Amazon', 'EU', 'regulated_by'),
    ('Apple', 'EU', 'regulated_by'),
    ('Microsoft', 'DOJ', 'investigated_by'),
]

# Combine all relationships
all_rels = supply_chain + financial + partnerships + industries + competitors + locations + regulatory

# Build the connections payload
connections = []
for from_name, to_name, rel_type in all_rels:
    connections.append({
        'from_name': from_name,
        'to_name': to_name,
        'relationship': rel_type,
        'confidence': 0.95,
        'source': 'industry_knowledge'
    })

# Entities that might be missing
missing_entities = [
    ('OpenAI', 'Corporation'),
    ('Anthropic', 'Corporation'),
    ('DeepMind', 'Corporation'),
    ('Foxconn', 'Corporation'),
    ('Panasonic', 'Corporation'),
    ('LG', 'Corporation'),
    ('Corning', 'Corporation'),
    ('SAP', 'Corporation'),
    ('Slack', 'Corporation'),
    ('LinkedIn', 'Corporation'),
    ('GitHub', 'Corporation'),
    ('YouTube', 'Corporation'),
    ('Instagram', 'Corporation'),
    ('WhatsApp', 'Corporation'),
    ('Twitch', 'Corporation'),
    ('MGM', 'Corporation'),
    ('Marvel', 'Corporation'),
    ('Lucasfilm', 'Corporation'),
    ('Pixar', 'Corporation'),
    ('PlayStation', 'Corporation'),
    ('Xbox', 'Corporation'),
    ('Lyft', 'Corporation'),
    ('Booking Holdings', 'Corporation'),
    ('Airbus', 'Corporation'),
    ('Volkswagen', 'Corporation'),
    ('PepsiCo', 'Corporation'),
    ('Burger King', 'Corporation'),
    ('Adidas', 'Corporation'),
    ('Target', 'Corporation'),
    ('Figma', 'Corporation'),
    ('Tableau', 'Corporation'),
    ('Mobileye', 'Corporation'),
    ('Mellanox', 'Corporation'),
    ('Xilinx', 'Corporation'),
    ('VMware', 'Corporation'),
    ('AWS', 'Corporation'),
    ('Azure', 'Corporation'),
    ('Google Cloud', 'Corporation'),
    ('DOJ', 'Government'),
    ('FTC', 'Government'),
    ('EU', 'Government'),
    ('Technology', 'Industry'),
    ('E-Commerce', 'Industry'),
    ('Cloud Computing', 'Industry'),
    ('Entertainment', 'Industry'),
    ('Finance', 'Industry'),
    ('Healthcare', 'Industry'),
    ('Energy', 'Industry'),
    ('Automotive', 'Industry'),
    ('Aerospace', 'Industry'),
    ('Defense', 'Industry'),
    ('Semiconductors', 'Industry'),
    ('California', 'Location'),
    ('Washington', 'Location'),
    ('Texas', 'Location'),
    ('Japan', 'Location'),
    ('South Korea', 'Location'),
    ('Taiwan', 'Location'),
    ('China', 'Location'),
    ('Germany', 'Location'),
    ('France', 'Location'),
]

entities = []
for name, etype in missing_entities:
    entities.append({
        'type': etype,
        'name': name,
        'source': 'industry_knowledge',
        'confidence': 0.95
    })

print(f'Adding {len(entities)} entities and {len(connections)} connections...')
print('Using longer delays to avoid 503 errors...\n')

# Post entities in smaller batches
entity_chunk = 10
for i in range(0, len(entities), entity_chunk):
    chunk = entities[i:i+entity_chunk]
    payload = {'entities': chunk}
    for attempt in range(3):
        try:
            resp = requests.post(f'{API}/ingest/bulk', headers=headers, json=payload, timeout=60)
            if resp.status_code == 503:
                print(f'Entities {i+1}-{i+len(chunk)}: 503 - retrying in 5s...')
                time.sleep(5)
                continue
            data = resp.json()
            added = data.get('added', 0)
            print(f'Entities {i+1}-{i+len(chunk)}: +{added}')
            break
        except Exception as e:
            print(f'Entities {i+1}-{i+len(chunk)} error: {e}')
            time.sleep(3)
    time.sleep(2)

# Post connections in small chunks with delays
chunk_size = 8
total_added = 0
for i in range(0, len(connections), chunk_size):
    chunk = connections[i:i+chunk_size]
    payload = {'connections': chunk}
    for attempt in range(3):
        try:
            resp = requests.post(f'{API}/ingest/bulk', headers=headers, json=payload, timeout=60)
            if resp.status_code == 503:
                print(f'Connections {i+1}-{i+len(chunk)}: 503 - retrying in 5s...')
                time.sleep(5)
                continue
            if resp.status_code == 200 or resp.status_code == 201:
                data = resp.json()
                added = data.get('connections_added', 0)
                total_added += added
                print(f'Connections {i+1}-{i+len(chunk)}: +{added} (total: {total_added})')
            else:
                print(f'Connections {i+1}-{i+len(chunk)}: {resp.status_code}')
            break
        except Exception as e:
            print(f'Connections {i+1}-{i+len(chunk)} error: {e}')
            time.sleep(3)
    time.sleep(2)

# Check final stats
print('\n' + '='*50)
print('Final stats:')
try:
    resp = requests.get(f'{API}/stats', headers=headers)
    data = resp.json()
    print(f"Entities: {data.get('total_entities', 0)}")
    print(f"Relationships: {data.get('total_relationships', 0)}")
    print(f"By type: {data.get('entities_by_type', {})}")
except Exception as e:
    print(f'Stats error: {e}')
