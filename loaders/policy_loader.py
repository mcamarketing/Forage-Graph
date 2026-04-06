#!/usr/bin/env python3
"""
Policy-Driven Connection Loader
================================
Systematically adds connections based on graph_policy.py requirements.
Uses very slow rate limiting (5s between requests) to avoid 503s.
"""
import os
import requests
import time

API = os.environ.get('GRAPH_API_URL', 'https://forage-graph-production.up.railway.app')
SECRET = os.environ['GRAPH_API_SECRET']
headers = {'Authorization': f'Bearer {SECRET}', 'Content-Type': 'application/json'}

# Corporation -> Industry mappings (operates_in)
CORP_INDUSTRIES = [
    ('Apple', 'Consumer Electronics'),
    ('Apple', 'Software'),
    ('Apple', 'Services'),
    ('Microsoft', 'Enterprise Software'),
    ('Microsoft', 'Cloud Computing'),
    ('Microsoft', 'Gaming'),
    ('Google', 'Advertising'),
    ('Google', 'Cloud Computing'),
    ('Google', 'AI'),
    ('Amazon', 'E-Commerce'),
    ('Amazon', 'Cloud Computing'),
    ('Amazon', 'Logistics'),
    ('Meta', 'Social Media'),
    ('Meta', 'Advertising'),
    ('Meta', 'VR/AR'),
    ('NVIDIA', 'Semiconductors'),
    ('NVIDIA', 'AI Infrastructure'),
    ('NVIDIA', 'Data Center'),
    ('Intel', 'Semiconductors'),
    ('Intel', 'Data Center'),
    ('AMD', 'Semiconductors'),
    ('AMD', 'Gaming'),
    ('Tesla', 'Automotive'),
    ('Tesla', 'Energy'),
    ('Tesla', 'AI'),
    ('Toyota', 'Automotive'),
    ('Toyota', 'Manufacturing'),
    ('Boeing', 'Aerospace'),
    ('Boeing', 'Defense'),
    ('JPMorgan', 'Banking'),
    ('JPMorgan', 'Asset Management'),
    ('Goldman Sachs', 'Investment Banking'),
    ('Goldman Sachs', 'Trading'),
    ('Netflix', 'Streaming'),
    ('Netflix', 'Entertainment'),
    ('Disney', 'Entertainment'),
    ('Disney', 'Streaming'),
    ('Disney', 'Theme Parks'),
    ('Uber', 'Transportation'),
    ('Uber', 'Food Delivery'),
    ('Pfizer', 'Pharmaceuticals'),
    ('Pfizer', 'Biotechnology'),
    ('ExxonMobil', 'Energy'),
    ('ExxonMobil', 'Oil & Gas'),
    ('Visa', 'Payments'),
    ('Visa', 'Financial Services'),
    ('Walmart', 'Retail'),
    ('Walmart', 'E-Commerce'),
    ('Coca-Cola', 'Beverages'),
    ('Coca-Cola', 'Consumer Goods'),
    ('Nike', 'Apparel'),
    ('Nike', 'Footwear'),
]

# Person -> Corporation (works_at, founded, board_member_of)
PERSON_CORP = [
    ('Tim Cook', 'Apple', 'works_at'),
    ('Satya Nadella', 'Microsoft', 'works_at'),
    ('Sundar Pichai', 'Google', 'works_at'),
    ('Andy Jassy', 'Amazon', 'works_at'),
    ('Mark Zuckerberg', 'Meta', 'works_at'),
    ('Jensen Huang', 'NVIDIA', 'works_at'),
    ('Elon Musk', 'Tesla', 'works_at'),
    ('Elon Musk', 'SpaceX', 'works_at'),
    ('Warren Buffett', 'Berkshire Hathaway', 'works_at'),
    ('Jamie Dimon', 'JPMorgan', 'works_at'),
    ('Sam Altman', 'OpenAI', 'works_at'),
    ('Dario Amodei', 'Anthropic', 'works_at'),
    ('Jensen Huang', 'NVIDIA', 'founded'),
    ('Mark Zuckerberg', 'Meta', 'founded'),
    ('Elon Musk', 'Tesla', 'founded'),
    ('Elon Musk', 'SpaceX', 'founded'),
    ('Jeff Bezos', 'Amazon', 'founded'),
    ('Jeff Bezos', 'Blue Origin', 'founded'),
    ('Larry Page', 'Google', 'founded'),
    ('Sergey Brin', 'Google', 'founded'),
    ('Bill Gates', 'Microsoft', 'founded'),
    ('Steve Jobs', 'Apple', 'founded'),
    ('Warren Buffett', 'Apple', 'invests_in'),
    ('Warren Buffett', 'Coca-Cola', 'invests_in'),
    ('Warren Buffett', 'Bank of America', 'invests_in'),
]

# Asset -> Corporation (issued_by)
ASSET_CORP = [
    ('AAPL', 'Apple', 'issued_by'),
    ('MSFT', 'Microsoft', 'issued_by'),
    ('GOOGL', 'Google', 'issued_by'),
    ('AMZN', 'Amazon', 'issued_by'),
    ('META', 'Meta', 'issued_by'),
    ('NVDA', 'NVIDIA', 'issued_by'),
    ('TSLA', 'Tesla', 'issued_by'),
    ('JPM', 'JPMorgan', 'issued_by'),
    ('V', 'Visa', 'issued_by'),
    ('WMT', 'Walmart', 'issued_by'),
    ('NFLX', 'Netflix', 'issued_by'),
    ('DIS', 'Disney', 'issued_by'),
    ('KO', 'Coca-Cola', 'issued_by'),
    ('NKE', 'Nike', 'issued_by'),
    ('PFE', 'Pfizer', 'issued_by'),
    ('XOM', 'ExxonMobil', 'issued_by'),
    ('BA', 'Boeing', 'issued_by'),
    ('GS', 'Goldman Sachs', 'issued_by'),
    ('INTC', 'Intel', 'issued_by'),
    ('AMD', 'AMD', 'issued_by'),
]

# Industry dependencies
INDUSTRY_DEPS = [
    ('AI', 'Semiconductors', 'depends_on'),
    ('AI', 'Cloud Computing', 'depends_on'),
    ('Cloud Computing', 'Semiconductors', 'depends_on'),
    ('Cloud Computing', 'Data Center', 'depends_on'),
    ('E-Commerce', 'Logistics', 'depends_on'),
    ('E-Commerce', 'Payments', 'depends_on'),
    ('Automotive', 'Manufacturing', 'depends_on'),
    ('Automotive', 'Semiconductors', 'depends_on'),
    ('Streaming', 'Cloud Computing', 'depends_on'),
    ('Gaming', 'Semiconductors', 'depends_on'),
    ('Semiconductors', 'Manufacturing', 'supplies'),
    ('Semiconductors', 'AI Infrastructure', 'supplies'),
    ('Oil & Gas', 'Energy', 'supplies'),
    ('Banking', 'Financial Services', 'supplies'),
]

# Government regulations
GOV_REGS = [
    ('DOJ', 'Google', 'investigates'),
    ('DOJ', 'Apple', 'investigates'),
    ('FTC', 'Meta', 'investigates'),
    ('FTC', 'Amazon', 'investigates'),
    ('FTC', 'Microsoft', 'investigates'),
    ('EU', 'Apple', 'regulates'),
    ('EU', 'Google', 'regulates'),
    ('EU', 'Meta', 'regulates'),
    ('EU', 'Amazon', 'regulates'),
    ('EU', 'Microsoft', 'regulates'),
    ('SEC', 'JPMorgan', 'regulates'),
    ('SEC', 'Goldman Sachs', 'regulates'),
    ('FDA', 'Pfizer', 'regulates'),
    ('NHTSA', 'Tesla', 'regulates'),
    ('NHTSA', 'Toyota', 'regulates'),
    ('FAA', 'Boeing', 'regulates'),
    ('US Government', 'Huawei', 'sanctions'),
    ('US Government', 'SMIC', 'sanctions'),
]

# Asset correlations
ASSET_CORRELATIONS = [
    ('AAPL', 'MSFT', 'correlated_with'),
    ('GOOGL', 'META', 'correlated_with'),
    ('NVDA', 'AMD', 'correlated_with'),
    ('TSLA', 'RIVN', 'correlated_with'),
    ('JPM', 'GS', 'correlated_with'),
    ('XOM', 'CVX', 'correlated_with'),
    ('DIS', 'NFLX', 'correlated_with'),
    ('KO', 'PEP', 'correlated_with'),
    ('V', 'MA', 'correlated_with'),
    ('BA', 'LMT', 'correlated_with'),
]


def add_connection(from_name, to_name, rel_type, from_type='Corporation', to_type='Corporation', confidence=0.9):
    """Add a single connection with retry logic."""
    payload = {
        'connections': [{
            'from_name': from_name,
            'to_name': to_name,
            'from_type': from_type,
            'to_type': to_type,
            'relationship': rel_type,
            'confidence': confidence,
            'source': 'policy_loader'
        }]
    }

    for attempt in range(5):
        try:
            resp = requests.post(f'{API}/ingest/bulk', headers=headers, json=payload, timeout=30)
            if resp.status_code == 503:
                print(f"  {from_name} -> {to_name}: 503, waiting 10s...")
                time.sleep(10)
                continue
            if resp.status_code in (200, 201):
                data = resp.json()
                added = data.get('connections', {}).get('added', 0)
                if added > 0:
                    print(f"  {from_name} --[{rel_type}]--> {to_name}: ADDED")
                else:
                    print(f"  {from_name} --[{rel_type}]--> {to_name}: (exists)")
                return added
            else:
                print(f"  {from_name} -> {to_name}: {resp.status_code}")
                return 0
        except Exception as e:
            print(f"  {from_name} -> {to_name}: error {e}")
            time.sleep(5)
    return 0


def run():
    print("=" * 60)
    print("POLICY-DRIVEN CONNECTION LOADER")
    print("=" * 60)

    # Get current stats
    try:
        resp = requests.get(f'{API}/stats', headers=headers, timeout=10)
        if resp.status_code == 200:
            stats = resp.json()
            print(f"\nBefore: {stats['total_entities']} entities, {stats['total_relationships']} rels")
            print(f"Ratio: {stats['total_relationships']/stats['total_entities']:.2f} edges/node")
    except:
        pass

    total_added = 0

    # Corporation -> Industry (operates_in)
    print(f"\n{'='*60}")
    print(f"LOADING: Corporation -> Industry ({len(CORP_INDUSTRIES)})")
    print("-" * 60)
    for corp, industry in CORP_INDUSTRIES:
        added = add_connection(corp, industry, 'operates_in', 'Corporation', 'Industry')
        total_added += added
        time.sleep(3)

    # Person -> Corporation
    print(f"\n{'='*60}")
    print(f"LOADING: Person -> Corporation ({len(PERSON_CORP)})")
    print("-" * 60)
    for person, corp, rel in PERSON_CORP:
        added = add_connection(person, corp, rel, 'Person', 'Corporation')
        total_added += added
        time.sleep(3)

    # Asset -> Corporation (issued_by)
    print(f"\n{'='*60}")
    print(f"LOADING: Asset -> Corporation ({len(ASSET_CORP)})")
    print("-" * 60)
    for asset, corp, rel in ASSET_CORP:
        added = add_connection(asset, corp, rel, 'Asset', 'Corporation')
        total_added += added
        time.sleep(3)

    # Industry dependencies
    print(f"\n{'='*60}")
    print(f"LOADING: Industry Dependencies ({len(INDUSTRY_DEPS)})")
    print("-" * 60)
    for from_ind, to_ind, rel in INDUSTRY_DEPS:
        added = add_connection(from_ind, to_ind, rel, 'Industry', 'Industry')
        total_added += added
        time.sleep(3)

    # Government regulations
    print(f"\n{'='*60}")
    print(f"LOADING: Government Regulations ({len(GOV_REGS)})")
    print("-" * 60)
    for gov, target, rel in GOV_REGS:
        added = add_connection(gov, target, rel, 'Government', 'Corporation')
        total_added += added
        time.sleep(3)

    # Asset correlations
    print(f"\n{'='*60}")
    print(f"LOADING: Asset Correlations ({len(ASSET_CORRELATIONS)})")
    print("-" * 60)
    for from_asset, to_asset, rel in ASSET_CORRELATIONS:
        added = add_connection(from_asset, to_asset, rel, 'Asset', 'Asset')
        total_added += added
        time.sleep(3)

    # Final stats
    print(f"\n{'='*60}")
    print("SUMMARY")
    print("=" * 60)
    print(f"Total added: {total_added}")

    try:
        resp = requests.get(f'{API}/stats', headers=headers, timeout=10)
        if resp.status_code == 200:
            stats = resp.json()
            print(f"After: {stats['total_entities']} entities, {stats['total_relationships']} rels")
            ratio = stats['total_relationships'] / stats['total_entities']
            print(f"Ratio: {ratio:.2f} edges/node")
            print(f"Progress to 5 edges/node: {ratio/5*100:.1f}%")
    except:
        pass


if __name__ == '__main__':
    run()
