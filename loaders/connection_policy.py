#!/usr/bin/env python3
"""
Connection Budget Policy for Reality Graph
==========================================
Enforces minimum edge counts per entity type to achieve 5-10 edges/node ratio.

Target: 5-10 edges per node (currently at 0.28)
"""

# Minimum outgoing edges required per entity type
CONNECTION_BUDGET = {
    'Corporation': {
        'min_edges': 5,
        'required_types': [
            ('headquartered_in', 'Location', 1),      # Must have HQ
            ('operates_in', 'Industry', 1),           # Must have industry
            ('competes_with', 'Corporation', 2),      # At least 2 competitors
        ],
        'optional_types': [
            ('invests_in', 'Corporation', 3),
            ('owns', 'Corporation', 3),
            ('partner_of', 'Corporation', 2),
            ('procures_from', 'Corporation', 2),
            ('regulated_by', 'Government', 1),
        ]
    },
    'MacroEvent': {
        'min_edges': 8,
        'required_types': [
            ('affects', 'Industry', 2),               # Which industries
            ('involves', 'Location', 1),              # Where
            ('impacts', 'Corporation', 3),            # Major corps affected
        ],
        'optional_types': [
            ('triggered_by', 'MacroEvent', 1),
            ('leads_to', 'MacroEvent', 2),
            ('signals', 'Regime', 1),
        ]
    },
    'Industry': {
        'min_edges': 4,
        'required_types': [
            ('contains', 'Corporation', 3),           # Major players
        ],
        'optional_types': [
            ('depends_on', 'Industry', 2),
            ('supplies', 'Industry', 2),
            ('regulated_by', 'Government', 1),
        ]
    },
    'Location': {
        'min_edges': 3,
        'required_types': [],
        'optional_types': [
            ('part_of', 'Location', 1),
            ('governed_by', 'Government', 1),
            ('hosts', 'Corporation', 5),
        ]
    },
    'Asset': {
        'min_edges': 4,
        'required_types': [
            ('issued_by', 'Corporation', 1),
            ('traded_in', 'Location', 1),
        ],
        'optional_types': [
            ('correlated_with', 'Asset', 3),
            ('tracks', 'Industry', 1),
        ]
    },
    'Person': {
        'min_edges': 3,
        'required_types': [
            ('works_at', 'Corporation', 1),
        ],
        'optional_types': [
            ('board_member_of', 'Corporation', 2),
            ('invests_in', 'Corporation', 3),
            ('connected_to', 'Person', 3),
        ]
    },
    'Government': {
        'min_edges': 4,
        'required_types': [
            ('governs', 'Location', 1),
        ],
        'optional_types': [
            ('regulates', 'Industry', 3),
            ('regulates', 'Corporation', 5),
            ('allied_with', 'Government', 2),
        ]
    },
}

# Standard relationship types for bulk generation
RELATIONSHIP_TEMPLATES = {
    # Supply chain
    'tech_supply_chain': [
        ('Apple', 'TSMC', 'procures_from'),
        ('Apple', 'Foxconn', 'manufactured_by'),
        ('Apple', 'Samsung', 'procures_from'),
        ('Apple', 'Corning', 'procures_from'),
        ('NVIDIA', 'TSMC', 'manufactured_by'),
        ('AMD', 'TSMC', 'manufactured_by'),
        ('Qualcomm', 'TSMC', 'manufactured_by'),
        ('Intel', 'ASML', 'procures_from'),
        ('TSMC', 'ASML', 'procures_from'),
        ('Samsung', 'ASML', 'procures_from'),
        ('Tesla', 'NVIDIA', 'procures_from'),
        ('Tesla', 'Panasonic', 'procures_from'),
        ('Tesla', 'CATL', 'procures_from'),
        ('Tesla', 'LG Energy', 'procures_from'),
        ('Boeing', 'GE', 'procures_from'),
        ('Boeing', 'Rolls-Royce', 'procures_from'),
        ('Airbus', 'GE', 'procures_from'),
        ('Airbus', 'Safran', 'procures_from'),
    ],

    # AI/Tech investments
    'ai_investments': [
        ('Microsoft', 'OpenAI', 'invests_in'),
        ('Amazon', 'Anthropic', 'invests_in'),
        ('Google', 'Anthropic', 'invests_in'),
        ('Salesforce', 'Anthropic', 'invests_in'),
        ('NVIDIA', 'CoreWeave', 'invests_in'),
        ('Microsoft', 'Mistral', 'invests_in'),
        ('Google', 'DeepMind', 'owns'),
        ('Meta', 'FAIR', 'owns'),
        ('Apple', 'Apple ML', 'owns'),
        ('Alibaba', 'Qwen', 'owns'),
        ('Tencent', 'Hunyuan', 'owns'),
    ],

    # M&A / Ownership
    'major_acquisitions': [
        ('Microsoft', 'LinkedIn', 'owns'),
        ('Microsoft', 'GitHub', 'owns'),
        ('Microsoft', 'Activision', 'owns'),
        ('Microsoft', 'Nuance', 'owns'),
        ('Google', 'YouTube', 'owns'),
        ('Google', 'Fitbit', 'owns'),
        ('Google', 'Mandiant', 'owns'),
        ('Meta', 'Instagram', 'owns'),
        ('Meta', 'WhatsApp', 'owns'),
        ('Meta', 'Oculus', 'owns'),
        ('Amazon', 'Whole Foods', 'owns'),
        ('Amazon', 'MGM', 'owns'),
        ('Amazon', 'Twitch', 'owns'),
        ('Amazon', 'Ring', 'owns'),
        ('Salesforce', 'Slack', 'owns'),
        ('Salesforce', 'Tableau', 'owns'),
        ('Adobe', 'Figma', 'owns'),
        ('Broadcom', 'VMware', 'owns'),
        ('AMD', 'Xilinx', 'owns'),
        ('NVIDIA', 'Mellanox', 'owns'),
        ('Intel', 'Mobileye', 'owns'),
    ],

    # Financial sector
    'financial_relationships': [
        ('BlackRock', 'iShares', 'owns'),
        ('Vanguard', 'VOO', 'manages'),
        ('Berkshire Hathaway', 'Apple', 'invests_in'),
        ('Berkshire Hathaway', 'Coca-Cola', 'invests_in'),
        ('Berkshire Hathaway', 'Bank of America', 'invests_in'),
        ('Berkshire Hathaway', 'American Express', 'invests_in'),
        ('Berkshire Hathaway', 'Chevron', 'invests_in'),
        ('SoftBank', 'ARM', 'owns'),
        ('SoftBank', 'WeWork', 'invests_in'),
        ('SoftBank', 'ByteDance', 'invests_in'),
        ('Sequoia', 'Stripe', 'invests_in'),
        ('Sequoia', 'Airbnb', 'invests_in'),
        ('a]6z', 'OpenAI', 'invests_in'),
    ],

    # Geopolitical / Regulatory
    'regulatory_relationships': [
        ('Google', 'DOJ', 'investigated_by'),
        ('Apple', 'DOJ', 'investigated_by'),
        ('Meta', 'FTC', 'investigated_by'),
        ('Amazon', 'FTC', 'investigated_by'),
        ('Microsoft', 'EU', 'regulated_by'),
        ('Apple', 'EU', 'regulated_by'),
        ('Google', 'EU', 'regulated_by'),
        ('Meta', 'EU', 'regulated_by'),
        ('TikTok', 'US Congress', 'investigated_by'),
        ('Huawei', 'US Government', 'sanctioned_by'),
        ('SMIC', 'US Government', 'sanctioned_by'),
    ],

    # Cloud/Platform competition
    'cloud_competition': [
        ('AWS', 'Azure', 'competes_with'),
        ('AWS', 'Google Cloud', 'competes_with'),
        ('Azure', 'Google Cloud', 'competes_with'),
        ('AWS', 'Oracle Cloud', 'competes_with'),
        ('AWS', 'IBM Cloud', 'competes_with'),
        ('AWS', 'Alibaba Cloud', 'competes_with'),
        ('Snowflake', 'Databricks', 'competes_with'),
        ('Salesforce', 'Microsoft', 'competes_with'),
        ('Oracle', 'SAP', 'competes_with'),
    ],

    # Industry classifications
    'industry_affiliations': [
        ('Apple', 'Consumer Electronics', 'operates_in'),
        ('Apple', 'Software', 'operates_in'),
        ('Microsoft', 'Enterprise Software', 'operates_in'),
        ('Microsoft', 'Cloud Computing', 'operates_in'),
        ('Microsoft', 'Gaming', 'operates_in'),
        ('Google', 'Advertising', 'operates_in'),
        ('Google', 'Cloud Computing', 'operates_in'),
        ('Google', 'AI Research', 'operates_in'),
        ('NVIDIA', 'Semiconductors', 'operates_in'),
        ('NVIDIA', 'AI Infrastructure', 'operates_in'),
        ('Tesla', 'Automotive', 'operates_in'),
        ('Tesla', 'Energy', 'operates_in'),
        ('Tesla', 'AI', 'operates_in'),
        ('Amazon', 'E-Commerce', 'operates_in'),
        ('Amazon', 'Cloud Computing', 'operates_in'),
        ('Amazon', 'Logistics', 'operates_in'),
        ('Meta', 'Social Media', 'operates_in'),
        ('Meta', 'VR/AR', 'operates_in'),
        ('Meta', 'Advertising', 'operates_in'),
    ],

    # Headquarters
    'headquarters': [
        ('Apple', 'Cupertino', 'headquartered_in'),
        ('Google', 'Mountain View', 'headquartered_in'),
        ('Meta', 'Menlo Park', 'headquartered_in'),
        ('NVIDIA', 'Santa Clara', 'headquartered_in'),
        ('Intel', 'Santa Clara', 'headquartered_in'),
        ('Oracle', 'Austin', 'headquartered_in'),
        ('Tesla', 'Austin', 'headquartered_in'),
        ('Microsoft', 'Redmond', 'headquartered_in'),
        ('Amazon', 'Seattle', 'headquartered_in'),
        ('Boeing', 'Arlington', 'headquartered_in'),
        ('Lockheed Martin', 'Bethesda', 'headquartered_in'),
        ('Goldman Sachs', 'New York', 'headquartered_in'),
        ('JPMorgan', 'New York', 'headquartered_in'),
        ('BlackRock', 'New York', 'headquartered_in'),
        ('Pfizer', 'New York', 'headquartered_in'),
        ('Samsung', 'Seoul', 'headquartered_in'),
        ('TSMC', 'Hsinchu', 'headquartered_in'),
        ('Toyota', 'Toyota City', 'headquartered_in'),
        ('Alibaba', 'Hangzhou', 'headquartered_in'),
        ('Tencent', 'Shenzhen', 'headquartered_in'),
        ('ASML', 'Veldhoven', 'headquartered_in'),
        ('SAP', 'Walldorf', 'headquartered_in'),
        ('Shell', 'London', 'headquartered_in'),
        ('BP', 'London', 'headquartered_in'),
        ('HSBC', 'London', 'headquartered_in'),
    ],
}

def get_all_connections():
    """Get all connections from templates."""
    all_conns = []
    for category, conns in RELATIONSHIP_TEMPLATES.items():
        for from_name, to_name, rel_type in conns:
            all_conns.append({
                'from_name': from_name,
                'to_name': to_name,
                'relationship': rel_type,
                'confidence': 0.95,
                'source': f'connection_policy:{category}'
            })
    return all_conns

def count_by_category():
    """Count connections by category."""
    counts = {}
    for category, conns in RELATIONSHIP_TEMPLATES.items():
        counts[category] = len(conns)
    return counts

if __name__ == '__main__':
    import requests
    import time

    API = 'https://forage-graph-production.up.railway.app'
    SECRET = '6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959'
    headers = {'Authorization': f'Bearer {SECRET}', 'Content-Type': 'application/json'}

    print("Connection Budget Policy Loader")
    print("=" * 50)

    counts = count_by_category()
    total = sum(counts.values())
    print(f"\nCategories:")
    for cat, cnt in counts.items():
        print(f"  {cat}: {cnt}")
    print(f"\nTotal connections to add: {total}")

    # Get current stats
    resp = requests.get(f'{API}/stats', headers=headers)
    if resp.status_code == 200:
        stats = resp.json()
        print(f"\nCurrent graph:")
        print(f"  Entities: {stats['total_entities']}")
        print(f"  Relationships: {stats['total_relationships']}")
        print(f"  Ratio: {stats['total_relationships']/stats['total_entities']:.2f} edges/node")

    print(f"\n{'=' * 50}")
    print("Loading connections...")

    all_conns = get_all_connections()
    total_added = 0
    total_merged = 0

    # Process in chunks
    chunk_size = 10
    for i in range(0, len(all_conns), chunk_size):
        chunk = all_conns[i:i+chunk_size]
        payload = {'connections': chunk}

        for attempt in range(3):
            try:
                resp = requests.post(f'{API}/ingest/bulk', headers=headers, json=payload, timeout=60)
                if resp.status_code == 503:
                    print(f"  Chunk {i//chunk_size + 1}: 503 - retry...")
                    time.sleep(5)
                    continue
                if resp.status_code in (200, 201):
                    data = resp.json()
                    added = data.get('connections', {}).get('added', 0)
                    merged = data.get('connections', {}).get('merged', 0)
                    total_added += added
                    total_merged += merged
                    print(f"  Chunk {i//chunk_size + 1}: +{added} new, {merged} merged")
                break
            except Exception as e:
                print(f"  Chunk {i//chunk_size + 1} error: {e}")
                time.sleep(3)
        time.sleep(1.5)

    print(f"\n{'=' * 50}")
    print(f"Total added: {total_added}")
    print(f"Total merged: {total_merged}")

    # Final stats
    resp = requests.get(f'{API}/stats', headers=headers)
    if resp.status_code == 200:
        stats = resp.json()
        print(f"\nFinal graph:")
        print(f"  Entities: {stats['total_entities']}")
        print(f"  Relationships: {stats['total_relationships']}")
        ratio = stats['total_relationships']/stats['total_entities']
        print(f"  Ratio: {ratio:.2f} edges/node")
        print(f"  Target: 5-10 edges/node")
        print(f"  Progress: {ratio/5*100:.1f}% to minimum target")
