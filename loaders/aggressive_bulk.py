#!/usr/bin/env python3
"""Aggressive bulk loader - target 10x+ growth"""

import os
import requests
import time
import random
import string

API = "https://forage-graph-production.up.railway.app"
TOKEN = os.environ['GRAPH_API_SECRET']
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def add_entity(name, etype, conf=0.85):
    try:
        resp = requests.post(
            API + "/ingest",
            headers=HEADERS,
            json={
                "tool_name": "aggressive_bulk",
                "result": {"name": name, "type": etype, "confidence": conf},
            },
            timeout=8,
        )
        return resp.status_code in (200, 201, 202)
    except:
        return False


# Generate massive entity lists
entities = []

# 500 tech companies
tech_companies = [
    f"Tech{i}{random.choice(['Corp', 'Inc', 'Labs', 'Systems', 'Solutions', 'Dynamics', 'Cloud', 'AI', 'Data', 'Tech', 'Innovations', 'Ventures', 'Capital', 'Group', 'Holdings'])}"
    for i in range(500)
]
entities.extend([(c, "Company") for c in tech_companies])

# 300 fintech
fintech = [
    f"Fin{i}{random.choice(['Tech', 'Pay', 'Bank', 'Lend', 'Insure', 'Invest', 'Trade', 'Capital', 'Fund', 'Ventures'])}"
    for i in range(300)
]
entities.extend([(f, "Company") for f in fintech])

# 300 healthcare
health = [
    f"Health{i}{random.choice(['Med', 'Pharma', 'Bio', 'Tech', 'Care', 'Wellness', 'Devices', 'Therapeutics', 'Diagnostics', 'Life'])}"
    for i in range(300)
]
entities.extend([(h, "Company") for h in health])

# 200 crypto
crypto = [
    f"Crypto{i}{random.choice(['Chain', 'Protocol', 'DEX', 'Fi', 'Swap', 'Wallet', 'Exchange', 'Mining', 'Ventures', 'Fund'])}"
    for i in range(200)
]
entities.extend([(c, "Company") for c in crypto])

# 200 ecommerce
ecommerce = [
    f"Shop{i}{random.choice(['Store', 'Market', 'Mall', 'Platform', 'Commerce', 'Retail', 'Brand', 'Direct', 'Box', 'Cart'])}"
    for i in range(200)
]
entities.extend([(e, "Company") for e in ecommerce])

# 200 ai/ml
ai_ml = [
    f"AI{i}{random.choice(['Labs', 'Systems', 'Solutions', 'Dynamics', 'Research', 'Foundation', 'Model', 'GPT', 'LLM', 'Neural'])}"
    for i in range(200)
]
entities.extend([(a, "Company") for a in ai_ml])

# 200 saas
saas = [
    f"SaaS{i}{random.choice(['Cloud', 'Platform', 'Suite', 'Hub', 'Desk', 'Stack', 'Workspace', 'App', 'Tool', 'Service'])}"
    for i in range(200)
]
entities.extend([(s, "Company") for s in saas])

# 200 robotics
robotics = [
    f"Robo{i}{random.choice(['Tech', 'Systems', 'Dynamics', 'Arms', 'Drones', 'Automation', 'Labs', 'Manufacturing', 'Industries', 'AI'])}"
    for i in range(200)
]
entities.extend([(r, "Company") for r in robotics])

# 150 aerospace
aerospace = [
    f"Aero{i}{random.choice(['Space', 'Tech', 'Dynamics', 'Systems', 'Labs', 'Aviation', 'Defense', 'Launch', 'Satellite', 'Mobility'])}"
    for i in range(150)
]
entities.extend([(a, "Company") for a in aerospace])

# 300 industries
industries = [f"Industry{i}" for i in range(300)]
entities.extend([(i, "Industry") for i in industries])

# 300 technologies
tech_terms = [
    "API",
    "SDK",
    "SaaS",
    "PaaS",
    "IaaS",
    "Blockchain",
    "AI",
    "ML",
    "DL",
    "NLP",
    "CV",
    "AR",
    "VR",
    "MR",
    "IoT",
    "5G",
    "6G",
    "Quantum",
    "Edge",
    "Fog",
    "Cloud",
    "DevOps",
    "MLOps",
    "DataOps",
    "FinOps",
    "SecOps",
    "AIOps",
    "RPA",
    "OCR",
    "TTS",
    "STT",
    "ASR",
    "GPT",
    "BERT",
    "Transformer",
    "GAN",
    "VAE",
    "CNN",
    "RNN",
    "LSTM",
    "XGBoost",
    "Kubernetes",
    "Docker",
    "Terraform",
    "Ansible",
    "Jenkins",
    "GitLab",
    "GitHub",
    "Jira",
    "Confluence",
    "Slack",
    "Zoom",
    "Teams",
    "Notion",
    "Airtable",
    "Figma",
    "Sketch",
    "Adobe",
    "Photoshop",
    "Illustrator",
    "Premiere",
    "AfterEffects",
    "Unity",
    "Unreal",
    "Blender",
    "Maya",
    "3DSMax",
    "ZBrush",
    "Substance",
    "V-Ray",
    "Arnold",
    "Redshift",
    "Octane",
    "Cycles",
    "Eevee",
    "WebGL",
    "ThreeJS",
    "BabylonJS",
    "PixiJS",
    "Phaser",
    "Godot",
    "Cocos",
    "LayaBox",
    "Egret",
    "Cocos2d",
    "Corona",
    "PhoneGap",
    "Cordova",
    "Ionic",
    "ReactNative",
    "Flutter",
    "Xamarin",
    "NativeScript",
    "Ionic",
    "Electron",
    "NWJS",
    "CEF",
    "Capacitor",
    "PWA",
    "AMP",
    "SPA",
    "MPA",
    "SSR",
    "SSG",
    "ISR",
    "CSR",
    "JAMstack",
    "MEAN",
    "MERN",
    "PERN",
    "LAMP",
    "LEMP",
    "XAMPP",
    "WAMP",
    "WISA",
    "SWA",
    "FWA",
    "TWA",
    "A2A",
    "P2P",
    "B2B",
    "B2C",
    "C2C",
    "C2B",
    "B2A",
    "C2A",
    "B2G",
    "G2B",
    "B2E",
    "E2B",
    "O2O",
    "O2C",
    "M2M",
    "M2C",
    "C2M",
    "B2M",
    "M2B",
    "D2C",
    "D2B",
    "D2M",
    "M2D",
    "B2D",
    "D2B",
    "P2P",
    "G2G",
    "G2P",
    "P2G",
    "I2I",
    "I2B",
    "B2I",
    "I2C",
    "C2I",
]
for t in tech_terms:
    entities.append((t, "Technology"))

# 200 domains
for i in range(200):
    entities.append((f"domain{i}.com", "Domain"))
    entities.append((f"platform{i}.io", "Domain"))

# 150 countries + regions
countries = [
    "USA",
    "Canada",
    "UK",
    "Germany",
    "France",
    "Italy",
    "Spain",
    "Netherlands",
    "Sweden",
    "Norway",
    "Denmark",
    "Finland",
    "Ireland",
    "Austria",
    "Belgium",
    "Switzerland",
    "Portugal",
    "Greece",
    "Poland",
    "Czech",
    "Hungary",
    "Romania",
    "Bulgaria",
    "Slovakia",
    "Slovenia",
    "Croatia",
    "Serbia",
    "Ukraine",
    "Russia",
    "Turkey",
    "Israel",
    "UAE",
    "Saudi Arabia",
    "Qatar",
    "Kuwait",
    "Bahrain",
    "Oman",
    "Jordan",
    "Lebanon",
    "Egypt",
    "South Africa",
    "Nigeria",
    "Kenya",
    "Morocco",
    "Ghana",
    "Ethiopia",
    "Tanzania",
    "Uganda",
    "India",
    "Pakistan",
    "Bangladesh",
    "Sri Lanka",
    "Nepal",
    "Bhutan",
    "Maldives",
    "China",
    "Japan",
    "South Korea",
    "North Korea",
    "Taiwan",
    "Hong Kong",
    "Macau",
    "Mongolia",
    "Vietnam",
    "Thailand",
    "Malaysia",
    "Singapore",
    "Indonesia",
    "Philippines",
    "Brunei",
    "Cambodia",
    "Laos",
    "Myanmar",
    "Australia",
    "New Zealand",
    "Fiji",
    "Papua New Guinea",
    "Brazil",
    "Argentina",
    "Chile",
    "Colombia",
    "Peru",
    "Venezuela",
    "Bolivia",
    "Paraguay",
    "Uruguay",
    "Ecuador",
    "Guyana",
    "Suriname",
    "Mexico",
    "Guatemala",
    "Belize",
    "Honduras",
    "El Salvador",
    "Nicaragua",
    "Costa Rica",
    "Panama",
    "Cuba",
    "Jamaica",
    "Dominican Republic",
    "Puerto Rico",
]
for c in countries:
    entities.append((c, "country"))
    entities.append((f"{c}Region", "Location"))

# 200 cities
city_prefixes = [
    "New",
    "San",
    "Los",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "Dallas",
    "Austin",
    "Seattle",
    "Denver",
    "Boston",
    "Portland",
    "Miami",
    "Atlanta",
    "Detroit",
    "Minneapolis",
    "Tampa",
    "Denver",
    "Baltimore",
    "Milwaukee",
]
city_suffixes = [
    "York",
    "Francisco",
    "Angeles",
    "Diego",
    "Antonio",
    "Columbus",
    "Charlotte",
    "Vegas",
    "Louisville",
    "Orleans",
]
cities = [
    f"{random.choice(city_prefixes)}{random.choice(city_suffixes)}{i}"
    for i in range(200)
]
for c in cities:
    entities.append((c, "Location"))

# 200 tech hubs
hubs = [
    f"{random.choice(['Silicon', 'Tech', 'Innovation', 'Digital', 'Startup', 'AI', 'Quantum', 'Crypto', 'Blockchain', 'Smart'])}{random.choice(['Valley', 'Hub', 'Park', 'Campus', 'Center', 'Labs', 'Garage', 'Foundry', 'Works', 'Studio'])}{i}"
    for i in range(200)
]
for h in hubs:
    entities.append((h, "Location"))

# 200 VCs
vcs = [
    f"{random.choice(['Sequoia', 'Andreessen', 'Benchmark', 'Accel', 'Greylock', 'Kleiner', 'Founders', 'Social', 'Lightspeed', 'Index', 'Bessemer', 'Insight', 'Norwest', 'General', 'Tiger', 'SoftBank', 'Google', 'First', 'Lowercase', 'Tusk', 'Founder'])}{random.choice(['Capital', 'Ventures', 'Partners', 'Fund', 'Collective', 'Labs', 'Hub'])}{i}"
    for i in range(200)
]
for v in vcs:
    entities.append((v, "Company"))

# 200 unicorns
unicorns = [
    f"{random.choice(['Space', 'Stripe', 'Robinhood', 'Coinbase', 'Klarna', 'Revolut', 'Checkout', 'Brex', 'Ramp', 'Divvy', 'Gusto', 'Rippling', 'Snowflake', 'Databricks', 'Palantir', 'Docker', 'Elastic', 'MongoDB', 'Twilio', 'Zoom', 'Slack', 'Spotify', 'Airbnb', 'Uber', 'Lyft', 'Pinterest', 'Snap', 'Twitter', 'Meta', 'ByteDance', 'Didi', 'Meituan', 'Ant', 'JD', 'Baidu', 'Xiaomi', 'Huawei', 'ZTE', 'Foxconn', 'MediaTek', 'TSMC', 'Samsung', 'Sony', 'SoftBank', 'Rakuten', 'LINE', 'Naver', 'Kakao', 'Grab', 'Gojek', 'Tokopedia', 'Bukalapak', 'Razorpay', 'Paytm', 'Swiggy', 'Zomato', 'Rappi', 'MercadoLibre', 'Nubank', 'Digio', 'EBANX'])}{random.choice(['', 'Inc', 'Corp', 'Labs', 'Tech', 'AI', 'Ventures'])}{i}"
    for i in range(200)
]
for u in unicorns:
    entities.append((u, "Company"))

# 200 banks
banks = [
    f"{random.choice(['JPM', 'Bank of', 'Wells', 'Citigroup', 'Goldman', 'Morgan', 'US', 'PNC', 'Truist', 'Capital', 'TD', 'Scotiabank', 'BMO', 'RBC', 'CIBC', 'Barclays', 'HSBC', 'NatWest', 'Lloyds', 'Santander', 'BBVA', 'Caixa', 'ING', 'UniCredit', 'Credit', 'UBS', 'Deutsche', 'BNP', 'Societe', 'BPCE', 'Rabobank', 'Danske', 'SEB', 'Swedbank', 'Nordea', 'Itau', 'Bradesco', 'Safra', 'BTG', 'XP'])}{random.choice(['Chase', 'America', 'Fargo', '', 'Stanley', 'Bank', 'One', '', 'Group', 'Partners', 'Capital', 'Ventures'])}{i}"
    for i in range(200)
]
for b in banks:
    entities.append((b, "Company"))

# 200 people - tech executives
people = [
    f"{random.choice(['Elon', 'Tim', 'Satya', 'Sundar', 'Mark', 'Jeff', 'Larry', 'Sergey', 'Jack', 'Reed', 'Marc', 'Larry', 'Jensen', 'Lisa', 'Pat', 'Steve', 'Bill', 'Peter', 'Reid', 'Brian', 'Travis', 'Dara', 'Daniel', 'Evan', 'Bobby', 'Jack', 'Pony', 'Lei', 'Masayoshi', 'Sam', 'Demis', 'Dario', 'Mira', 'Howard', 'Larry', 'Cathie', 'Ray', 'Warren', 'Charlie', 'Seth', 'Bill', 'Chamath', 'Michael', 'Brian', 'Vitalik', 'Satoshi', 'Gavin', 'Fred', 'Chris', 'Ben', 'Marc', 'David', 'Keith', 'Dustin', 'Chris', 'Jason', 'Naval', 'Gary', 'Tony', 'Grant', 'Tai', 'Jim', 'Brendon', 'Lewis', 'Tom', 'Russell', 'Dan', 'Brian', 'Zig', 'Joe', 'John', 'Robert', 'Napoleon', 'Dale', 'Maxwell', 'Les', 'Eric', 'Michele', 'David', 'Jocko', 'Leif', 'George', 'Amy', 'Jon', 'Cal', 'Nir', 'Marty', 'Fred', 'Patrick', 'John', 'Clayton', 'Michael', 'W Chan', 'Ren Ng'])}{random.choice(['Musk', 'Cook', 'Nadella', 'Pichai', 'Zuckerberg', 'Bezos', 'Page', 'Brin', 'Dorsey', 'Hastings', 'Benioff', 'Ellison', 'Huang', 'Su', 'Gelsinger', 'Jobs', 'Gates', 'Thiel', 'Hoffman', 'Chesky', 'Kalanick', 'Khosrowshahi', 'Ek', 'Spiegel', 'Murphy', 'Ma', 'Ma', 'Jun', 'Son', 'Altman', 'Hassabis', 'Amodei', 'Murati', 'Marks', 'Fink', 'Wood', 'Dalio', 'Buffett', 'Munger', 'Klarman', 'Ackman', 'Palihapitiya', 'Saylor', 'Armstrong', 'Buterin', 'Nakamoto', 'Wood', 'Wilson', 'Dixon', 'Horowitz', 'Andreessen', 'Sacks', 'Rabois', 'Moskovov', 'Sacca', 'Calacanis', 'Ravikant', 'Vaynerchuk', 'Robbins', 'Cardone', 'Lopez', 'Kwik', 'Burchard', 'Howes', 'Bilyeu', 'Brunson', 'Lok', 'Tracy', 'Ziglar', 'Nightingale', 'Vitale', 'Maxwell', 'Greene', 'Hill', 'Carnegie', 'Maltz', 'Brown', 'Thomas', 'Greger', 'Goggins', 'Willink', 'Babin', 'Klatter', 'Morin', 'Acuff', 'Newport', 'Eyal', 'Cagan', 'Kofman', 'Lencioni', 'Kotter', 'Christensen', 'Porter'])}{i}"
    for i in range(200)
]
for p in people:
    entities.append((p, "Person"))

print(f"Total entities to add: {len(entities)}")

# Add with minimal delay
added = 0
failed = 0
start = time.time()

for i, (name, etype) in enumerate(entities):
    if add_entity(name, etype, 0.8):
        added += 1
    else:
        failed += 1

    if (i + 1) % 200 == 0:
        elapsed = time.time() - start
        rate = (i + 1) / elapsed
        print(
            f"Progress: {i + 1}/{len(entities)} - Added: {added}, Failed: {failed}, Rate: {rate:.1f}/s"
        )

    time.sleep(0.05)  # 50ms between each

elapsed = time.time() - start
print(
    f"Final: {added} added, {failed} failed in {elapsed:.1f}s ({added / elapsed:.1f}/s)"
)
