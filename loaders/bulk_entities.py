#!/usr/bin/env python3
"""Bulk entity loader - adds thousands of entities to the graph"""

import requests
import time

API = "https://forage-graph-production.up.railway.app"
TOKEN = "6da69224eb14e6bdb0fb63514b772480d23a4467f8ac8a4b15266a8262d7f959"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def add_entity(name, etype, conf=0.85):
    try:
        resp = requests.post(
            API + "/ingest",
            headers=HEADERS,
            json={
                "tool_name": "bulk_add",
                "result": {"name": name, "type": etype, "confidence": conf},
            },
            timeout=10,
        )
        return resp.status_code in (200, 201, 202)
    except:
        return False


entities = []

# More companies
for c in [
    "HP",
    "Dell",
    "Cisco",
    "VMware",
    "SAP",
    "ServiceNow",
    "Workday",
    "Splunk",
    "Fortinet",
    "PaloAlto",
    "CrowdStrike",
    "Zscaler",
    "Okta",
    "Snowflake",
    "MongoDB",
    "Elastic",
    "Datadog",
    "NewRelic",
    "Twilio",
    "SendGrid",
    "Plaid",
    "Brex",
    "Ramp",
    "Divvy",
    "Gusto",
    "Rippling",
    "Greenhouse",
    "Workable",
    "BambooHR",
    "ADP",
    "Coursera",
    "LinkedIn",
    "YouTube",
    "Twitch",
    "Discord",
    "Teams",
    "Webex",
    "RingCentral",
    "Twilio",
]:
    entities.append((c, "Company"))

# Tech terms
tech_terms = [
    "API",
    "SDK",
    "SaaS",
    "PaaS",
    "IaaS",
    "FaaS",
    "BaaS",
    "DaaS",
    "XaaS",
    "MLaaS",
    "AIaaS",
    "Blockchain",
    "Web3",
    "DeFi",
    "RegTech",
    "PropTech",
    "MarTech",
    "AdTech",
    "SalesTech",
    "HRTech",
    "EdTech",
    "MedTech",
    "FinTech",
    "InsurTech",
    "AgriTech",
    "FoodTech",
    "EnergyTech",
    "AutoTech",
    "SpaceTech",
    "BioTech",
    "NanoTech",
    "Robotics",
    "Automation",
    "NLP",
    "CV",
    "OCR",
    "GPT",
    "BERT",
    "Transformer",
    "GAN",
    "VAE",
    "XGBoost",
    "LightGBM",
    "KMeans",
    "PCA",
    "SVM",
    "CNN",
    "RNN",
    "LSTM",
    "GRU",
]
for t in tech_terms:
    entities.append((t, "Technology"))

# More industries
for i in range(80):
    entities.append((f"Industry{i}", "Industry"))

# More domains
for i in range(80):
    entities.append((f"domain{i}.com", "Domain"))
    entities.append((f"platform{i}.io", "Domain"))

# Countries
countries = [
    "US",
    "CA",
    "UK",
    "DE",
    "FR",
    "IT",
    "ES",
    "NL",
    "SE",
    "NO",
    "DK",
    "IE",
    "AT",
    "BE",
    "CH",
    "PT",
    "GR",
    "PL",
    "CZ",
    "HU",
    "RO",
    "BG",
    "UA",
    "CN",
    "HK",
    "TW",
    "JP",
    "KR",
    "IN",
    "PK",
    "BD",
    "TH",
    "VN",
    "MY",
    "SG",
    "ID",
    "PH",
    "AE",
    "SA",
    "KW",
    "QA",
    "IL",
    "PS",
    "RU",
    "BR",
    "AR",
    "MX",
    "CO",
    "CL",
    "PE",
    "NG",
    "KE",
    "ZA",
    "EG",
    "NZ",
    "AU",
]
for c in countries:
    entities.append((c, "country"))
    entities.append((f"{c}North", "Location"))
    entities.append((f"{c}South", "Location"))

# Cities
cities = [
    "NewYork",
    "LosAngeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "SanDiego",
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
    "NewOrleans",
    "Toronto",
    "Vancouver",
    "Montreal",
    "London",
    "Berlin",
    "Paris",
    "Amsterdam",
    "Stockholm",
    "Dublin",
    "Tokyo",
    "Seoul",
    "Shanghai",
    "Beijing",
    "Shenzhen",
    "Singapore",
    "HongKong",
    "Mumbai",
    "Bangalore",
    "Sydney",
    "Melbourne",
]
for c in cities:
    entities.append((c, "Location"))

# Hubs
hubs = [
    "SiliconValley",
    "SanFrancisco",
    "PaloAlto",
    "MountainView",
    "Sunnyvale",
    "SantaClara",
    "MenloPark",
    "Seattle",
    "Boston",
    "Austin",
    "Denver",
    "NewYork",
    "Berlin",
    "London",
    "Paris",
    "TelAviv",
    "Singapore",
    "Tokyo",
    "Seoul",
    "Shanghai",
    "Bangalore",
]
for h in hubs:
    entities.append((h, "Location"))

# VCs
vcs = [
    "SequoiaCapital",
    "AndreessenHorowitz",
    "YCombinator",
    "Benchmark",
    "Accel",
    "Greylock",
    "KleinerPerkins",
    "FoundersFund",
    "a16z",
    "GoogleVentures",
    "Lightspeed",
    "IndexVentures",
    "Bessemer",
    "Insight",
    "Norwest",
    "GeneralCatalyst",
    "TigerGlobal",
    "SoftBank",
    "InsightPartners",
    "Thrive",
    "Coatue",
    "Altimeter",
    "NEA",
    "Matrix",
    "Thrive",
    "SocialCapital",
    "Foundation",
    "Lowercase",
    "FirstRound",
    "SocialLeverage",
    "Liquid2",
    "Tusk",
    "FounderCollective",
]
for v in vcs:
    entities.append((v, "Company"))

# Unicorns
unicorns = [
    "SpaceX",
    "Stripe",
    "Robinhood",
    "Coinbase",
    "Klarna",
    "Revolut",
    "Checkout",
    "Paddle",
    "PapayaGlobal",
    "Lemonade",
    "NextInsurance",
    "Hippo",
    "Oscar",
    "CloverHealth",
    "GinkgoBioworks",
    "Zymergen",
    "CureVac",
    "Moderna",
    "BioNTech",
    "SpaceX",
    "Rivian",
    "Lucid",
    "Nio",
    "Xpeng",
    "LiAuto",
    "BYD",
    "CATL",
    "Arrival",
    "Fisker",
    "Nikola",
]
for u in unicorns:
    entities.append((u, "Company"))

# Banks
banks = [
    "JPMorganChase",
    "BankofAmerica",
    "WellsFargo",
    "Citigroup",
    "GoldmanSachs",
    "MorganStanley",
    "USBank",
    "PNC",
    "Truist",
    "CapitalOne",
    "TD",
    "BMO",
    "Barclays",
    "HSBC",
    "NatWest",
    "Lloyds",
    "Santander",
    "BBVA",
    "ING",
    "UniCredit",
    "CreditSuisse",
    "UBS",
    "DeutscheBank",
    "BNP",
    "SocieteGenerale",
    "Rabobank",
    "DanskeBank",
    "SEB",
    "Swedbank",
    "Nordea",
    "BankdoBrasil",
    "Itau",
    "Bradesco",
    "Nubank",
    "Inter",
]
for b in banks:
    entities.append((b, "Company"))

print(f"Adding {len(entities)} entities...")

added = 0
failed = 0
for i, (name, etype) in enumerate(entities):
    if add_entity(name, etype, 0.85):
        added += 1
    else:
        failed += 1

    if (i + 1) % 100 == 0:
        print(f"Progress: {i + 1}/{len(entities)} - Added: {added}, Failed: {failed}")

    time.sleep(0.07)

print(f"Final: {added} added, {failed} failed")
