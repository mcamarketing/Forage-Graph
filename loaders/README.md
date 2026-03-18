# Forage Graph — Bulk Data Loaders

These scripts bootstrap the graph with high-quality, free, open datasets. Run them once to establish the foundational skeleton, then let the n8n pipeline handle continuous enrichment.

## Loaders

| Script | Source | Data | License | Est. Entities |
|---|---|---|---|---|
| `wikidata_loader.py` | Wikidata SPARQL | Countries, heads of government, central banks, international organisations | CC0 | ~2,000 |
| `gleif_loader.py` | GLEIF Bulk Files | 2.3M legal entities + ownership chains (Level 1 + Level 2) | CC0 | ~2.3M |

## Usage

```bash
# Set environment variables (or edit defaults in each script)
export GRAPH_URL="https://forage-graph-production.up.railway.app"
export GRAPH_SECRET="your_secret_here"

# Run Wikidata loader first (creates country/government skeleton)
python3 wikidata_loader.py

# Run GLEIF loader (creates company nodes with ownership chains)
# Set GLEIF_MAX_ENTITIES=50000 for a sample load, 0 for full 2.3M
export GLEIF_MAX_ENTITIES=50000
python3 gleif_loader.py
```

## Recommended Load Order

1. **`wikidata_loader.py`** — creates all country, government, and central bank nodes first. Every other entity links to these.
2. **`gleif_loader.py`** — loads companies with `registered_in` edges to country nodes created in step 1.

## Planned Loaders

| Script | Source | Data |
|---|---|---|
| `acled_loader.py` | ACLED API | 2M+ conflict events (requires free registration at acleddata.com) |
| `companies_house_loader.py` | Companies House UK | UK corporate registry + officer relationships |
| `icij_loader.py` | ICIJ Offshore Leaks | Shell company ownership chains (Panama Papers, Pandora Papers) |
| `un_comtrade_loader.py` | UN Comtrade | Bilateral trade flows between countries |
| `open_sanctions_loader.py` | OpenSanctions | Sanctioned entities, PEPs, wanted persons |
