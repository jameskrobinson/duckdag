# Sample Workspace

Demonstrates the canonical workspace layout for the pipeline platform.

## Structure

```
sample_workspace/
  variables.yaml                   # Shared variable defaults
  data/
    prices.csv                     # 20 crypto assets with prices, market cap, sector
    positions.csv                  # Sample portfolio holdings
  transforms/
    enrichment.py                  # Custom transforms: classify_change, add_market_tier, compute_pnl
  templates/
    top_movers.sql                 # Rank assets by absolute 24h move
    sector_summary.sql             # Aggregate by sector + market tier
    portfolio_summary.sql          # P&L summary with classification
    export_csv.sql                 # COPY … TO … reusable export template
  pipelines/
    market_summary/pipeline.yaml   # Prices → enrich → top movers + sector summary CSVs
    portfolio/pipeline.yaml        # Positions + prices → P&L → portfolio summary CSV
  output/                          # Written by pipeline runs (gitignored)
  runs/                            # Session bundles (created automatically)
```

## Usage in the UI

1. Set workspace to the absolute path of this directory.
2. Click **Load** → select a pipeline from the list.
3. Click **▶ Run** to create a dev session and execute.
4. Outputs appear in `output/` after the session completes.

## Custom transforms

`transforms/enrichment.py` contains three transforms registered in `REGISTRY`:

| Transform | What it does |
|---|---|
| `classify_change` | Adds `direction`, `magnitude`, `change_class` from a % change column |
| `add_market_tier` | Adds `market_tier` (large/mid/small/micro) from market cap |
| `compute_pnl` | Joins positions to prices, computes `pnl_usd` and `pnl_pct` |

Open the **ƒ Transforms** editor to view or edit these files directly.
