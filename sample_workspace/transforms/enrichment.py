"""Workspace transforms for market data enrichment.

Each function follows the standard pipeline_core transform interface:
    fn(inputs: dict[str, pd.DataFrame], params: dict) -> pd.DataFrame

Docstrings use the structured format that the /node-types/inspect endpoint
parses to generate the node config form in the UI.
"""
from __future__ import annotations

from typing import Any

import pandas as pd

DFMap = dict[str, pd.DataFrame]


# ---------------------------------------------------------------------------
# classify_change
# ---------------------------------------------------------------------------

def classify_change(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Classify a percentage-change column into directional labels and magnitude
      buckets (e.g. "strong_gain", "slight_loss").

    Details:
      - Adds a 'direction' column: "gain" | "loss" | "flat" based on the sign
        of the change column.
      - Adds a 'magnitude' column: "strong" | "moderate" | "slight" based on
        configurable thresholds.
      - Combines both into a 'change_class' column: e.g. "strong_gain".

    Params:
      - column (str, required): name of the % change column to classify.
      - strong_threshold (float, default=5.0): absolute % above which change is
        "strong".
      - moderate_threshold (float, default=2.0): absolute % above which change
        is "moderate" (below strong_threshold).
      - flat_threshold (float, default=0.1): absolute % below which change is
        "flat" (overrides direction).

    Input requirements:
      - inputs: 1 DataFrame containing the named column.
      - column must be numeric.

    Output:
      - Original DataFrame plus 'direction', 'magnitude', and 'change_class'
        columns.

    Tags:
      - market, classification, enrichment
    """
    df = next(iter(inputs.values())).copy()
    col: str = params.get("column", "change_24h_pct")
    strong_thresh: float = float(params.get("strong_threshold", 5.0))
    moderate_thresh: float = float(params.get("moderate_threshold", 2.0))
    flat_thresh: float = float(params.get("flat_threshold", 0.1))

    if col not in df.columns:
        raise KeyError(f"classify_change: column '{col}' not found in DataFrame")

    s = pd.to_numeric(df[col], errors="coerce")
    abs_s = s.abs()

    direction = pd.Series("flat", index=df.index)
    direction[s > flat_thresh] = "gain"
    direction[s < -flat_thresh] = "loss"

    magnitude = pd.Series("slight", index=df.index)
    magnitude[abs_s >= moderate_thresh] = "moderate"
    magnitude[abs_s >= strong_thresh] = "strong"
    magnitude[abs_s < flat_thresh] = "flat"

    df["direction"] = direction
    df["magnitude"] = magnitude
    df["change_class"] = magnitude + "_" + direction

    # Clean up: flat stays just "flat" regardless of direction
    flat_mask = abs_s < flat_thresh
    df.loc[flat_mask, "change_class"] = "flat"

    return df


# ---------------------------------------------------------------------------
# add_market_tier
# ---------------------------------------------------------------------------

def add_market_tier(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Bucket assets into market-cap tiers: large, mid, small, or micro.

    Details:
      - Adds a 'market_tier' column based on configurable thresholds.
      - Thresholds are compared against the market cap column in USD.

    Params:
      - market_cap_column (str, default="market_cap_usd"): column holding
        market cap values.
      - large_cap (float, default=50000000000): minimum USD market cap for
        large-cap classification.
      - mid_cap (float, default=5000000000): minimum USD market cap for
        mid-cap classification.
      - small_cap (float, default=500000000): minimum USD market cap for
        small-cap classification (below = micro).

    Input requirements:
      - inputs: 1 DataFrame containing the market cap column.

    Output:
      - Original DataFrame plus 'market_tier' column.

    Tags:
      - market, classification, enrichment
    """
    df = next(iter(inputs.values())).copy()
    mc_col: str = params.get("market_cap_column", "market_cap_usd")
    large_cap: float = float(params.get("large_cap", 50_000_000_000))
    mid_cap: float = float(params.get("mid_cap", 5_000_000_000))
    small_cap: float = float(params.get("small_cap", 500_000_000))

    if mc_col not in df.columns:
        raise KeyError(f"add_market_tier: column '{mc_col}' not found in DataFrame")

    mc = pd.to_numeric(df[mc_col], errors="coerce")
    tier = pd.Series("micro", index=df.index)
    tier[mc >= small_cap] = "small"
    tier[mc >= mid_cap] = "mid"
    tier[mc >= large_cap] = "large"

    df["market_tier"] = tier
    return df


# ---------------------------------------------------------------------------
# compute_pnl
# ---------------------------------------------------------------------------

def compute_pnl(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Compute unrealised P&L for a positions DataFrame joined with current prices.

    Details:
      - Joins on a symbol column (case-insensitive by default).
      - Adds 'current_value_usd', 'cost_usd', 'pnl_usd', and 'pnl_pct' columns.
      - Designed to work with a positions input containing 'quantity' and
        'cost_basis_usd', and a prices input containing a price column.

    Params:
      - positions_key (str, default="positions"): key for the positions DataFrame
        in the inputs dict.
      - prices_key (str, default="prices"): key for the prices DataFrame in the
        inputs dict.
      - symbol_col (str, default="symbol"): join key column name (must exist in
        both DataFrames).
      - price_col (str, default="price_usd"): column in the prices DataFrame
        holding the current price.
      - quantity_col (str, default="quantity"): column in positions holding units
        held.
      - cost_basis_col (str, default="cost_basis_usd"): column in positions
        holding per-unit cost basis.

    Input requirements:
      - inputs: must contain both the positions and prices DataFrames, keyed by
        positions_key and prices_key respectively.

    Output:
      - Positions DataFrame enriched with price, current value, cost, PnL columns.

    Tags:
      - portfolio, pnl, finance, enrichment
    """
    pos_key: str = params.get("positions_key", "positions")
    px_key: str = params.get("prices_key", "prices")
    sym_col: str = params.get("symbol_col", "symbol")
    price_col: str = params.get("price_col", "price_usd")
    qty_col: str = params.get("quantity_col", "quantity")
    cost_col: str = params.get("cost_basis_col", "cost_basis_usd")

    if pos_key not in inputs or px_key not in inputs:
        available = list(inputs.keys())
        raise KeyError(
            f"compute_pnl: expected inputs '{pos_key}' and '{px_key}', "
            f"got: {available}"
        )

    positions = inputs[pos_key].copy()
    # Keep all price columns so that enriched fields (sector, market_tier, etc.)
    # carry through into the joined output.
    prices = inputs[px_key].copy()

    # Normalise symbol case for join
    positions["_sym_upper"] = positions[sym_col].str.upper()
    prices["_sym_upper"] = prices[sym_col].str.upper()

    # Drop the original symbol column from prices to avoid duplication;
    # the positions symbol column is kept as the canonical join key.
    prices_for_merge = prices.drop(columns=[sym_col]).copy()

    merged = positions.merge(
        prices_for_merge,
        on="_sym_upper",
        how="left",
    ).drop(columns=["_sym_upper"])

    merged["current_value_usd"] = (
        pd.to_numeric(merged[qty_col], errors="coerce")
        * pd.to_numeric(merged[price_col], errors="coerce")
    )
    merged["cost_usd"] = (
        pd.to_numeric(merged[qty_col], errors="coerce")
        * pd.to_numeric(merged[cost_col], errors="coerce")
    )
    merged["pnl_usd"] = merged["current_value_usd"] - merged["cost_usd"]
    merged["pnl_pct"] = (merged["pnl_usd"] / merged["cost_usd"].replace(0, float("nan"))) * 100

    return merged


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

REGISTRY: dict[str, Any] = {
    "classify_change": classify_change,
    "add_market_tier": add_market_tier,
    "compute_pnl": compute_pnl,
}
