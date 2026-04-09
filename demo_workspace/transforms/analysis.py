"""Workspace transforms for retail sales analysis.

Each function follows the standard pipeline_core transform interface:
    fn(inputs: dict[str, pd.DataFrame], params: dict) -> pd.DataFrame
"""
from __future__ import annotations
from typing import Any
import pandas as pd

DFMap = dict[str, pd.DataFrame]


def add_revenue(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Add revenue and net_price columns to a sales DataFrame.

    Details:
      - revenue = quantity * unit_price * (1 - discount_pct / 100)
      - net_price = unit_price * (1 - discount_pct / 100)

    Params:
      - quantity_col (str, default="quantity"): column holding units sold.
      - price_col (str, default="unit_price"): column holding the unit price.
      - discount_col (str, default="discount_pct"): column holding the discount %.

    Input requirements:
      - inputs: 1 DataFrame with the quantity, price, and discount columns.

    Output:
      - Original DataFrame plus 'net_price' and 'revenue' columns.

    Tags:
      - sales, finance, enrichment
    """
    df = next(iter(inputs.values())).copy()
    qty_col: str = params.get("quantity_col", "quantity")
    price_col: str = params.get("price_col", "unit_price")
    disc_col: str = params.get("discount_col", "discount_pct")

    df["net_price"] = df[price_col] * (1 - df[disc_col] / 100)
    df["revenue"] = df[qty_col] * df["net_price"]
    return df
