-- Roll up portfolio P&L to a summary row plus per-position detail.
-- Input: pnl (output of compute_pnl node)
SELECT
    symbol,
    ROUND(quantity, 4)              AS quantity,
    ROUND(cost_basis_usd, 2)        AS cost_basis_usd,
    ROUND(price_usd, 4)             AS current_price_usd,
    ROUND(cost_usd, 2)              AS total_cost_usd,
    ROUND(current_value_usd, 2)     AS current_value_usd,
    ROUND(pnl_usd, 2)               AS pnl_usd,
    ROUND(pnl_pct, 1)               AS pnl_pct,
    CASE
        WHEN pnl_pct >= 20  THEN 'strong_gain'
        WHEN pnl_pct >= 5   THEN 'gain'
        WHEN pnl_pct <= -20 THEN 'strong_loss'
        WHEN pnl_pct <= -5  THEN 'loss'
        ELSE 'flat'
    END                             AS pnl_class,
    sector,
    market_tier
FROM pnl
ORDER BY pnl_usd DESC
