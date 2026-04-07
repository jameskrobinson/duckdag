-- Top movers by absolute 24h price change.
-- Parameterised via Jinja: {{ limit }} rows returned.
-- Input: enriched (output of classify_change node)
SELECT
    rank() OVER (ORDER BY ABS(change_24h_pct) DESC) AS mover_rank,
    symbol,
    name,
    sector,
    market_tier,
    ROUND(price_usd, 4)                 AS price_usd,
    ROUND(change_24h_pct, 2)            AS change_24h_pct,
    change_class,
    ROUND(market_cap_usd / 1e9, 1)     AS market_cap_bn
FROM enriched
ORDER BY ABS(change_24h_pct) DESC
LIMIT {{ limit | default(10) }}
