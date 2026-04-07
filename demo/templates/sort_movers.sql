-- Cast numeric columns to proper types and sort by absolute 24h change.
SELECT
    CAST(rank        AS INTEGER) AS rank,
    name,
    symbol,
    CAST(price_usd        AS DOUBLE)  AS price_usd,
    CAST(market_cap_usd   AS BIGINT)  AS market_cap_usd,
    ROUND(CAST(change_24h_pct AS DOUBLE), 2) AS change_24h_pct
FROM clean_cols
WHERE change_24h_pct IS NOT NULL
ORDER BY ABS(change_24h_pct) DESC
