-- Aggregate enriched price data by sector.
-- Input: enriched (output of add_market_tier + classify_change nodes)
SELECT
    sector,
    market_tier,
    COUNT(*)                                    AS asset_count,
    ROUND(AVG(change_24h_pct), 2)              AS avg_change_24h_pct,
    ROUND(SUM(market_cap_usd) / 1e9, 1)       AS total_market_cap_bn,
    ROUND(SUM(volume_24h_usd) / 1e9, 2)       AS total_volume_24h_bn,
    COUNT(*) FILTER (WHERE direction = 'gain')  AS gainers,
    COUNT(*) FILTER (WHERE direction = 'loss')  AS losers
FROM enriched
GROUP BY sector, market_tier
ORDER BY total_market_cap_bn DESC
