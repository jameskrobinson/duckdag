-- Aggregate classified price data by sector.
-- Input: classify_change (output of classify_change node in dq_demo pipeline)
SELECT
    sector,
    COUNT(*)                                    AS asset_count,
    ROUND(AVG(change_24h_pct), 2)              AS avg_change_24h_pct,
    ROUND(SUM(market_cap_usd) / 1e9, 1)       AS total_market_cap_bn,
    COUNT(*) FILTER (WHERE change_class = 'strong_gain')  AS strong_gainers,
    COUNT(*) FILTER (WHERE change_class = 'strong_loss')  AS strong_losers
FROM classify_change
GROUP BY sector
ORDER BY total_market_cap_bn DESC
