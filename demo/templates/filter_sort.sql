-- Filter out World Bank aggregate regions (they have 2-char iso3 codes like "1A", "S3")
-- Cast types and sort by population descending.
SELECT
    country,
    iso3,
    CAST(year       AS INTEGER) AS year,
    CAST(population AS BIGINT)  AS population
FROM clean_cols
WHERE population IS NOT NULL
  AND LENGTH(iso3) = 3
ORDER BY population DESC
