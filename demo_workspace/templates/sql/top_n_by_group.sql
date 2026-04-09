-- Return top N rows per group using ROW_NUMBER().
-- Params: group_col (default: category), sort_col (default: revenue), n (default: 5)
SELECT *
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY {{ params.group_col | default('category') }}
      ORDER BY {{ params.sort_col | default('revenue') }} DESC
    ) AS _rn
  FROM {{ input }}
)
WHERE _rn <= {{ params.n | default(5) }}
