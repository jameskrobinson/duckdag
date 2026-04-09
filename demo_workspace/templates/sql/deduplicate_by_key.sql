-- Keep the most recent row per key, removing duplicates.
-- Params: key_col (default: id), tie_break_col (default: updated_at)
SELECT * EXCLUDE (_rn)
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY {{ params.key_col | default('id') }}
      ORDER BY {{ params.tie_break_col | default('updated_at') }} DESC
    ) AS _rn
  FROM {{ input }}
)
WHERE _rn = 1
