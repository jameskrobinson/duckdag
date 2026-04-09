-- Filter rows to only those within the last N days
-- Params: date_column (default: order_date), days (default: 90)
SELECT *
FROM {{ input }}
WHERE TRY_CAST({{ params.date_column | default('order_date') }} AS DATE)
      >= CURRENT_DATE - INTERVAL '{{ params.days | default(90) }}' DAY
