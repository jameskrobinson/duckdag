-- Filter rows to only those within the last N days.
SELECT *
FROM {{ input }}
WHERE TRY_CAST({{ params.date_column | default('order_date') }} AS DATE)
      >= CURRENT_DATE - INTERVAL '{{ params.days | default(90) }}' DAY
