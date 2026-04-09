-- Aggregate by year-month and optional category column.
SELECT
  STRFTIME(TRY_CAST({{ params.date_col | default('order_date') }} AS DATE), '%Y-%m') AS year_month,
  {% if params.group_col %}
  {{ params.group_col }},
  {% endif %}
  COUNT(*) AS row_count,
  SUM({{ params.value_col | default('revenue') }})  AS total_value,
  AVG({{ params.value_col | default('revenue') }})  AS avg_value,
  MIN({{ params.value_col | default('revenue') }})  AS min_value,
  MAX({{ params.value_col | default('revenue') }})  AS max_value
FROM {{ input }}
GROUP BY 1{% if params.group_col %}, 2{% endif %}
ORDER BY 1{% if params.group_col %}, 2{% endif %}
