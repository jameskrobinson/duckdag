-- Export a named table to a CSV file.
-- Jinja params: {{ source_table }}, {{ output_path }}
COPY {{ source_table }}
TO '{{ output_path }}'
(HEADER, DELIMITER ',')
