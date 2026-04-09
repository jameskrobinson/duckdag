-- Export the final summary table to a CSV file in the output directory.
COPY (SELECT * FROM "monthly_summary") TO '{{ params.output_path | default("output/monthly_summary.csv") }}' (FORMAT CSV, HEADER TRUE)
