-- DuckDBStore persists each node output as _store_{node_id} in the session DuckDB.
COPY (
    SELECT * FROM "_store_sorted_movers"
)
TO 'crypto_summary.csv' (FORMAT CSV, HEADER TRUE);
