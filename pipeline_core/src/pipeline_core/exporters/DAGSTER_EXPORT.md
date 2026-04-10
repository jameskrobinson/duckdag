# Dagster Exporter

Generates a standalone Dagster definitions file from a `pipeline.yaml`. The output is a single self-contained Python file with one `@asset` per pipeline node, wired together via Dagster's asset dependency graph.

---

## Quick start

### CLI

```bash
pipeline export dagster pipelines/my_pipeline/pipeline.yaml -o my_pipeline_dagster.py
```

With variable overrides and a specific env file:

```bash
pipeline export dagster pipelines/my_pipeline/pipeline.yaml \
  -o my_pipeline_dagster.py \
  --env env.yaml \
  --var start_date=2024-01-01 \
  --var country=UK
```

If `--output` / `-o` is omitted, the file is written to `<pipeline_name>_dagster.py` in the current directory.

### Python API

```python
from pathlib import Path
from pipeline_core.resolver import resolve_pipeline
from pipeline_core.exporters.dagster_exporter import export_dagster

spec = resolve_pipeline("pipelines/my_pipeline/pipeline.yaml", env={}, variables={})
src = export_dagster(
    spec,
    pipeline_name="my_pipeline",
    templates_dir=Path("pipelines/my_pipeline/config"),
)
Path("my_pipeline_dagster.py").write_text(src, encoding="utf-8")
```

---

## What gets generated

### File structure

```
"""
Dagster job — my_pipeline
Generated 2026-04-09 10:00 UTC by pipeline-core Dagster exporter.
...
"""

import pandas as pd
import duckdb
import pyodbc
from dagster import asset, Definitions, ConfigurableResource

# Pipeline variables
PIPELINE_VARIABLES: dict = {"start_date": "2024-01-01", "country": "UK"}

# ODBC resources (one class per distinct connection)
class _ODBCResource_odbc_warehouse(ConfigurableResource):
    connection_string: str = "DRIVER={SQL Server};SERVER=..."
    def get_connection(self): return pyodbc.connect(self.connection_string)

# One @asset per node
@asset
def load_sales(odbc_warehouse: _ODBCResource_odbc_warehouse) -> pd.DataFrame:
    ...

@asset
def transform_margins(load_sales: pd.DataFrame) -> pd.DataFrame:
    ...

# Definitions wiring
defs = Definitions(
    assets=[load_sales, transform_margins, ...],
    resources={"odbc_warehouse": _ODBCResource_odbc_warehouse(...)},
)
```

### Node type mapping

| Pipeline node type | Generated asset body |
|---|---|
| `sql_transform` | In-memory DuckDB connection; inputs registered as views; SQL template rendered via Jinja2; returns DataFrame |
| `sql_exec` | Same as `sql_transform` but executes for side effects (returns `None`) |
| `pandas_transform` | Imports the transform function by dotted path; calls `_transform_fn(_inputs, _params)` |
| `load_odbc` | Renders SQL template; reads via `pd.read_sql` using the ODBC resource |
| `load_duckdb` | Connects to the DuckDB file (read-only); executes `table` or `query` param |
| `load_file` | `pd.read_csv` / `read_parquet` / `read_excel` / `read_stata` based on file extension |
| `load_rest_api` | `requests.request(...)` call; navigates `record_path` keys; returns DataFrame |
| `push_odbc` | Calls `_push_odbc_dataframe` helper; supports `replace` and `append` modes |
| `push_duckdb` | Registers upstream DataFrame in DuckDB; `CREATE TABLE … AS SELECT * FROM …` |
| `export_dta` | `df.to_stata(path, write_index=False)` |
| `load_ssas` | Raises `NotImplementedError` with a comment to implement manually |
| `load_internal_api` | Raises `NotImplementedError` with a comment to implement manually |

### SQL templates

SQL templates (`.sql.j2` files) are read at export time and embedded directly in the generated file as string literals. They are rendered at asset execution time using Jinja2, with `PIPELINE_VARIABLES` merged with any node-level params.

If a template file cannot be found, a `# TODO` comment is emitted with a fallback to the node's inline `query` param (if any).

### ODBC connections

ODBC connections are collected from the pipeline's `odbc:` block (referenced via `odbc_key`) or from inline connection params on the node itself. Each distinct connection becomes a `ConfigurableResource` subclass. The connection string is embedded in the class default but can be overridden when constructing the `Definitions` object.

The `_push_odbc_dataframe` helper is emitted once when any `push_odbc` nodes are present. It handles `replace` (DROP + CREATE TABLE + INSERT) and `append` (INSERT only) modes, with `fast_executemany` enabled and NaN-to-None normalisation.

### Pipeline variables

Variables from `variables.yaml` (or `--var` overrides) are embedded as a `PIPELINE_VARIABLES` dict at the top of the file. All SQL templates and pandas transform params reference this dict at runtime. Edit the dict directly in the generated file to change defaults, or pass overrides when running with Dagster's config system.

---

## Running the generated file

### Materialise all assets

```bash
dagster asset materialize -f my_pipeline_dagster.py --select '*'
```

### Materialise a subset

```bash
dagster asset materialize -f my_pipeline_dagster.py --select 'transform_margins'
```

### Load into Dagster's UI (dagster dev)

```bash
dagster dev -f my_pipeline_dagster.py
```

Then open `http://localhost:3000` and materialise assets from the asset graph.

---

## Dependencies

The generated file requires these packages to be installed in the target environment:

| Package | When required |
|---|---|
| `dagster` | Always |
| `pandas` | Always |
| `duckdb` | Any `sql_transform`, `sql_exec`, `load_duckdb`, or `push_duckdb` node |
| `pyodbc` | Any `load_odbc` or `push_odbc` node |
| `requests` | Any `load_rest_api` node |
| `jinja2` | Any node with a SQL template |

The pipeline platform's own packages (`pipeline_core`, `pipeline_service`, etc.) are **not** required in the target environment — the generated file is fully standalone.

---

## Known limitations

- **`load_ssas`** — Uses the Windows OLEDB SSAS driver, which has no Python equivalent. A `NotImplementedError` stub is emitted; implement the asset manually.
- **`load_internal_api`** — Calls back into the pipeline platform's own service. A stub is emitted; replace with the equivalent direct data source call.
- **DuckDB `push_duckdb` without a path** — Creates a table in an in-memory DuckDB instance, which is discarded when the asset completes. Specify a `path` param to persist to a file.
- **Credentials in generated file** — ODBC connection strings (including passwords) are embedded in the file. Store the generated file securely and consider overriding `connection_string` via environment variables or Dagster's secrets management before committing.
- **Pandas transform imports** — The transform function is imported by dotted module path (e.g. `transforms.finance.calc_margin`). The module must be importable from the environment where Dagster runs. Ensure `transforms/` is on `PYTHONPATH` or installed as a package.
