# Palette Model

The palette is the left-hand panel in the pipeline builder.  It gives users a
structured, searchable catalogue of everything they can drag onto the canvas to
create a new pipeline node.

---

## Structure

The palette is divided into three top-level **buckets**:

| Bucket | Node categories | Example types |
|--------|----------------|---------------|
| **Sources** | `load` | `load_file`, `load_odbc`, `load_rest_api`, `load_duckdb` |
| **Transforms** | `transform`, `sql` (produces output) | `pandas_transform`, `sql_transform` |
| **Sinks** | `export`, `sql` (no output) | `push_odbc`, `export_dta`, `sql_exec` |

A fourth section, **Pipeline**, appears at the bottom when a pipeline is open.
It shows the current canvas nodes as draggable presets — a quick way to
duplicate or reuse an already-configured node.

---

## Two-level hierarchy: function → config

Every draggable item in the palette belongs to one of two levels:

### Function (◇)

A *function* represents a node type (or, for pandas transforms, a specific
Python callable).  Dragging a function onto the canvas creates a blank node of
that type.  Functions are the top-level draggable.

### Config (●)

A *config* (or preset) is a pre-filled version of a function — it carries a
set of params that are stamped onto the node when dropped.  Configs are shown
as indented children of their parent function.  Dragging a config creates a
node with those params already populated.

Configs come from three origins:

| Origin | Where they come from | Badge colour |
|--------|---------------------|--------------|
| `builtin` | Hardcoded in `pipeline_service/api/templates.py` `_COMMON` list | (none) |
| `workspace` | Discovered from the open workspace directory | green |
| `pipeline` | Discovered from `{workspace}/pipelines/{name}/config/` | cyan |

---

## Transforms grouping

Transforms are shown three levels deep: **group → function → config**.

Groups come from two sources:

1. **Python groups** — one group per module discovered by the pandas transform
   scanner (e.g. `Basic`, `Finance`).

2. **SQL groups** — a flat `SQL` group always present for `sql_transform`.
   If any local `.sql` files declare a `-- category:` front-matter comment
   (see below), they appear in additional named SQL groups *above* the flat
   SQL group (e.g. `Finance`, `Analytics`).  The group label comes from the
   `category` value; the group `name` is `SQL/{category}`.

---

## Tag system

Every node type, pandas transform function, and template config can carry one
or more **tags** — short searchable labels like `sql`, `finance`, `daily`.

Tags are used in two ways:

1. **Text filter** — typing in the search box matches tag text in addition to
   labels and descriptions.

2. **Tag browser** — the `# Tags` toggle in the palette header switches to a
   flat alphabetical list of all tags with occurrence counts.  Clicking a tag
   shows only nodes/configs that carry that tag across all three buckets.

### Declaring tags on node types

Tags are declared directly in `pipeline_service/node_types.py`:

```python
NodeTypeSchema(
    type="load_file",
    ...
    tags=["load", "file", "csv", "parquet", "excel", "stata", "source"],
)
```

### Declaring tags on pandas transforms

Use the `Tags:` section in the function docstring (same format as `Params:`):

```python
def my_transform(inputs, params):
    """
    Short summary.

    Tags: finance, daily, reporting
    """
```

### Declaring tags on SQL templates

Use front-matter comment lines at the top of the `.sql` file (before any SQL):

```sql
-- category: Finance
-- tags: daily, reporting, revenue
-- description: Monthly revenue summary by product category
-- label: Monthly Revenue Summary

SELECT
    date_trunc('month', order_date) AS month,
    ...
```

All four keys are optional.  `category` is the only key that affects grouping;
the others affect how the template appears in the palette.

---

## Where configs live on disk

| Origin | Directory | Format |
|--------|-----------|--------|
| Workspace SQL templates | `{workspace}/templates/*.sql` or `{workspace}/templates/sql/*.sql` | `.sql` file with optional front-matter |
| Workspace YAML node templates | `{workspace}/node_templates/*.yaml` or `{workspace}/templates/pandas/*.yaml` | YAML with `node_type`, `label`, `description`, `params`, optional `tags` |
| Pipeline-specific configs | `{workspace}/pipelines/{name}/config/*.yaml` or `*.sql` | Same formats; shown with `pipeline` origin badge |
| Builtin templates | `pipeline_service/api/templates.py` `_COMMON` list | Hardcoded Python |

### YAML node template format

```yaml
node_type: pandas_transform
label: Add Revenue Column
description: Multiplies quantity × unit_price to produce a revenue column.
tags:
  - finance
  - revenue
params:
  transform: mypackage.transforms.analysis.add_revenue
```

---

## Saving a node as a config

Right-clicking a canvas node shows **⊞ Save as config…**  The dialog prompts
for a name, description, and scope:

- **Pipeline** — writes to `{workspace}/pipelines/{name}/config/{slug}.yaml`
- **Workspace** — writes to `{workspace}/node_templates/{slug}.yaml`

After saving, the palette refreshes automatically and the new config appears
under the appropriate function row.

---

## API endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /palette?workspace=…` | Full palette response (sources, transforms, sinks) |
| `GET /palette/tags?workspace=…` | All tags with occurrence counts |
| `GET /templates?workspace=…` | Raw template list (legacy; not used by the UI) |
