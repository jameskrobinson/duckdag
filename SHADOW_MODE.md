# Shadow Mode

Shadow mode lets you run an alternative implementation of a pipeline node alongside the primary one, then automatically diff the outputs. Use it to validate a rewrite, compare two SQL approaches, or regression-test after a refactor — without touching the main pipeline.

Shadow execution is opt-in. Normal runs are completely unaffected.

---

## How it works

1. You define one or more **shadow nodes** in a `pipeline.shadow.yaml` file that sits next to `pipeline.yaml`.
2. When you run in shadow mode, each shadow node receives the same inputs as its primary counterpart and executes independently.
3. The outputs are diffed via a FULL OUTER JOIN on the **key columns** you specify.
4. Results — matched rows, breaches, per-column max diffs — are written to the `shadow` schema in `session.duckdb` and surfaced in the GUI or CLI summary.

---

## Defining a shadow node

Create `pipeline.shadow.yaml` in the same directory as `pipeline.yaml`. Each top-level key is the `node_id` of the primary node being shadowed:

```yaml
# pipeline.shadow.yaml

sales_transform:
  type: sql_transform
  template: shadow_sales.sql.j2
  key_columns: [order_id, product_id]
  on_breach: warn
  default_tolerance:
    absolute: 0.01       # allow up to 0.01 absolute diff on numeric columns
    relative: 0.001      # or up to 0.1% relative diff
  compare_row_count: true
  row_count_tolerance_pct: 0.0

margin_calc:
  type: pandas_transform
  params:
    transform: transforms.finance.calc_margin_v2
  key_columns: [order_id]
  on_breach: fail_node
```

### Key fields

| Field | Required | Description |
|---|---|---|
| `type` | Yes | Node type for the shadow implementation (e.g. `sql_transform`, `pandas_transform`) |
| `key_columns` | **Yes** | Column(s) to join primary and shadow outputs on. At least one is required. |
| `on_breach` | No | What to do when a diff breach is found: `warn` (default), `fail_node`, or `fail_pipeline` |
| `default_tolerance` | No | Tolerances applied to all numeric columns not explicitly listed in `tolerances` |
| `tolerances` | No | Per-column overrides: `{ my_col: { absolute: 0.5 } }` |
| `compare_row_count` | No | Whether a row count mismatch counts as a breach (default: `true`) |
| `row_count_tolerance_pct` | No | Fraction of row count difference allowed before breaching (default: `0.0`) |
| `preprocess_sql` | No | DuckDB SQL run against the shadow node's input before execution. The input is registered as a view called `input`. Single-input nodes only. |
| `postprocess_sql` | No | DuckDB SQL run against the shadow node's output before diffing. The output is registered as a view called `output`. |

### `on_breach` behaviour

| Value | Effect |
|---|---|
| `warn` | Logs the breach; pipeline continues normally |
| `fail_node` | Marks the primary node as failed; downstream nodes are skipped |
| `fail_pipeline` | Aborts the entire run immediately |

---

## Using shadow mode in the GUI

### Adding a shadow node

1. Open a pipeline from the workspace.
2. Click a node on the canvas to open its config panel.
3. Click **⊛ Shadow** in the panel footer.
4. Fill in the form:
   - **Node type** — the shadow implementation's type
   - **Key columns** — comma-separated column names to join on (required)
   - **On breach** — what happens when outputs differ
   - **Default tolerance** — numeric thresholds (absolute and/or relative)
   - **Pre-process SQL** / **Post-process SQL** — optional transforms applied before/after the shadow node runs
5. Click **Save shadow**. The entry is written to `pipeline.shadow.yaml` immediately.

Once saved, the node shows a purple **⊛** badge on the canvas.

### Running in shadow mode

Shadow mode must currently be enabled at the API/session level (a UI toggle in the Run modal is planned). To run in shadow mode from the GUI for now, the session needs to be created with `shadow_mode: true` — see the CLI section below for the simplest path.

### Viewing diff results

After a shadow-mode session completes:

- Nodes with breaches show a red **⚠ shadow** badge on the canvas.
- Click the node → **⊛ Diff** in the footer to load the diff results inline:
  - A summary grid showing primary/shadow row counts, matched rows, and breach count.
  - Per-column maximum diffs.
  - A sample of breach rows from the diff table, with `_diff_status` highlighted.

---

## Using shadow mode via the CLI

```bash
pipeline run pipeline.yaml --shadow
```

This loads `pipeline.shadow.yaml` from the same directory, runs all shadow nodes, and prints a summary table on completion:

```
[shadow] loaded 2 shadow node(s): sales_transform, margin_calc

[ok] Pipeline completed in 4.31s

Shadow diff summary:
  NODE                          STATUS        PRIMARY     SHADOW    MATCHED  BREACHES
  ────────────────────────────────────────────────────────────────────────────────────
  sales_transform               pass           50,000     50,000     50,000  0
  margin_calc                   breach         50,000     50,000     49,812  188
```

Status is colour-coded: green = pass, amber = warn, red = breach, grey = not run.

### With verbose output

```bash
pipeline run pipeline.yaml --shadow --verbose
```

Per-node timing is shown as usual, with a **⊛** marker next to shadow-enabled nodes.

### With variable overrides

```bash
pipeline run pipeline.yaml --shadow --var start_date=2024-01-01 --var country=UK
```

Variables are passed to both primary and shadow nodes.

### Combining with `--node`

```bash
pipeline run pipeline.yaml --shadow --node margin_calc
```

Runs only `margin_calc` and its ancestors. Shadow mode applies only to nodes in the filtered plan that have shadow entries.

---

## Inspecting raw diff results

Shadow outputs are stored in the `shadow` schema inside `session.duckdb` (or the run bundle's copy). You can query them directly:

```sql
-- Summary for a node
SELECT * FROM shadow.margin_calc_summary;

-- All breach rows
SELECT * FROM shadow.margin_calc_diff
WHERE _diff_status = 'breach';

-- Rows present in primary but missing from shadow
SELECT * FROM shadow.margin_calc_diff
WHERE _diff_status = 'primary_only';
```

Table names use underscores in place of hyphens and dots in the node ID.
