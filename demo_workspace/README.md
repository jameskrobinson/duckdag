# Demo Workspace

A demo workspace for the Pipeline Builder, showcasing new features.
Set this directory as your workspace in the Pipeline Builder toolbar.

## Pipelines

### retail_analysis  ← start here
Fully runnable end-to-end pipeline using local CSV data.

**What to try:**
1. Open the pipeline (Load → retail_analysis)
2. Click any `sql_transform` node — the SQL editor opens with the template pre-loaded
3. Edit the SQL and click **▶ Run** in the editor to preview results interactively
4. Open ⚙ Vars to change `start_date` or `top_n`, then re-run
5. Click **▶ Run** in the toolbar to execute the full pipeline
6. After running, click nodes to inspect outputs and use the Table/Chart tabs

### source_demo  ← shows new source node UIs
Demonstrates the SQL editor on `load_duckdb` and `load_odbc` nodes.
Not runnable without additional infrastructure, but shows the UI.

**What to try:**
1. Click the **query_recent_sales** node (load_duckdb)
   - SQL editor shows the template, pre-linked to `templates/query_recent_sales.sql.j2`
   - After running `retail_analysis` once, click ▶ Run to query its DuckDB file
2. Click the **load_customers** node (load_odbc)
   - Connection params are inline (driver, server, database, uid, pwd, trusted)
   - Sensitive values reference `${env.sql_server}` etc. — put these in `env.yaml`
   - SQL editor shows the template with `{{ region }}` variable injection
3. Click **✦ New** in the toolbar to create a brand-new pipeline from scratch

## Adding a new pipeline
Click **✦ New** in the toolbar, enter a name, and the scaffold is created automatically.
Drag source nodes onto the canvas — the SQL editor is available immediately.
Click **Save** in the SQL editor to be prompted for a template filename.
