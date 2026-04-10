# Pipeline Variables — How They Work

This document explains every variable mechanism in the pipeline platform: what each one is for, where it lives, how it is referenced in YAML and SQL, and what overrides what.

---

## The four value namespaces

The resolver recognises four distinct namespaces that can be referenced using `${...}` syntax in `pipeline.yaml` and in Jinja2 SQL/MDX templates:

| Namespace | Syntax | Source | Committed to git? |
|-----------|--------|--------|-------------------|
| `parameters` | `${parameters.name}` | Inline in `pipeline.yaml` | Yes |
| `variables` | `${variables.name}` | `variables.yaml` next to `pipeline.yaml` | Yes (defaults only) |
| `env` | `${env.name}` | `env.yaml` next to `pipeline.yaml` | **No** — gitignored |
| Node params | direct value in `params:` | Inline in the node definition | Yes |

---

## Precedence (highest wins)

```
env  >  variables (runtime override)  >  variables (file defaults)  >  variable_declarations defaults  >  parameters
```

More precisely, when the resolver builds the substitution context it does:

```python
context = {
    "parameters": raw["parameters"],          # from pipeline.yaml
    "variables":  {**decl_defaults,           # from variable_declarations[].default
                   **variables_yaml_values,   # from variables.yaml
                   **runtime_overrides},      # from --var CLI flag or Run modal
    "env":        env_yaml_values,            # from env.yaml (never overridden at runtime)
}
```

A `${variables.start_date}` reference resolves using the merged `variables` dict — whichever source provided the value wins according to the order above.

**Node params always win over everything.** If a node's `params:` block contains a literal value, it is used as-is. `${...}` substitution only happens in *string values* that contain the placeholder syntax.

---

## `parameters:` — static pipeline-level constants

```yaml
parameters:
  schema: finance
  top_n: 100
  rc_list:
    - AT
    - AU
    - DE
```

- Lives directly in `pipeline.yaml`.
- Intended for values that are fixed for this pipeline and never change between runs — schema names, magic numbers, static lists.
- Referenced as `${parameters.schema}`, `${parameters.top_n}`, `${parameters.rc_list}`.
- **When to use:** anything that is truly a property of the pipeline definition itself, not a runtime input.
- Committed to git; no mechanism to override at runtime.

---

## `variables.yaml` — run-level defaults

```yaml
# variables.yaml (committed — contains sensible defaults)
start_date: "2024-01-01"
end_date:   "2024-12-31"
country:    "AT"
```

- Lives in the same directory as `pipeline.yaml`.
- Referenced as `${variables.start_date}` in `pipeline.yaml` node params and in SQL/MDX templates.
- **Committed to git as the default values** — the pipeline should run correctly with these values without any manual setup.
- Overridden at runtime by the CLI `--var` flag or by the Run Variables modal in the builder.
- **When to use:** anything that changes between runs — date ranges, filter values, output paths, row limits.

### In SQL templates

Inside a `.sql.j2` template, variables are available as bare Jinja2 names (no `${...}` syntax):

```sql
-- load_prices.sql.j2
SELECT *
FROM prices
WHERE date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  AND country = '{{ country }}'
```

Node `params` take priority over variables in the template render context, so a node can override a variable for its specific template call:

```yaml
nodes:
  - id: load_prices
    type: sql_transform
    template: load_prices.sql.j2
    params:
      country: "DE"          # overrides variables.yaml country for this node only
```

---

## `env.yaml` — machine-local environment settings

```yaml
# env.yaml (NOT committed — gitignored; exists per developer machine / deployment)
paths:
  data_dir: C:/Data/pipeline_data
  output_dir: C:/Data/outputs

odbc_dsns:
  prod_db: "DSN=PROD_SQL;UID=svc_user;PWD=secret"

api:
  base_url: https://internal-api.corp.example.com
  token: eyJhbGci...
```

- Lives in the same directory as `pipeline.yaml`.
- Referenced as `${env.paths.data_dir}`, `${env.odbc_dsns.prod_db}`, etc.
- **Never committed to git** — add `env.yaml` to `.gitignore`. It contains machine paths, DSNs, credentials, and tokens.
- Cannot be overridden at runtime (no `--env` flag by design — it is environment configuration, not a run parameter).
- **When to use:** anything that differs between machines or deployment environments — file system paths, connection strings, API tokens, base URLs.

### Combining env and variables

A common pattern is to compose a full path from a machine-local root (`env`) and a run-level suffix (`variables`):

```yaml
nodes:
  - id: push_results
    type: push_duckdb
    params:
      path: "${env.paths.output_dir}/${variables.end_date}/results.duckdb"
```

---

## `variable_declarations:` — schema for variables

```yaml
variable_declarations:
  - name: start_date
    type: string
    default: "2024-01-01"
    description: "Start of the analysis window (YYYY-MM-DD)"
    required: false

  - name: country
    type: string
    default: null
    description: "ISO 3166-1 alpha-2 country code filter"
    required: true

  - name: top_n
    type: integer
    default: 100
    description: "Number of rows to return from ranking nodes"
    required: false
```

This block does **not** supply values to the resolver directly — it is a schema declaration. Its purposes are:

1. **Default values** — if a variable is not present in `variables.yaml` and not overridden at runtime, the resolver seeds it from `variable_declarations[].default`. This means the pipeline can resolve cleanly even with no `variables.yaml` on disk.

2. **Builder UI** — the Vars panel and Run Variables modal read declarations to show friendly descriptions, enforce types, and flag required variables that have no value.

3. **Validation** — `required: true` variables with no value (no default, not in `variables.yaml`, not passed at runtime) produce a validation warning in the builder and a runtime error in the CLI.

4. **Autocomplete** — the builder reads declarations to drive `${variables.X}` autocomplete in param fields.

**The declarations are not enforced at resolution time for optional variables** — if a variable is declared but absent and has no default, the `${variables.X}` reference is left unresolved (strict mode raises, lenient mode leaves the placeholder).

---

## Runtime variable overrides

### CLI

```bash
pipeline run pipeline.yaml \
  --var start_date=2025-01-01 \
  --var end_date=2025-03-31 \
  --var country=DE
```

`--var` flags are parsed as `KEY=VALUE` pairs and passed to the resolver as the `variables` dict, overriding whatever is in `variables.yaml` for that run only. The file on disk is not modified.

### Builder (Run Variables modal)

When you click **▶ Run**, the builder opens a pre-run modal showing the current `variables.yaml` values as an editable table. Changes apply to that session only and are not written to disk.

### Session re-run

When re-executing an existing session (▶ Run on an active session), the variables from the *original* session creation are reused unless you explicitly update them. The stored `variables_yaml` is shown in the Run Variables modal for confirmation.

---

## `parameters:` vs `variables:` — when to use which

| Criterion | `parameters:` | `variables:` |
|-----------|--------------|-------------|
| Changes between runs? | No | Yes |
| Overridable at runtime? | No | Yes |
| Committed to git? | Yes | Yes (defaults) |
| Typed + declared? | No | Yes (via `variable_declarations`) |
| Typical content | Schema names, static lists, magic numbers | Date ranges, country filters, row limits, output suffixes |

A rule of thumb: if a data analyst running the pipeline for a new reporting period would need to change it, it belongs in `variables`. If it is a structural choice about the pipeline that only a developer would change, it belongs in `parameters`.

---

## Reference syntax summary

### In `pipeline.yaml` node params

```yaml
nodes:
  - id: load_raw
    type: load_duckdb
    params:
      path:   "${env.paths.data_dir}/prices.duckdb"    # machine-local path
      table:  "prices_${variables.country}"             # embedded interpolation
      schema: "${parameters.schema}"                    # static constant
```

- `${...}` syntax only; Jinja2 is not used in the YAML file itself.
- Whole-value substitution (`"${variables.rc_list}"`) preserves the original type (list, int, etc.).
- Embedded interpolation (`"${variables.country}_suffix"`) always produces a string.

### In SQL / MDX templates (`.sql.j2`, `.mdx.j2`)

```sql
-- Jinja2 syntax; variables and node params are merged into the render context
SELECT *
FROM {{ parameters.schema }}.{{ table }}    -- 'table' is a node param
WHERE date >= '{{ start_date }}'            -- 'start_date' from variables
  AND country = '{{ country }}'
```

- Jinja2 `{{ }}` syntax (not `${}`).
- The render context is `{**variables, **node.params}` — node params take priority over variables.
- Both `parameters` (as a dict) and `variables` (as a dict) are also available if needed: `{{ variables.start_date }}`, `{{ parameters.schema }}`.

---

## Where variables are stored in a session

When a session is created, the `variables_yaml` string is stored in the `sessions` table in the service database. This means:

- Re-executing a session uses the original variables automatically.
- Branching from a session (`POST /sessions/branch`) inherits the source session's variables unless you supply new ones in the branch request.
- The bundle manifest does not currently record variables, but the session record does.

---

## Validation and error surfacing

| Condition | Where flagged |
|-----------|--------------|
| `${variables.X}` reference with no value and no default | Amber ⚠ warning in validation banner; orange node border in canvas |
| `required: true` variable with no value | Same as above + error in CLI run |
| `${env.X}` reference with no `env.yaml` on disk | Lenient: left as placeholder; Strict: KeyError at resolve time |
| `${parameters.X}` with no matching key in `parameters:` block | KeyError at resolve time (always strict) |

The builder validates continuously as you edit. The CLI validates at the start of `pipeline run` before any nodes execute.
