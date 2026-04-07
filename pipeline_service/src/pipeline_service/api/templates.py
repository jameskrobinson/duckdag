"""Node template endpoint — pre-filled node configs draggable from the palette.

Templates are split into two scopes:
  common — ships with the service; useful patterns any pipeline can use.
  local  — discovered from the workspace:
             {workspace}/templates/*.sql   → sql_transform / sql_exec leaves
             {workspace}/node_templates/*.yaml → any node type
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Literal

import yaml
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

class NodeTemplate(BaseModel):
    id: str
    node_type: str
    label: str
    description: str
    scope: Literal["common", "local", "config", "pipeline"]
    """
    common  — built-in templates shipped with the service.
    local   — workspace-level templates from {workspace}/templates/ or node_templates/.
    config  — pipeline-specific solidified configs from {workspace}/pipelines/{name}/config/.
    pipeline — live canvas node configs (generated client-side, not persisted here).
    """
    params: dict[str, Any] = {}
    template_file: str | None = None
    """Filename relative to the workspace templates dir (e.g. 'sort_movers.sql')."""
    template_path: str | None = None
    """Absolute path to the template file (set for local templates)."""
    sql_preview: str | None = None
    """First ~300 chars of SQL for display in the palette tooltip."""


# ---------------------------------------------------------------------------
# Common templates — hardcoded, shipped with the service
# ---------------------------------------------------------------------------

_COMMON: list[NodeTemplate] = [
    # ── Load ────────────────────────────────────────────────────────────────
    NodeTemplate(
        id="common/load_rest_get",
        node_type="load_rest_api",
        label="Load REST API (GET)",
        description="HTTP GET that returns a JSON array; use 'params' for query string args.",
        scope="common",
        params={
            "url": "https://example.com/api/data",
            "method": "GET",
            "params": {},
            "timeout": 30,
        },
    ),
    NodeTemplate(
        id="common/load_coingecko_markets",
        node_type="load_rest_api",
        label="CoinGecko: Top 20 Coins",
        description="Fetch top 20 cryptocurrencies by market cap from CoinGecko (no API key required).",
        scope="common",
        params={
            "url": "https://api.coingecko.com/api/v3/coins/markets",
            "method": "GET",
            "params": {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 20,
                "page": 1,
                "sparkline": False,
            },
            "timeout": 20,
        },
    ),
    NodeTemplate(
        id="common/load_world_bank",
        node_type="load_rest_api",
        label="World Bank: Indicator Series",
        description="Fetch a World Bank indicator time series (e.g. NY.GDP.MKTP.CD for GDP).",
        scope="common",
        params={
            "url": "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD",
            "method": "GET",
            "params": {"format": "json", "per_page": 500, "mrv": 5},
            "timeout": 30,
            "json_path": "1",
        },
    ),
    NodeTemplate(
        id="common/load_csv",
        node_type="load_csv",
        label="Load CSV file",
        description="Read a local CSV file into a DataFrame.",
        scope="common",
        params={"path": "data/input.csv"},
    ),
    # ── Transform (pandas) ──────────────────────────────────────────────────
    NodeTemplate(
        id="common/pandas_select_rename",
        node_type="pandas_transform",
        label="Select & Rename",
        description="Keep a subset of columns and rename them to canonical names.",
        scope="common",
        params={
            "transform": "pipeline_core.transforms.basic.select_rename",
            "select": ["col_a", "col_b"],
            "rename": {"col_a": "id", "col_b": "value"},
        },
    ),
    NodeTemplate(
        id="common/pandas_cast_columns",
        node_type="pandas_transform",
        label="Cast Column Types",
        description="Cast one or more columns to a target dtype (e.g. float, int, str).",
        scope="common",
        params={
            "transform": "pipeline_core.transforms.basic.cast_columns",
            "casts": {"amount": "float", "created_at": "datetime64[ns]"},
        },
    ),
    NodeTemplate(
        id="common/pandas_map_values",
        node_type="pandas_transform",
        label="Map Values",
        description="Replace values in one or more columns using a lookup dict.",
        scope="common",
        params={
            "transform": "pipeline_core.transforms.basic.map_values",
            "column": "status_code",
            "mapping": {"1": "active", "0": "inactive"},
        },
    ),
    NodeTemplate(
        id="common/pandas_derive_columns",
        node_type="pandas_transform",
        label="Derive Columns",
        description="Add computed columns using pandas eval() expressions.",
        scope="common",
        params={
            "transform": "pipeline_core.transforms.basic.derive_columns",
            "expressions": {"full_name": "first_name + ' ' + last_name"},
        },
    ),
    NodeTemplate(
        id="common/pandas_reorder_columns",
        node_type="pandas_transform",
        label="Reorder Columns",
        description="Reorder DataFrame columns into a specified sequence.",
        scope="common",
        params={
            "transform": "pipeline_core.transforms.basic.reorder_columns",
            "order": ["id", "name", "value", "created_at"],
        },
    ),
    # ── SQL ─────────────────────────────────────────────────────────────────
    NodeTemplate(
        id="common/sql_filter_nulls",
        node_type="sql_transform",
        label="Filter nulls",
        description="Remove rows where a key column is NULL.",
        scope="common",
        params={"query": "SELECT * FROM {{input}} WHERE key_column IS NOT NULL"},
        sql_preview="SELECT *\nFROM {{input}}\nWHERE key_column IS NOT NULL",
    ),
    NodeTemplate(
        id="common/sql_sort_desc",
        node_type="sql_transform",
        label="Sort descending",
        description="Sort rows by a numeric column, largest first.",
        scope="common",
        params={"query": "SELECT * FROM {{input}} ORDER BY amount DESC"},
        sql_preview="SELECT *\nFROM {{input}}\nORDER BY amount DESC",
    ),
    NodeTemplate(
        id="common/sql_top_n",
        node_type="sql_transform",
        label="Top N rows",
        description="Return the top N rows after ordering by a column.",
        scope="common",
        params={"query": "SELECT * FROM {{input}} ORDER BY score DESC LIMIT 100"},
        sql_preview="SELECT *\nFROM {{input}}\nORDER BY score DESC\nLIMIT 100",
    ),
    NodeTemplate(
        id="common/sql_deduplicate",
        node_type="sql_transform",
        label="Deduplicate",
        description="Keep one row per unique key using ROW_NUMBER.",
        scope="common",
        params={"query": (
            "SELECT * FROM (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn\n"
            "  FROM {{input}}\n"
            ") WHERE rn = 1"
        )},
        sql_preview=(
            "SELECT * FROM (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn\n"
            "  FROM {{input}}\n"
            ") WHERE rn = 1"
        ),
    ),
    # ── Export ──────────────────────────────────────────────────────────────
    NodeTemplate(
        id="common/sql_export_csv",
        node_type="sql_exec",
        label="Export to CSV",
        description="Write a named store table to a CSV file using DuckDB COPY.",
        scope="common",
        params={"query": "COPY (SELECT * FROM \"_store_node_id\") TO 'output.csv' (FORMAT CSV, HEADER TRUE)"},
        sql_preview="COPY (SELECT * FROM \"_store_node_id\")\nTO 'output.csv' (FORMAT CSV, HEADER TRUE);",
    ),
]


# ---------------------------------------------------------------------------
# Local template discovery
# ---------------------------------------------------------------------------

_SQL_EXEC_KEYWORDS = re.compile(
    r"\b(COPY|INSERT\s+INTO|CREATE\s+(OR\s+REPLACE\s+)?TABLE|DROP\s+TABLE)\b",
    re.IGNORECASE,
)


def _sql_node_type(sql_text: str) -> str:
    """Heuristic: files containing DML/DDL statements become sql_exec nodes."""
    return "sql_exec" if _SQL_EXEC_KEYWORDS.search(sql_text) else "sql_transform"


def _local_from_sql_files(templates_dir: Path, id_prefix: str = "local/sql") -> list[NodeTemplate]:
    """Discover SQL templates from a directory.

    Scans both the directory itself and its ``sql/`` subdirectory (new layout).
    """
    results: list[NodeTemplate] = []
    # Scan both the flat root and the nested sql/ subdirectory
    scan_dirs = [templates_dir, templates_dir / "sql"]
    seen_stems: set[str] = set()
    for scan_dir in scan_dirs:
        if not scan_dir.is_dir():
            continue
        for sql_file in sorted(scan_dir.glob("*.sql")):
            if sql_file.stem in seen_stems:
                continue
            seen_stems.add(sql_file.stem)
            try:
                sql_text = sql_file.read_text(encoding="utf-8")
            except OSError:
                continue
            node_type = _sql_node_type(sql_text)
            preview = sql_text[:400].strip()
            results.append(NodeTemplate(
                id=f"{id_prefix}/{sql_file.stem}",
                node_type=node_type,
                label=sql_file.stem.replace("_", " ").title(),
                description=f"SQL template from {sql_file.name}",
                scope="local",
                params={},
                template_file=sql_file.name,
                template_path=str(sql_file),
                sql_preview=preview,
            ))
    return results


def _local_from_yaml_files(node_templates_dir: Path, id_prefix: str = "local/yaml") -> list[NodeTemplate]:
    """Discover YAML node templates from a directory.

    Scans the directory itself and, for new-layout workspaces, also
    ``templates/pandas/`` and ``templates/api/`` subdirectories.
    """
    results: list[NodeTemplate] = []
    if not node_templates_dir.is_dir():
        return results
    for yaml_file in sorted(node_templates_dir.rglob("*.yaml")):
        # Skip files that look like pipeline configs, not node templates
        if yaml_file.stem.lower() in ("pipeline", "env", "variables"):
            continue
        try:
            raw = yaml.safe_load(yaml_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(raw, dict) or "node_type" not in raw:
            continue
        # Build a stable id from the relative path within the templates dir
        rel = yaml_file.relative_to(node_templates_dir)
        template_id = f"{id_prefix}/{rel.with_suffix('').as_posix().replace('/', '_')}"
        results.append(NodeTemplate(
            id=template_id,
            node_type=raw["node_type"],
            label=raw.get("label", yaml_file.stem.replace("_", " ").title()),
            description=raw.get("description", ""),
            scope="local",
            params=raw.get("params", {}),
            template_file=raw.get("template_file"),
            template_path=str(
                node_templates_dir / raw["template_file"]
            ) if raw.get("template_file") else None,
        ))
    return results


def _pipeline_configs(workspace: Path) -> list[NodeTemplate]:
    """Discover pipeline-specific config files from ``pipelines/*/config/``.

    These are solidified templates — already fully configured for a specific
    pipeline. SQL files become sql_transform/sql_exec templates; YAML files
    become node templates. They are shown in the palette under the pipeline name.
    """
    results: list[NodeTemplate] = []
    pipelines_dir = workspace / "pipelines"
    if not pipelines_dir.is_dir():
        return results
    for pipeline_dir in sorted(pipelines_dir.iterdir()):
        if not pipeline_dir.is_dir():
            continue
        config_dir = pipeline_dir / "config"
        if not config_dir.is_dir():
            continue
        pipeline_name = pipeline_dir.name
        sql_tmpl = _local_from_sql_files(config_dir, id_prefix=f"pipeline/{pipeline_name}/sql")
        yaml_tmpl = _local_from_yaml_files(config_dir, id_prefix=f"pipeline/{pipeline_name}/yaml")
        # Re-tag these as "config" scope so the palette can show them separately
        for tmpl in sql_tmpl + yaml_tmpl:
            tmpl.scope = "config"  # type: ignore[assignment]
        results.extend(sql_tmpl)
        results.extend(yaml_tmpl)
    return results


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get("", response_model=list[NodeTemplate])
def list_templates(workspace: str | None = None) -> list[NodeTemplate]:
    """Return all available node templates (common + local).

    Common templates ship with the service. Local templates are discovered
    from the workspace. Supports two layouts:

    **New layout** (preferred):
      - ``{workspace}/templates/sql/*.sql``       → sql_transform / sql_exec workspace templates
      - ``{workspace}/templates/api/*.yaml``      → api node templates
      - ``{workspace}/templates/pandas/*.yaml``   → pandas node templates
      - ``{workspace}/node_templates/*.yaml``     → any node type (backward compat)
      - ``{workspace}/pipelines/{name}/config/``  → pipeline-specific solidified configs

    **Legacy/flat layout** (backward compatible):
      - ``{workspace}/templates/*.sql``           → sql_transform / sql_exec
      - ``{workspace}/node_templates/*.yaml``     → any node type
    """
    templates: list[NodeTemplate] = list(_COMMON)

    if workspace:
        root = Path(workspace)
        # SQL templates: flat templates/ and nested templates/sql/
        templates.extend(_local_from_sql_files(root / "templates"))
        # YAML node templates: node_templates/ (all layouts) and templates/pandas/, templates/api/
        templates.extend(_local_from_yaml_files(root / "node_templates"))
        templates.extend(_local_from_yaml_files(root / "templates" / "pandas", id_prefix="local/pandas"))
        templates.extend(_local_from_yaml_files(root / "templates" / "api", id_prefix="local/api"))
        # Pipeline-specific solidified configs (new layout only)
        templates.extend(_pipeline_configs(root))

    return templates
