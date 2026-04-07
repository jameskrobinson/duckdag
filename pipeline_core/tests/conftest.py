"""Shared fixtures for pipeline_core tests."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml


# ---------------------------------------------------------------------------
# Minimal valid pipeline dict (already resolved — no ${...} references)
# ---------------------------------------------------------------------------

MINIMAL_PIPELINE: dict = {
    "duckdb": {"path": "/data/test.duckdb"},
    "nodes": [
        {
            "id": "create_schema",
            "type": "sql_exec",
            "inputs": [],
            "output": None,
            "template": "create_schema.sql.j2",
            "params": {"schema_name": "sources"},
        },
        {
            "id": "load_raw",
            "type": "load_odbc",
            "inputs": [],
            "output": "sources.raw",
            "template": "load.sql.j2",
            "params": {"odbc_key": "my_db"},
        },
        {
            "id": "transform_a",
            "type": "pandas_transform",
            "inputs": ["sources.raw"],
            "output": "model.result",
            "params": {"transform": "my_transform"},
        },
    ],
}


@pytest.fixture()
def minimal_pipeline() -> dict:
    """A minimal valid pipeline dict (no unresolved variables)."""
    import copy
    return copy.deepcopy(MINIMAL_PIPELINE)


@pytest.fixture()
def pipeline_yaml_file(tmp_path: Path) -> Path:
    """Write a pipeline YAML to a temp file and return its path."""
    raw = {
        "duckdb": {"path": "${env.paths.data_dir}/test.duckdb"},
        "parameters": {
            "period_min": 20120630,
            "rc_list": ["AT", "AU", "BE"],
        },
        "nodes": [
            {
                "id": "load_raw",
                "type": "load_odbc",
                "inputs": [],
                "output": "sources.raw",
                "template": "load.sql.j2",
                "params": {
                    "odbc_key": "export_views",
                    "period_min": "${parameters.period_min}",
                    "rc_list": "${parameters.rc_list}",
                },
            },
            {
                "id": "transform_a",
                "type": "pandas_transform",
                "inputs": ["sources.raw"],
                "output": "model.result",
                "params": {"transform": "my_transform"},
            },
        ],
    }
    p = tmp_path / "pipeline.yaml"
    p.write_text(yaml.dump(raw), encoding="utf-8")
    return p


@pytest.fixture()
def env_dict() -> dict:
    return {
        "paths": {"data_dir": "/data", "logs_dir": "/logs"},
        "odbc": {
            "export_views": {
                "driver": "ODBC Driver 17",
                "server": "myserver",
                "database": "mydb",
                "trusted": True,
            }
        },
    }
