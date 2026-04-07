from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pipeline_service.api import create_app

# Minimal pipeline YAML that runs without any external dependencies.
SIMPLE_PIPELINE_YAML = """
duckdb:
  path: ":memory:"
nodes:
  - id: load
    type: load_duckdb
    output: t.raw
    params:
      query: "SELECT 1 AS x"
  - id: transform
    type: load_duckdb
    inputs: [t.raw]
    output: t.out
    params:
      query: "SELECT 2 AS y"
"""

TWO_NODE_PIPELINE_YAML = """
duckdb:
  path: ":memory:"
nodes:
  - id: step_a
    type: load_duckdb
    output: t.a
    params:
      query: "SELECT 10 AS val"
  - id: step_b
    type: load_duckdb
    inputs: [t.a]
    output: t.b
    params:
      query: "SELECT 11 AS val"
"""


@pytest.fixture()
def client(tmp_path):
    """TestClient backed by a fresh temp DuckDB — background tasks run synchronously."""
    app = create_app(db_path=str(tmp_path / "test.duckdb"))
    with TestClient(app) as c:
        yield c
