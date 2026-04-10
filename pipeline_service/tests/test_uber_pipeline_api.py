"""Tests for GET /workspace/uber-pipeline."""
from __future__ import annotations

from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_pipeline(directory: Path, nodes: list[dict]) -> Path:
    """Write a minimal pipeline.yaml into *directory* and return its path."""
    import yaml
    directory.mkdir(parents=True, exist_ok=True)
    spec = {
        "duckdb": {"path": ":memory:"},
        "nodes": nodes,
    }
    p = directory / "pipeline.yaml"
    p.write_text(yaml.dump(spec), encoding="utf-8")
    return p


def _load_node(path: str) -> dict:
    return {"id": "src", "type": "load_file", "output": "t.raw", "params": {"path": path}}


def _export_node(path: str) -> dict:
    return {"id": "out", "type": "export_dta", "inputs": ["t.raw"], "params": {"path": path}}


def _push_node(path: str) -> dict:
    return {"id": "push", "type": "push_duckdb", "inputs": ["t.raw"],
            "params": {"path": path, "table": "result"}}


def _duckdb_node(query: str = "SELECT 1") -> dict:
    return {"id": "load", "type": "load_duckdb", "output": "t.raw", "params": {"query": query}}


# ---------------------------------------------------------------------------
# Basic discovery
# ---------------------------------------------------------------------------

def test_uber_pipeline_empty_workspace(client, tmp_path):
    """An empty workspace returns an empty pipelines list."""
    ws = tmp_path / "ws"
    ws.mkdir()
    resp = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)})
    assert resp.status_code == 200
    body = resp.json()
    assert body["pipelines"] == []
    assert body["edges"] == []


def test_uber_pipeline_nonexistent_workspace_ignored(client, tmp_path):
    """A workspace path that does not exist is silently skipped."""
    resp = client.get("/workspace/uber-pipeline", params={"workspace": str(tmp_path / "missing")})
    assert resp.status_code == 200
    assert resp.json()["pipelines"] == []


def test_uber_pipeline_discovers_new_layout(client, tmp_path):
    """pipelines/{name}/pipeline.yaml files are discovered."""
    ws = tmp_path / "ws"
    _write_pipeline(ws / "pipelines" / "alpha", [_duckdb_node()])
    _write_pipeline(ws / "pipelines" / "beta", [_duckdb_node()])
    resp = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)})
    assert resp.status_code == 200
    names = {p["pipeline_name"] for p in resp.json()["pipelines"]}
    assert names == {"alpha", "beta"}


def test_uber_pipeline_node_has_required_fields(client, tmp_path):
    ws = tmp_path / "ws"
    _write_pipeline(ws / "pipelines" / "alpha", [_duckdb_node()])
    body = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)}).json()
    node = body["pipelines"][0]
    assert "pipeline_path" in node
    assert "pipeline_name" in node
    assert "workspace" in node
    assert "source_files" in node
    assert "sink_files" in node
    assert "last_run_status" in node


def test_uber_pipeline_last_run_status_never_for_new_pipeline(client, tmp_path):
    ws = tmp_path / "ws"
    _write_pipeline(ws / "pipelines" / "alpha", [_duckdb_node()])
    body = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)}).json()
    assert body["pipelines"][0]["last_run_status"] == "never"


# ---------------------------------------------------------------------------
# Source / sink file extraction
# ---------------------------------------------------------------------------

def test_uber_pipeline_extracts_load_file_source(client, tmp_path):
    ws = tmp_path / "ws"
    data_file = str(tmp_path / "data.csv")
    _write_pipeline(ws / "pipelines" / "alpha", [_load_node(data_file)])
    body = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)}).json()
    node = body["pipelines"][0]
    assert len(node["source_files"]) == 1
    assert node["sink_files"] == []


def test_uber_pipeline_extracts_export_dta_sink(client, tmp_path):
    ws = tmp_path / "ws"
    out_file = str(tmp_path / "output.dta")
    _write_pipeline(ws / "pipelines" / "alpha", [_duckdb_node(), _export_node(out_file)])
    body = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)}).json()
    node = body["pipelines"][0]
    assert len(node["sink_files"]) == 1
    assert node["source_files"] == []


def test_uber_pipeline_extracts_push_duckdb_sink(client, tmp_path):
    ws = tmp_path / "ws"
    db_file = str(tmp_path / "store.duckdb")
    _write_pipeline(ws / "pipelines" / "alpha", [_duckdb_node(), _push_node(db_file)])
    body = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)}).json()
    node = body["pipelines"][0]
    assert len(node["sink_files"]) == 1


def test_uber_pipeline_load_duckdb_node_has_no_files(client, tmp_path):
    """load_duckdb nodes (no params.path) should not appear in source_files."""
    ws = tmp_path / "ws"
    _write_pipeline(ws / "pipelines" / "alpha", [_duckdb_node()])
    body = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)}).json()
    node = body["pipelines"][0]
    assert node["source_files"] == []
    assert node["sink_files"] == []


# ---------------------------------------------------------------------------
# Cross-pipeline edges
# ---------------------------------------------------------------------------

def test_uber_pipeline_edge_created_when_sink_matches_source(client, tmp_path):
    """An edge is built when pipeline A's sink file is pipeline B's source file."""
    ws = tmp_path / "ws"
    shared = str(tmp_path / "shared.dta")
    # Pipeline A exports to shared
    _write_pipeline(ws / "pipelines" / "producer", [_duckdb_node(), _export_node(shared)])
    # Pipeline B loads from shared
    _write_pipeline(ws / "pipelines" / "consumer", [_load_node(shared)])

    body = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)}).json()
    edges = body["edges"]
    assert len(edges) == 1
    edge = edges[0]
    assert edge["resolved"] is True
    # source_pipeline is the producer
    producer_path = next(p["pipeline_path"] for p in body["pipelines"] if p["pipeline_name"] == "producer")
    consumer_path = next(p["pipeline_path"] for p in body["pipelines"] if p["pipeline_name"] == "consumer")
    assert edge["source_pipeline"] == producer_path
    assert edge["target_pipeline"] == consumer_path


def test_uber_pipeline_no_edge_for_unmatched_files(client, tmp_path):
    ws = tmp_path / "ws"
    _write_pipeline(ws / "pipelines" / "alpha", [_duckdb_node(), _export_node(str(tmp_path / "a.dta"))])
    _write_pipeline(ws / "pipelines" / "beta", [_load_node(str(tmp_path / "b.csv"))])
    body = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)}).json()
    assert body["edges"] == []


def test_uber_pipeline_no_self_edges(client, tmp_path):
    """A pipeline that exports and loads the same file should not get a self-edge."""
    ws = tmp_path / "ws"
    shared = str(tmp_path / "loop.dta")
    _write_pipeline(ws / "pipelines" / "alpha", [
        _load_node(shared),
        _export_node(shared),
    ])
    body = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)}).json()
    assert body["edges"] == []


def test_uber_pipeline_edge_has_shared_path(client, tmp_path):
    ws = tmp_path / "ws"
    shared = str(tmp_path / "link.dta")
    _write_pipeline(ws / "pipelines" / "prod", [_duckdb_node(), _export_node(shared)])
    _write_pipeline(ws / "pipelines" / "cons", [_load_node(shared)])
    body = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)}).json()
    assert body["edges"][0]["shared_path"] == shared


# ---------------------------------------------------------------------------
# Jinja variable resolution
# ---------------------------------------------------------------------------

def test_uber_pipeline_jinja_resolved_from_variables_yaml(client, tmp_path):
    """Variables in params.path are resolved via the pipeline's variables.yaml."""
    import yaml as _yaml
    ws = tmp_path / "ws"
    pipeline_dir = ws / "pipelines" / "alpha"
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    # Write variables.yaml into the pipeline directory
    (pipeline_dir / "variables.yaml").write_text(
        _yaml.dump({"data_file": str(tmp_path / "actual.dta")}), encoding="utf-8"
    )
    _write_pipeline(pipeline_dir, [_load_node("{{ data_file }}")])
    body = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)}).json()
    node = body["pipelines"][0]
    # The resolved path should not contain {{ }}
    assert "{{" not in node["source_files"][0]


def test_uber_pipeline_unresolved_jinja_kept_as_literal(client, tmp_path):
    """When a variable is missing, the raw {{ }} string is kept (edge resolved=False)."""
    ws = tmp_path / "ws"
    shared_template = "{{ output_dir }}/out.dta"
    _write_pipeline(ws / "pipelines" / "prod", [_duckdb_node(), _export_node(shared_template)])
    _write_pipeline(ws / "pipelines" / "cons", [_load_node(shared_template)])
    body = client.get("/workspace/uber-pipeline", params={"workspace": str(ws)}).json()
    # Edge should still be created (paths match literally) but resolved=False
    edges = body["edges"]
    assert len(edges) == 1
    assert edges[0]["resolved"] is False


# ---------------------------------------------------------------------------
# Multiple workspaces
# ---------------------------------------------------------------------------

def test_uber_pipeline_multiple_workspaces(client, tmp_path):
    ws1 = tmp_path / "ws1"
    ws2 = tmp_path / "ws2"
    _write_pipeline(ws1 / "pipelines" / "alpha", [_duckdb_node()])
    _write_pipeline(ws2 / "pipelines" / "beta", [_duckdb_node()])
    body = client.get(
        "/workspace/uber-pipeline",
        params=[("workspace", str(ws1)), ("workspace", str(ws2))],
    ).json()
    names = {p["pipeline_name"] for p in body["pipelines"]}
    assert names == {"alpha", "beta"}
    workspaces = {p["workspace"] for p in body["pipelines"]}
    assert str(ws1) in workspaces
    assert str(ws2) in workspaces
