"""Tests for POST/GET /sessions and related session lifecycle endpoints."""
from __future__ import annotations

import pytest

from tests.conftest import SIMPLE_PIPELINE_YAML, TWO_NODE_PIPELINE_YAML

INVALID_YAML = "duckdb:\n  path: ':memory:'\nnodes:\n  - id: x\n    type: BAD_TYPE\n"


# ---------------------------------------------------------------------------
# Helper — create_bundle requires the pipeline YAML to exist on disk
# ---------------------------------------------------------------------------

def _make_workspace(tmp_path, subdir: str = "ws") -> tuple[str, str]:
    """Return (workspace_str, pipeline_path_str) with pipeline.yaml written to disk."""
    ws = tmp_path / subdir
    ws.mkdir(parents=True, exist_ok=True)
    p = ws / "pipeline.yaml"
    p.write_text(SIMPLE_PIPELINE_YAML, encoding="utf-8")
    return str(ws), str(p)


# ---------------------------------------------------------------------------
# POST /sessions
# ---------------------------------------------------------------------------

def test_create_session_returns_201(client, tmp_path):
    ws, pp = _make_workspace(tmp_path)
    resp = client.post("/sessions", json={
        "pipeline_yaml": SIMPLE_PIPELINE_YAML,
        "workspace": ws,
        "pipeline_path": pp,
    })
    assert resp.status_code == 201


def test_create_session_returns_session_id(client, tmp_path):
    ws, pp = _make_workspace(tmp_path)
    resp = client.post("/sessions", json={
        "pipeline_yaml": SIMPLE_PIPELINE_YAML,
        "workspace": ws,
        "pipeline_path": pp,
    })
    body = resp.json()
    assert "session_id" in body
    assert body["session_id"]  # non-empty


def test_create_session_has_expected_fields(client, tmp_path):
    ws, pp = _make_workspace(tmp_path)
    resp = client.post("/sessions", json={
        "pipeline_yaml": SIMPLE_PIPELINE_YAML,
        "workspace": ws,
        "pipeline_path": pp,
    })
    body = resp.json()
    assert "status" in body
    assert "created_at" in body
    assert "bundle_path" in body
    assert "probe_status" in body


def test_create_session_invalid_yaml_returns_422(client, tmp_path):
    ws, pp = _make_workspace(tmp_path)
    resp = client.post("/sessions", json={
        "pipeline_yaml": INVALID_YAML,
        "workspace": ws,
        "pipeline_path": pp,
    })
    assert resp.status_code == 422


def test_create_session_executes_in_background(client, tmp_path):
    """TestClient runs background tasks synchronously — nodes should be completed."""
    ws, pp = _make_workspace(tmp_path)
    resp = client.post("/sessions", json={
        "pipeline_yaml": SIMPLE_PIPELINE_YAML,
        "workspace": ws,
        "pipeline_path": pp,
    })
    session_id = resp.json()["session_id"]
    status_resp = client.get(f"/sessions/{session_id}")
    # After background task completes, session stays 'active' (open for re-execution)
    assert status_resp.json()["status"] in ("active", "running", "completed")


def test_create_session_missing_workspace_returns_422(client):
    resp = client.post("/sessions", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML})
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /sessions
# ---------------------------------------------------------------------------

def test_list_sessions_empty(client):
    resp = client.get("/sessions")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_sessions_returns_created_sessions(client, tmp_path):
    ws1, pp1 = _make_workspace(tmp_path, "ws1")
    ws2, pp2 = _make_workspace(tmp_path, "ws2")
    client.post("/sessions", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML, "workspace": ws1, "pipeline_path": pp1})
    client.post("/sessions", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML, "workspace": ws2, "pipeline_path": pp2})
    resp = client.get("/sessions")
    assert len(resp.json()) == 2


# ---------------------------------------------------------------------------
# GET /sessions/{id}
# ---------------------------------------------------------------------------

def test_get_session_not_found(client):
    resp = client.get("/sessions/nonexistent-session-id")
    assert resp.status_code == 404


def test_get_session_has_probe_status_field(client, tmp_path):
    ws, pp = _make_workspace(tmp_path)
    session_id = client.post("/sessions", json={
        "pipeline_yaml": SIMPLE_PIPELINE_YAML,
        "workspace": ws,
        "pipeline_path": pp,
    }).json()["session_id"]
    body = client.get(f"/sessions/{session_id}").json()
    assert "probe_status" in body
    # Probe has not been triggered — should be null
    assert body["probe_status"] is None


# ---------------------------------------------------------------------------
# GET /sessions/{id}/nodes
# ---------------------------------------------------------------------------

def test_get_session_nodes_not_found(client):
    resp = client.get("/sessions/nonexistent/nodes")
    assert resp.status_code == 404


def test_get_session_nodes_returns_all_pipeline_nodes(client, tmp_path):
    ws, pp = _make_workspace(tmp_path)
    session_id = client.post("/sessions", json={
        "pipeline_yaml": SIMPLE_PIPELINE_YAML,
        "workspace": ws,
        "pipeline_path": pp,
    }).json()["session_id"]
    resp = client.get(f"/sessions/{session_id}/nodes")
    assert resp.status_code == 200
    node_ids = {n["node_id"] for n in resp.json()}
    assert node_ids == {"load", "transform"}


def test_get_session_nodes_completed_after_run(client, tmp_path):
    ws, pp = _make_workspace(tmp_path)
    session_id = client.post("/sessions", json={
        "pipeline_yaml": SIMPLE_PIPELINE_YAML,
        "workspace": ws,
        "pipeline_path": pp,
    }).json()["session_id"]
    nodes = {n["node_id"]: n["status"] for n in client.get(f"/sessions/{session_id}/nodes").json()}
    assert all(s == "completed" for s in nodes.values())


# ---------------------------------------------------------------------------
# POST /sessions/{id}/abandon
# ---------------------------------------------------------------------------

def test_abandon_session(client, tmp_path):
    ws, pp = _make_workspace(tmp_path)
    session_id = client.post("/sessions", json={
        "pipeline_yaml": SIMPLE_PIPELINE_YAML,
        "workspace": ws,
        "pipeline_path": pp,
    }).json()["session_id"]
    resp = client.post(f"/sessions/{session_id}/abandon", json={})
    assert resp.status_code == 200
    assert resp.json()["status"] == "abandoned"


def test_abandon_nonexistent_session_returns_404(client):
    resp = client.post("/sessions/nonexistent/abandon", json={})
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# One-active-per-pipeline enforcement
# ---------------------------------------------------------------------------

def test_second_session_for_same_pipeline_returns_409(client, tmp_path):
    """Creating a second active session for the same pipeline_path must fail with 409."""
    ws, pp = _make_workspace(tmp_path, "ws1")

    client.post("/sessions", json={
        "pipeline_yaml": SIMPLE_PIPELINE_YAML,
        "workspace": ws,
        "pipeline_path": pp,
    })
    # Second create for the same pipeline_path (before abandon/finalize)
    ws2, _ = _make_workspace(tmp_path, "ws2")
    resp = client.post("/sessions", json={
        "pipeline_yaml": SIMPLE_PIPELINE_YAML,
        "workspace": ws2,
        "pipeline_path": pp,  # same pipeline path
    })
    assert resp.status_code == 409


def test_second_session_allowed_after_abandon(client, tmp_path):
    ws, pp = _make_workspace(tmp_path, "ws1")
    session_id = client.post("/sessions", json={
        "pipeline_yaml": SIMPLE_PIPELINE_YAML,
        "workspace": ws,
        "pipeline_path": pp,
    }).json()["session_id"]

    client.post(f"/sessions/{session_id}/abandon", json={})

    # Re-use the same workspace (pipeline_path is inside ws)
    resp = client.post("/sessions", json={
        "pipeline_yaml": SIMPLE_PIPELINE_YAML,
        "workspace": ws,
        "pipeline_path": pp,
    })
    assert resp.status_code == 201
