from __future__ import annotations

import pytest

from tests.conftest import SIMPLE_PIPELINE_YAML, TWO_NODE_PIPELINE_YAML


# ---------------------------------------------------------------------------
# POST /runs
# ---------------------------------------------------------------------------

def test_create_run_returns_201(client):
    resp = client.post("/runs", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML})
    assert resp.status_code == 201


def test_create_run_returns_run_id(client):
    resp = client.post("/runs", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML})
    body = resp.json()
    assert "run_id" in body
    assert len(body["run_id"]) == 36  # UUID


def test_create_run_invalid_yaml_returns_422(client):
    bad_yaml = "duckdb:\n  path: ':memory:'\nnodes:\n  - id: bad\n    type: NONEXISTENT_TYPE\n"
    resp = client.post("/runs", json={"pipeline_yaml": bad_yaml})
    assert resp.status_code == 422


def test_create_run_cycle_returns_422(client):
    cycle_yaml = """
duckdb:
  path: ":memory:"
nodes:
  - id: a
    type: load_duckdb
    inputs: [t.b]
    output: t.a
    params:
      query: "SELECT 1"
  - id: b
    type: load_duckdb
    inputs: [t.a]
    output: t.b
    params:
      query: "SELECT 1"
"""
    resp = client.post("/runs", json={"pipeline_yaml": cycle_yaml})
    assert resp.status_code == 422


def test_create_run_background_task_completes(client):
    """Background tasks run synchronously in TestClient."""
    resp = client.post("/runs", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML})
    run_id = resp.json()["run_id"]

    status_resp = client.get(f"/runs/{run_id}")
    assert status_resp.json()["status"] == "completed"


def test_create_run_with_completed_nodes_skips_them(client):
    resp = client.post(
        "/runs",
        json={"pipeline_yaml": TWO_NODE_PIPELINE_YAML, "completed_nodes": ["step_a"]},
    )
    run_id = resp.json()["run_id"]

    nodes_resp = client.get(f"/runs/{run_id}/nodes")
    nodes = {n["node_id"]: n["status"] for n in nodes_resp.json()}
    assert nodes["step_a"] == "skipped"
    # step_b depends on t.a which isn't in the store → this run will fail,
    # but the skip status on step_a should be recorded.
    assert nodes["step_a"] == "skipped"


# ---------------------------------------------------------------------------
# GET /runs
# ---------------------------------------------------------------------------

def test_list_runs_empty(client):
    resp = client.get("/runs")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_runs_returns_submitted_runs(client):
    client.post("/runs", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML})
    client.post("/runs", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML})
    resp = client.get("/runs")
    assert len(resp.json()) == 2


# ---------------------------------------------------------------------------
# GET /runs/{run_id}
# ---------------------------------------------------------------------------

def test_get_run_not_found(client):
    resp = client.get("/runs/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404


def test_get_run_has_expected_fields(client):
    run_id = client.post("/runs", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML}).json()["run_id"]
    resp = client.get(f"/runs/{run_id}")
    body = resp.json()
    assert body["run_id"] == run_id
    assert "status" in body
    assert "created_at" in body


# ---------------------------------------------------------------------------
# GET /runs/{run_id}/nodes
# ---------------------------------------------------------------------------

def test_get_nodes_not_found(client):
    resp = client.get("/runs/00000000-0000-0000-0000-000000000000/nodes")
    assert resp.status_code == 404


def test_get_nodes_returns_all_pipeline_nodes(client):
    run_id = client.post("/runs", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML}).json()["run_id"]
    resp = client.get(f"/runs/{run_id}/nodes")
    node_ids = {n["node_id"] for n in resp.json()}
    assert node_ids == {"load", "transform"}


def test_get_nodes_completed_after_successful_run(client):
    run_id = client.post("/runs", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML}).json()["run_id"]
    nodes = {n["node_id"]: n["status"] for n in client.get(f"/runs/{run_id}/nodes").json()}
    assert all(s == "completed" for s in nodes.values())
