from __future__ import annotations

from tests.conftest import SIMPLE_PIPELINE_YAML


INVALID_YAML = "duckdb:\n  path: ':memory:'\nnodes:\n  - id: x\n    type: BAD_TYPE\n"

DIAMOND_YAML = """
duckdb:
  path: ":memory:"
nodes:
  - id: root
    type: load_duckdb
    output: t.root
    params:
      query: "SELECT 1 AS v"
  - id: left
    type: load_duckdb
    inputs: [t.root]
    output: t.left
    params:
      query: 'SELECT v FROM "t.root"'
  - id: right
    type: load_duckdb
    inputs: [t.root]
    output: t.right
    params:
      query: 'SELECT v FROM "t.root"'
  - id: sink
    type: load_duckdb
    inputs: [t.left, t.right]
    output: t.sink
    params:
      query: 'SELECT v FROM "t.left"'
"""


# ---------------------------------------------------------------------------
# POST /pipelines/validate
# ---------------------------------------------------------------------------

def test_validate_valid_pipeline(client):
    resp = client.post("/pipelines/validate", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML})
    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is True
    assert body["errors"] == []


def test_validate_invalid_type(client):
    resp = client.post("/pipelines/validate", json={"pipeline_yaml": INVALID_YAML})
    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert len(body["errors"]) > 0


def test_validate_dangling_input(client):
    dangling = """
duckdb:
  path: ":memory:"
nodes:
  - id: a
    type: load_duckdb
    inputs: [t.nonexistent]
    output: t.a
    params:
      query: "SELECT 1"
"""
    resp = client.post("/pipelines/validate", json={"pipeline_yaml": dangling})
    assert resp.json()["valid"] is False


# ---------------------------------------------------------------------------
# POST /pipelines/dag
# ---------------------------------------------------------------------------

def test_dag_returns_nodes_and_edges(client):
    resp = client.post("/pipelines/dag", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML})
    assert resp.status_code == 200
    body = resp.json()
    assert "nodes" in body
    assert "edges" in body


def test_dag_node_ids_match_pipeline(client):
    resp = client.post("/pipelines/dag", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML})
    node_ids = {n["id"] for n in resp.json()["nodes"]}
    assert node_ids == {"load", "transform"}


def test_dag_edge_connects_producer_to_consumer(client):
    resp = client.post("/pipelines/dag", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML})
    edges = resp.json()["edges"]
    assert len(edges) == 1
    assert edges[0]["source"] == "load"
    assert edges[0]["target"] == "transform"


def test_dag_node_has_reactflow_fields(client):
    resp = client.post("/pipelines/dag", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML})
    node = resp.json()["nodes"][0]
    assert "id" in node
    assert "data" in node
    assert "position" in node
    assert "x" in node["position"]
    assert "y" in node["position"]
    assert "label" in node["data"]
    assert "node_type" in node["data"]


def test_dag_diamond_has_correct_edge_count(client):
    resp = client.post("/pipelines/dag", json={"pipeline_yaml": DIAMOND_YAML})
    # root→left, root→right, left→sink, right→sink = 4 edges
    assert len(resp.json()["edges"]) == 4


def test_dag_linear_layout_increases_x_with_depth(client):
    resp = client.post("/pipelines/dag", json={"pipeline_yaml": SIMPLE_PIPELINE_YAML})
    nodes = {n["id"]: n for n in resp.json()["nodes"]}
    # 'load' has no inputs → level 0; 'transform' depends on it → level 1
    assert nodes["load"]["position"]["x"] < nodes["transform"]["position"]["x"]


def test_dag_invalid_yaml_returns_422(client):
    resp = client.post("/pipelines/dag", json={"pipeline_yaml": INVALID_YAML})
    assert resp.status_code == 422
