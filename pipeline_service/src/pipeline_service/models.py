from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel

from pipeline_core.resolver.models import ColumnSchema

__all__ = ["ColumnSchema"]


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class NodeStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------

class RunRequest(BaseModel):
    """Submit a pipeline run."""

    pipeline_yaml: str
    """Raw YAML content of the pipeline file."""

    env_yaml: str | None = None
    """Raw YAML content of the environment file (optional)."""

    variables_yaml: str | None = None
    """Raw YAML content of the variables file (optional)."""

    completed_nodes: list[str] = []
    """Node IDs to treat as already done — skips them in the plan."""

    workspace: str | None = None
    """Absolute path to the workspace root. When set, a run bundle is created."""

    pipeline_path: str | None = None
    """Path to the pipeline YAML file within the workspace (for bundle manifest)."""


class ValidateRequest(BaseModel):
    """Validate a pipeline spec without running it."""

    pipeline_yaml: str
    env_yaml: str | None = None
    variables_yaml: str | None = None
    pipeline_dir: str | None = None
    """When provided, template files are read from this directory so that
    Jinja2 variable references inside SQL templates can be validated."""
    workspace: str | None = None


class DagRequest(BaseModel):
    """Parse a pipeline spec into a ReactFlow-compatible DAG."""

    pipeline_yaml: str
    env_yaml: str | None = None
    variables_yaml: str | None = None


# ---------------------------------------------------------------------------
# Response bodies
# ---------------------------------------------------------------------------

class RunResponse(BaseModel):
    run_id: str
    status: RunStatus
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    bundle_path: str | None = None


class NodeRunResponse(BaseModel):
    node_id: str
    status: NodeStatus
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None


class ValidationResponse(BaseModel):
    valid: bool
    errors: list[str] = []
    warnings: list[str] = []
    """Non-fatal issues — e.g. unresolved Jinja2 variable references that may
    cause template-render failures at execution time."""


# ---------------------------------------------------------------------------
# ReactFlow DAG shapes
# ---------------------------------------------------------------------------

class ReactFlowPosition(BaseModel):
    x: float
    y: float


class ReactFlowNodeData(BaseModel):
    label: str
    node_type: str
    description: str | None = None
    output_schema: list[ColumnSchema] | None = None
    """Inferred output schema for this node (populated from schema file or execute-node)."""


class ReactFlowNode(BaseModel):
    id: str
    data: ReactFlowNodeData
    position: ReactFlowPosition


class ReactFlowEdge(BaseModel):
    id: str
    source: str
    target: str
    contract: list[ColumnSchema] | None = None
    """Data contract on this edge — columns produced by source, consumed by target."""


class DagResponse(BaseModel):
    nodes: list[ReactFlowNode]
    edges: list[ReactFlowEdge]
