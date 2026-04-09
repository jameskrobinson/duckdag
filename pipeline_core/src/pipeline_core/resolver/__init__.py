from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import yaml

from pipeline_core.resolver.loader import load_yaml, resolve_variables
from pipeline_core.resolver.models import NodeOutputSchema, PipelineSchema, PipelineSpec, VariableDeclaration
from pipeline_core.resolver.validator import check_dag

__all__ = [
    "resolve_pipeline",
    "resolve_pipeline_from_str",
    "PipelineSpec",
    "PipelineSchema",
    "NodeOutputSchema",
    "VariableDeclaration",
]


def _load_pipeline_schema(spec: PipelineSpec, base_dir: Path | None) -> PipelineSchema | None:
    """Load the companion schema file if schema_path is set and the file exists."""
    if spec.schema_path is None:
        return None
    schema_file = Path(spec.schema_path)
    if not schema_file.is_absolute() and base_dir is not None:
        schema_file = base_dir / schema_file
    if not schema_file.exists():
        return None
    raw = json.loads(schema_file.read_text(encoding="utf-8"))
    return {node_id: NodeOutputSchema.model_validate(v) for node_id, v in raw.items()}


def _get_git_info(path: Path) -> tuple[str | None, bool]:
    """Return (commit_hash, has_uncommitted_changes) for the repo containing path."""
    try:
        cwd = str(path.parent)
        git_hash = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=cwd,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        dirty = subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=cwd,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return git_hash, bool(dirty)
    except Exception:
        return None, False


def resolve_pipeline(
    pipeline_path: Path | str,
    *,
    env_path: Path | str | None = None,
    env: dict[str, Any] | None = None,
    variables_path: Path | str | None = None,
    variables: dict[str, Any] | None = None,
    skip_git: bool = False,
) -> PipelineSpec:
    """Load, resolve, validate, and return a PipelineSpec.

    ``pipeline_path`` may be either the path to a ``pipeline.yaml`` file or
    the path to a pipeline *directory* (new workspace layout: ``pipelines/{name}/``).
    When a directory is given, ``pipeline.yaml`` inside it is used.

    Args:
        pipeline_path: Path to a pipeline YAML file or pipeline directory.
        env_path: Path to an environment YAML file (mutually exclusive with ``env``).
        env: Pre-loaded environment dict (mutually exclusive with ``env_path``).
        variables_path: Path to a variables YAML file (mutually exclusive with ``variables``).
        variables: Pre-loaded variables dict (mutually exclusive with ``variables_path``).
        skip_git: Skip git introspection. Useful in tests and CI environments
                  where the pipeline file may not be inside a git repo.

    Returns:
        A validated :class:`PipelineSpec` with all ``${...}`` references resolved.

    Raises:
        ValueError: If both ``env_path`` and ``env`` are provided.
        KeyError: If a ``${...}`` reference cannot be resolved.
        pydantic.ValidationError: If the resolved spec fails schema validation.
        ValueError: If the DAG contains cycles, duplicate outputs, or dangling inputs.
    """
    if env_path is not None and env is not None:
        raise ValueError("Provide env_path or env, not both")
    if variables_path is not None and variables is not None:
        raise ValueError("Provide variables_path or variables, not both")

    pipeline_path = Path(pipeline_path)
    # Pipeline-as-directory: if a directory is given, look for pipeline.yaml inside it.
    if pipeline_path.is_dir():
        pipeline_path = pipeline_path / "pipeline.yaml"

    raw = load_yaml(pipeline_path)

    if env_path is not None:
        env = load_yaml(Path(env_path))
    if variables_path is not None:
        variables = load_yaml(Path(variables_path))

    resolved = resolve_variables(raw, env=env, variables=variables)
    spec = PipelineSpec.model_validate(resolved)
    if variables:
        spec = spec.model_copy(update={"variables": variables})
    check_dag(spec)

    if not skip_git:
        git_hash, has_uncommitted = _get_git_info(pipeline_path)
        spec = spec.model_copy(
            update={
                "git_hash": git_hash,
                "has_uncommitted_changes": has_uncommitted,
            }
        )

    pipeline_schema = _load_pipeline_schema(spec, base_dir=pipeline_path.parent)
    if pipeline_schema is not None:
        spec = spec.model_copy(update={"pipeline_schema": pipeline_schema})

    return spec


def resolve_pipeline_from_str(
    pipeline_yaml: str,
    *,
    env: dict[str, Any] | None = None,
    variables: dict[str, Any] | None = None,
    strict: bool = True,
    warnings: list[str] | None = None,
) -> PipelineSpec:
    """Resolve a pipeline spec from a raw YAML string.

    Identical to :func:`resolve_pipeline` but accepts YAML content directly
    instead of a file path. Git introspection is always skipped.

    Args:
        pipeline_yaml: Raw YAML content of the pipeline file.
        env: Optional pre-loaded environment dict.
        variables: Optional pre-loaded variables dict.
        strict: If ``False``, unresolvable ``${...}`` references are left as-is
                rather than raising ``KeyError``.
        warnings: Optional list to collect unresolved reference messages when
                  ``strict=False``.

    Returns:
        A validated :class:`PipelineSpec` with all ``${...}`` references resolved.
    """
    raw = yaml.safe_load(pipeline_yaml) or {}
    resolved = resolve_variables(raw, env=env, variables=variables, strict=strict, warnings=warnings)
    spec = PipelineSpec.model_validate(resolved)
    if variables:
        spec = spec.model_copy(update={"variables": variables})
    check_dag(spec)
    return spec
