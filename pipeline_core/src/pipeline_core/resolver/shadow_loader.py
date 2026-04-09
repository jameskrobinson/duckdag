"""Shadow spec loader — reads pipeline.shadow.yaml alongside a pipeline file.

Usage::

    from pipeline_core.resolver.shadow_loader import load_shadow_spec

    shadow_specs = load_shadow_spec("/path/to/pipeline/dir")
    # Returns dict[str, ShadowNodeSpec] keyed by primary node_id, or {} if no file.
"""
from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from pipeline_core.resolver.models import ShadowNodeSpec

SHADOW_FILENAME = "pipeline.shadow.yaml"


class ShadowConfigError(Exception):
    """Raised when pipeline.shadow.yaml exists but cannot be parsed."""


def load_shadow_spec(pipeline_dir: str | Path) -> dict[str, ShadowNodeSpec]:
    """Load and validate ``pipeline.shadow.yaml`` from *pipeline_dir*.

    Returns:
        A dict mapping primary ``node_id`` → :class:`ShadowNodeSpec`.
        Returns an empty dict when the file does not exist.

    Raises:
        :class:`ShadowConfigError`: When the file exists but is invalid YAML
            or fails Pydantic validation.
    """
    path = Path(pipeline_dir) / SHADOW_FILENAME
    if not path.exists():
        return {}

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ShadowConfigError(
            f"{SHADOW_FILENAME} is not valid YAML: {exc}"
        ) from exc

    if not raw:
        return {}

    if not isinstance(raw, dict):
        raise ShadowConfigError(
            f"{SHADOW_FILENAME} must be a YAML mapping of node_id → shadow spec, "
            f"got {type(raw).__name__}"
        )

    result: dict[str, ShadowNodeSpec] = {}
    errors: list[str] = []

    for node_id, entry in raw.items():
        if not isinstance(entry, dict):
            errors.append(
                f"  node '{node_id}': expected a mapping, got {type(entry).__name__}"
            )
            continue
        # Inject id from the YAML key if not explicitly set
        if "id" not in entry:
            entry = {**entry, "id": node_id}
        try:
            result[str(node_id)] = ShadowNodeSpec.model_validate(entry)
        except ValidationError as exc:
            errors.append(f"  node '{node_id}':\n" + "\n".join(
                f"    {e['loc']}: {e['msg']}" for e in exc.errors()
            ))

    if errors:
        raise ShadowConfigError(
            f"{SHADOW_FILENAME} has validation errors:\n" + "\n".join(errors)
        )

    return result


def write_shadow_spec(pipeline_dir: str | Path, specs: dict[str, ShadowNodeSpec]) -> None:
    """Serialise *specs* back to ``pipeline.shadow.yaml`` in *pipeline_dir*.

    Primarily used by the service layer to persist UI-authored changes.
    """
    path = Path(pipeline_dir) / SHADOW_FILENAME
    raw: dict = {}
    for node_id, spec in specs.items():
        d = spec.model_dump(exclude_none=True, exclude_defaults=False)
        # Remove the id key — it's redundant (it's the YAML map key)
        d.pop("id", None)
        # Remove empty collections for cleaner YAML
        for k in list(d.keys()):
            if d[k] in ([], {}, None):
                del d[k]
        raw[node_id] = d
    path.write_text(yaml.dump(raw, default_flow_style=False, sort_keys=False), encoding="utf-8")
