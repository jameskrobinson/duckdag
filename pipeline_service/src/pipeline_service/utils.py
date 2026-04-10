from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from pipeline_core.resolver.models import PipelineSpec


def coerce_value(v: Any) -> Any:
    """Convert numpy/pandas scalar types to JSON-serialisable Python primitives.

    Pandas DataFrames produced by transform nodes often contain numpy dtypes
    (``numpy.int64``, ``numpy.float64``, ``numpy.bool_``, etc.).  These are not
    recognised by Pydantic's JSON serialiser, causing ``PydanticSerializationError``.
    This function normalises every value to a plain Python type so that the row
    lists passed to Pydantic response models are always serialisable.

    Mapping:
    * NaN / NaT / pd.NA  → ``None``
    * numpy integer       → ``int``
    * numpy floating      → ``float`` (NaN already handled above)
    * numpy bool\_         → ``bool``
    * numpy ndarray       → ``list`` (via ``.tolist()``)
    * any other numpy scalar with ``.item()`` → native Python scalar
    * everything else     → unchanged
    """
    if v is None:
        return None
    # Catch pandas NA / NaT which compare equal to nothing
    try:
        import pandas as pd
        if v is pd.NA or v is pd.NaT:
            return None
    except Exception:
        pass
    if isinstance(v, float) and v != v:  # NaN
        return None
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return None if np.isnan(v) else float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, np.ndarray):
        return v.tolist()
    # Generic fallback for any remaining numpy scalar
    if isinstance(v, np.generic):
        return v.item()
    return v


def coerce_row(row: tuple | list) -> list[Any]:
    """Apply ``coerce_value`` to every element of a result row."""
    return [coerce_value(v) for v in row]


def resolve_transforms_root(pipeline_dir: str, workspace: str | None = None) -> str | None:
    """Return the directory to add to sys.path for workspace/pipeline-local transforms.

    Checks for a ``transforms/`` subdirectory in the following order:
    1. ``{pipeline_dir}/transforms/`` — pipeline-local transforms (highest priority)
    2. ``{workspace}/transforms/`` — workspace-wide shared transforms
    3. Returns ``None`` if neither location has a ``transforms/`` directory,
       meaning only built-in transforms are available.

    The returned path is the *parent* of the ``transforms/`` package, i.e. the
    directory that should be prepended to ``sys.path``.
    """
    for candidate_root in filter(None, [pipeline_dir, workspace]):
        if (Path(candidate_root) / "transforms").is_dir():
            return candidate_root
    return None


def resolve_templates_dir(
    pipeline_dir: str,
    spec: PipelineSpec,
    workspace: str | None = None,
) -> str:
    """Resolve the absolute path to the templates directory for a pipeline.

    Resolution order (first that exists wins):
    1. If ``spec.templates.dir`` is already absolute — use it as-is.
    2. ``{pipeline_dir}/{spec.templates.dir}`` resolved — explicit relative path from spec.
    3. ``{pipeline_dir}/config/`` — new-layout canonical location.
    4. ``{pipeline_dir}/templates/`` — legacy/flat-layout location.
    5. ``{workspace}/templates/`` — workspace-level shared templates.
    6. ``{workspace}/config/`` — workspace-level config directory.
    7. Fall back to ``{pipeline_dir}/templates`` even if it doesn't exist yet.
    """
    root = Path(pipeline_dir)
    if spec.templates and Path(spec.templates.dir).is_absolute():
        return spec.templates.dir
    if spec.templates:
        explicit = (root / spec.templates.dir).resolve()
        if explicit.exists():
            return str(explicit)
    for candidate in ("config", "templates"):
        candidate_path = root / candidate
        if candidate_path.exists():
            return str(candidate_path)
    if workspace:
        for candidate in ("templates", "config"):
            candidate_path = Path(workspace) / candidate
            if candidate_path.exists():
                return str(candidate_path)
    return str(root / "templates")
