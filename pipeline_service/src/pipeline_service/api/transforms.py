from __future__ import annotations

import importlib
import inspect
import re
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from pipeline_core.transforms import TRANSFORM_MODULES

from pipeline_service.node_types import NODE_TYPE_SCHEMAS, NodeTypeSchema, ParamSchema

router = APIRouter()


# ---------------------------------------------------------------------------
# GET /node-types
# ---------------------------------------------------------------------------

@router.get("", response_model=list[NodeTypeSchema])
def list_node_types() -> list[NodeTypeSchema]:
    """Return schema for every supported node type.

    Used by the builder to populate the transform palette and render the
    correct config form for each node type.
    """
    return NODE_TYPE_SCHEMAS


# ---------------------------------------------------------------------------
# POST /node-types/inspect  — parse pandas_transform docstring
# ---------------------------------------------------------------------------

class InspectRequest(BaseModel):
    transform_path: str
    """Fully-qualified dotted path to the transform function or its REGISTRY module,
    e.g. 'transforms.basic' (loads REGISTRY) or 'transforms.basic.select_rename'."""


class InspectResponse(BaseModel):
    name: str
    summary: str
    params: list[ParamSchema]
    phrases: list[str] = []
    tags: list[str] = []


@router.post("/inspect", response_model=list[InspectResponse])
def inspect_transforms(body: InspectRequest) -> list[InspectResponse]:
    """Import a transform module or function and return its parameter schema.

    If the path resolves to a module with a ``REGISTRY`` dict, all registered
    transforms (excluding ``__lineage`` helpers) are returned. If it resolves
    to a specific callable, only that transform is returned.
    """
    try:
        obj = _import_dotted(body.transform_path)
    except (ImportError, AttributeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # Module with REGISTRY → return all registered transforms
    if inspect.ismodule(obj):
        registry: dict[str, Any] = getattr(obj, "REGISTRY", None)
        if registry is None:
            raise HTTPException(
                status_code=422,
                detail=f"Module '{body.transform_path}' has no REGISTRY dict.",
            )
        results = []
        for key, fn in registry.items():
            if key.endswith("__lineage"):
                continue
            if callable(fn):
                results.append(_parse_transform(key, fn))
        return results

    # Single callable
    if callable(obj):
        fn_name = body.transform_path.rpartition(".")[-1]
        return [_parse_transform(fn_name, obj)]

    raise HTTPException(
        status_code=422,
        detail=f"'{body.transform_path}' is neither a module with REGISTRY nor a callable.",
    )


# ---------------------------------------------------------------------------
# Docstring parser
# ---------------------------------------------------------------------------

# Section header pattern: "Summary:", "Params:", "Parameters:", etc.
_SECTION_RE = re.compile(
    r"^(Summary|Details|Params|Parameters|Input requirements|Output|Phrases|Tags)\s*:",
    re.IGNORECASE,
)

# Param line: "- name (type_info): description"
_PARAM_RE = re.compile(
    r"^\s*-\s+(\w+)\s+\(([^)]+)\)\s*:\s*(.+)$"
)

# default=X extractor (handles bare values, quoted strings, and dicts/lists)
_DEFAULT_RE = re.compile(r"default\s*=\s*(.+?)(?:,\s*allowed|$)", re.IGNORECASE)
_ALLOWED_RE = re.compile(r"allowed\s*=\s*(\{[^}]+\})", re.IGNORECASE)

_TYPE_MAP = {
    "str": "string",
    "bool": "boolean",
    "int": "integer",
    "float": "number",
    "list": "list",
    "dict": "dict",
    "any": "any",
    "pd.dataframe": "dataframe",
    "optional": "any",
}


def _canonical_type(raw_type: str) -> str:
    """Map a raw type token to a canonical type string."""
    t = raw_type.lower().strip()
    # list[...] → list, dict[...] → dict
    base = t.split("[")[0].strip()
    return _TYPE_MAP.get(base, t)


def _parse_param_line(line: str) -> ParamSchema | None:
    m = _PARAM_RE.match(line)
    if not m:
        return None
    name, type_info, description = m.group(1), m.group(2), m.group(3).strip()

    tokens = [t.strip() for t in type_info.split(",")]
    raw_type = tokens[0] if tokens else "any"
    type_str = _canonical_type(raw_type)

    # required / optional / default
    type_lower = type_info.lower()
    required = "required" in type_lower

    default: Any = None
    dm = _DEFAULT_RE.search(type_info)
    if dm:
        raw_default = dm.group(1).strip().strip('"').strip("'")
        # Attempt bool / int / float coercion; leave as string otherwise
        if raw_default.lower() == "true":
            default = True
        elif raw_default.lower() == "false":
            default = False
        elif raw_default.lower() in ("null", "none"):
            default = None
        else:
            try:
                default = int(raw_default)
            except ValueError:
                try:
                    default = float(raw_default)
                except ValueError:
                    default = raw_default
        required = False  # having a default implies not required

    return ParamSchema(
        name=name,
        type=type_str,
        required=required,
        description=description,
        default=default,
    )


def _parse_transform(name: str, fn: Any) -> InspectResponse:
    """Parse a transform function's docstring into an InspectResponse."""
    doc = inspect.getdoc(fn) or ""
    sections: dict[str, list[str]] = {}
    current: str | None = None

    for raw_line in doc.splitlines():
        m = _SECTION_RE.match(raw_line.strip())
        if m:
            current = m.group(1).lower()
            # Normalise "params" → "parameters"
            if current == "params":
                current = "parameters"
            sections[current] = []
        elif current is not None:
            sections[current].append(raw_line)

    summary = " ".join(l.strip() for l in sections.get("summary", []) if l.strip())

    params: list[ParamSchema] = []
    for line in sections.get("parameters", []):
        p = _parse_param_line(line)
        if p and p.name != "no_intermediate_materialization":
            params.append(p)

    phrases = [l.strip().lstrip("- ") for l in sections.get("phrases", []) if l.strip()]
    tags_raw = sections.get("tags", [])
    tags = [
        t.strip()
        for line in tags_raw
        for t in line.lstrip("- ").split(",")
        if t.strip()
    ]

    return InspectResponse(name=name, summary=summary, params=params, phrases=phrases, tags=tags)


# ---------------------------------------------------------------------------
# GET /node-types/pandas-transforms — hierarchical transform tree for palette
# ---------------------------------------------------------------------------

class PandasTransformEntry(InspectResponse):
    full_path: str
    """Fully-qualified dotted path to pass as the 'transform' param."""


class PandasTransformCategory(BaseModel):
    category: str
    module_path: str
    transforms: list[PandasTransformEntry]
    scope: str = "builtin"
    """Origin of the transforms: 'builtin' | 'workspace' | 'pipeline'."""


@router.get("/pandas-transforms", response_model=list[PandasTransformCategory])
def list_pandas_transforms(
    workspace: str | None = None,
) -> list[PandasTransformCategory]:
    """Return all registered pandas transform modules and their transforms.

    Iterates ``pipeline_core.transforms.TRANSFORM_MODULES`` and inspects each
    module's REGISTRY. When ``workspace`` is provided, also scans the workspace
    for Python files with a ``REGISTRY`` dict and appends them as additional
    categories. Used by the builder palette to populate the expandable
    pandas_transform sub-tree.
    """
    result: list[PandasTransformCategory] = []

    # Built-in transforms from pipeline_core
    for category, module_path in TRANSFORM_MODULES.items():
        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Could not import transform module '{module_path}': {exc}",
            )
        registry = getattr(module, "REGISTRY", None)
        if registry is None:
            continue
        transforms: list[PandasTransformEntry] = []
        for key, fn in registry.items():
            if key.endswith("__lineage") or not callable(fn):
                continue
            base = _parse_transform(key, fn)
            transforms.append(
                PandasTransformEntry(
                    **base.model_dump(),
                    full_path=f"{module_path}.{key}",
                )
            )
        result.append(
            PandasTransformCategory(
                category=category,
                module_path=module_path,
                transforms=transforms,
                scope="builtin",
            )
        )

    # Workspace transforms — scan for .py files containing a REGISTRY dict
    if workspace:
        result.extend(_load_workspace_transforms(workspace))

    return result


def _load_workspace_transforms(workspace: str) -> list[PandasTransformCategory]:
    """Scan a workspace directory for Python transform modules.

    A file is treated as a transform module if it contains a top-level
    ``REGISTRY`` dict. The workspace is added to ``sys.path`` so that imports
    work correctly.

    Supports two layouts:

    **New layout** (preferred):
      - ``{workspace}/transforms/*.py``             → labelled "workspace / {stem}"
      - ``{workspace}/pipelines/{name}/transforms/`` → labelled "pipeline:{name} / {stem}"

    **Legacy layout** (backward compatible):
      Any ``.py`` file with a ``REGISTRY`` dict found recursively in the
      workspace is included, labelled as "workspace / {stem}".

    Excludes ``runs/``, ``.git/``, ``__pycache__``, ``.venv``, ``node_modules``.
    """
    import sys
    from pathlib import Path

    _SKIP_DIRS = {"__pycache__", ".git", ".venv", "venv", "runs", "node_modules"}
    root = Path(workspace)
    if not root.exists() or not root.is_dir():
        return []

    # Add workspace to sys.path so relative imports work
    ws_str = str(root)
    path_added = ws_str not in sys.path
    if path_added:
        sys.path.insert(0, ws_str)

    def _classify(py_file: Path) -> tuple[str, str]:
        """Return (category_label, scope) based on file location."""
        rel_parts = py_file.relative_to(root).parts
        # New layout: workspace/transforms/{stem}.py
        if len(rel_parts) == 2 and rel_parts[0] == "transforms":
            return f"workspace / {py_file.stem}", "workspace"
        # New layout: workspace/pipelines/{name}/transforms/{stem}.py
        if len(rel_parts) >= 4 and rel_parts[0] == "pipelines" and rel_parts[2] == "transforms":
            return f"{rel_parts[1]} / {py_file.stem}", "pipeline"
        # Legacy: any other location
        return f"workspace / {py_file.stem}", "workspace"

    categories: list[PandasTransformCategory] = []
    try:
        for py_file in sorted(root.rglob("*.py")):
            # Skip excluded directories
            if any(part in _SKIP_DIRS for part in py_file.relative_to(root).parts):
                continue
            # Derive dotted module path relative to workspace root
            rel = py_file.relative_to(root)
            module_path = ".".join(rel.with_suffix("").parts)
            try:
                module = importlib.import_module(module_path)
            except Exception:
                continue
            registry = getattr(module, "REGISTRY", None)
            if not isinstance(registry, dict) or not registry:
                continue
            transforms: list[PandasTransformEntry] = []
            for key, fn in registry.items():
                if key.endswith("__lineage") or not callable(fn):
                    continue
                base = _parse_transform(key, fn)
                transforms.append(
                    PandasTransformEntry(
                        **base.model_dump(),
                        full_path=f"{module_path}.{key}",
                    )
                )
            if transforms:
                label, scope = _classify(py_file)
                categories.append(
                    PandasTransformCategory(
                        category=label,
                        module_path=module_path,
                        transforms=transforms,
                        scope=scope,
                    )
                )
    finally:
        if path_added:
            try:
                sys.path.remove(ws_str)
            except ValueError:
                pass

    return categories


def _import_dotted(path: str) -> Any:
    """Import a dotted path, returning either the module or the attribute on it."""
    # Try importing as a module first
    try:
        return importlib.import_module(path)
    except ImportError:
        pass
    # Try splitting into module + attribute
    module_path, _, attr = path.rpartition(".")
    if not module_path:
        raise ImportError(f"Cannot import '{path}'")
    module = importlib.import_module(module_path)
    if not hasattr(module, attr):
        raise AttributeError(f"Module '{module_path}' has no attribute '{attr}'")
    return getattr(module, attr)
