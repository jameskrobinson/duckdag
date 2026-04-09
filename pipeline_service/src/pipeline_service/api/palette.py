"""Unified palette endpoint — sources / transforms / sinks.

Returns a single structured response that replaces the three separate
node-types / pandas-transforms / templates endpoints for UI palette purposes.

Response shape
--------------
- **sources** (list[PaletteFunction])  — load_* node types; two-level: connector → config
- **transforms** (list[PaletteGroup])  — grouped three-level: group → function → config
  - Python groups from workspace/pipeline transform modules
  - Synthetic "SQL" group for sql_transform
- **sinks** (list[PaletteFunction])    — push_*/export_*/sql_exec; two-level: connector → config
"""
from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter
from pydantic import BaseModel

from pipeline_service.api.templates import (
    _COMMON,
    _local_from_sql_files,
    _local_from_yaml_files,
    _pipeline_configs,
    NodeTemplate,
)
from pipeline_service.api.transforms import list_pandas_transforms, PandasTransformCategory
from pipeline_service.node_types import NODE_TYPE_SCHEMAS, NodeTypeSchema, ParamSchema

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class PaletteConfig(BaseModel):
    """A pre-filled config preset — drag to canvas to create a node with params pre-populated."""

    id: str
    label: str
    description: str
    origin: Literal["builtin", "workspace", "pipeline"]
    """Where the preset came from: builtin (shipped), workspace (user-shared), pipeline (solidified)."""
    params: dict[str, Any] = {}
    template_file: str | None = None
    template_path: str | None = None
    sql_preview: str | None = None
    tags: list[str] = []


class PaletteFunction(BaseModel):
    """A draggable function entry — drag to canvas to create a blank node of this type."""

    kind: Literal["source", "transform", "sink"]
    node_type: str
    """Underlying pipeline node type (e.g. 'load_csv', 'pandas_transform', 'sql_transform')."""
    label: str
    description: str
    tags: list[str] = []
    origin: Literal["builtin", "workspace", "pipeline"] = "builtin"
    fixed_params: list[ParamSchema] = []
    needs_template: bool = False
    accepts_template_params: bool = False
    full_path: str | None = None
    """Fully-qualified dotted path — set only for pandas transform functions."""
    configs: list[PaletteConfig] = []


class PaletteGroup(BaseModel):
    """A category grouping transform functions (Python library module, SQL group, etc.)."""

    name: str
    label: str
    origin: Literal["builtin", "workspace", "pipeline"] = "builtin"
    functions: list[PaletteFunction] = []


class PaletteResponse(BaseModel):
    sources: list[PaletteFunction]
    transforms: list[PaletteGroup]
    sinks: list[PaletteFunction]


class PaletteTagEntry(BaseModel):
    tag: str
    count: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _kind_for(nt: NodeTypeSchema) -> Literal["source", "transform", "sink"]:
    """Map a node type to its palette bucket based on category."""
    if nt.category == "load":
        return "source"
    if nt.category == "export":
        return "sink"
    # sql_exec produces no output — treat as sink (most common use: COPY TO, INSERT INTO)
    if nt.category == "sql" and not nt.produces_output:
        return "sink"
    return "transform"


def _origin_for(tmpl: NodeTemplate) -> Literal["builtin", "workspace", "pipeline"]:
    if tmpl.scope == "common":
        return "builtin"
    if tmpl.scope in ("pipeline", "config"):
        return "pipeline"
    return "workspace"


def _tmpl_to_config(tmpl: NodeTemplate) -> PaletteConfig:
    return PaletteConfig(
        id=tmpl.id,
        label=tmpl.label,
        description=tmpl.description,
        origin=_origin_for(tmpl),
        params=tmpl.params,
        template_file=tmpl.template_file,
        template_path=tmpl.template_path,
        sql_preview=tmpl.sql_preview,
        tags=tmpl.tags,
    )


def _gather_templates(workspace: str | None) -> list[NodeTemplate]:
    """Collect all templates: common + workspace-local + pipeline configs."""
    templates: list[NodeTemplate] = list(_COMMON)
    if workspace:
        from pathlib import Path
        root = Path(workspace)
        templates.extend(_local_from_sql_files(root / "templates"))
        templates.extend(_local_from_yaml_files(root / "node_templates"))
        templates.extend(_local_from_yaml_files(root / "templates" / "pandas", id_prefix="local/pandas"))
        templates.extend(_local_from_yaml_files(root / "templates" / "api", id_prefix="local/api"))
        templates.extend(_pipeline_configs(root))
    return templates


# ---------------------------------------------------------------------------
# GET /palette
# ---------------------------------------------------------------------------


@router.get("", response_model=PaletteResponse)
def get_palette(workspace: str | None = None) -> PaletteResponse:
    """Return the unified palette: sources, transforms, sinks.

    **Sources** — load_* node types, each with attached config presets.
    **Transforms** — grouped by library/category:
      - Python groups (builtin / workspace / pipeline modules)
      - Synthetic "SQL" group containing ``sql_transform``
    **Sinks** — push_* / export_* / sql_exec node types with config presets.
    """
    all_templates = _gather_templates(workspace)
    templates_by_type: dict[str, list[NodeTemplate]] = {}
    for t in all_templates:
        templates_by_type.setdefault(t.node_type, []).append(t)

    sources: list[PaletteFunction] = []
    sinks: list[PaletteFunction] = []
    sql_transform_types: list[NodeTypeSchema] = []

    for nt in NODE_TYPE_SCHEMAS:
        # pandas_transform is handled via the module/function tree below
        if nt.type == "pandas_transform":
            continue
        kind = _kind_for(nt)
        configs = [_tmpl_to_config(t) for t in templates_by_type.get(nt.type, [])]
        fn = PaletteFunction(
            kind=kind,
            node_type=nt.type,
            label=nt.label,
            description=nt.description,
            tags=nt.tags,
            fixed_params=nt.fixed_params,
            needs_template=nt.needs_template,
            accepts_template_params=nt.accepts_template_params,
            configs=configs,
        )
        if kind == "source":
            sources.append(fn)
        elif kind == "sink":
            sinks.append(fn)
        else:
            # kind == "transform" and category == "sql"
            sql_transform_types.append(nt)

    # Build transform groups
    transform_groups: list[PaletteGroup] = []

    # Python groups — pandas transforms (builtin + workspace + pipeline)
    pandas_cats: list[PandasTransformCategory] = list_pandas_transforms(workspace)
    for cat in pandas_cats:
        pandas_tmpls = templates_by_type.get("pandas_transform", [])
        fns: list[PaletteFunction] = []
        for entry in cat.transforms:
            # Attach only presets that reference exactly this function via the 'transform' param
            fn_configs = [
                _tmpl_to_config(t) for t in pandas_tmpls
                if t.params.get("transform") == entry.full_path
            ]
            origin: Literal["builtin", "workspace", "pipeline"] = (
                "pipeline" if cat.scope == "pipeline"
                else "workspace" if cat.scope == "workspace"
                else "builtin"
            )
            fns.append(PaletteFunction(
                kind="transform",
                node_type="pandas_transform",
                label=entry.name,
                description=entry.summary,
                tags=entry.tags,
                origin=origin,
                full_path=entry.full_path,
                configs=fn_configs,
            ))
        if fns:
            cat_origin: Literal["builtin", "workspace", "pipeline"] = (
                "pipeline" if cat.scope == "pipeline"
                else "workspace" if cat.scope == "workspace"
                else "builtin"
            )
            transform_groups.append(PaletteGroup(
                name=cat.category,
                label=cat.category,
                origin=cat_origin,
                functions=fns,
            ))

    # SQL group — sql_transform (and any future sql-category produces_output types)
    if sql_transform_types:
        # Split templates by category: those with a declared category go into named groups;
        # the rest stay in the flat "SQL" group alongside the sql_transform node type itself.
        sql_all_tmpls = templates_by_type.get("sql_transform", [])
        categorised: dict[str, list[NodeTemplate]] = {}
        uncategorised: list[NodeTemplate] = []
        for t in sql_all_tmpls:
            if t.category:
                categorised.setdefault(t.category, []).append(t)
            else:
                uncategorised.append(t)

        # Named SQL category groups (workspace/pipeline templates only)
        for cat_name, cat_tmpls in sorted(categorised.items()):
            cat_fns: list[PaletteFunction] = []
            for tmpl in cat_tmpls:
                cat_fns.append(PaletteFunction(
                    kind="transform",
                    node_type=tmpl.node_type,
                    label=tmpl.label,
                    description=tmpl.description,
                    tags=tmpl.tags,
                    origin=_origin_for(tmpl),
                    needs_template=True,
                    accepts_template_params=True,
                    configs=[_tmpl_to_config(tmpl)],
                ))
            if cat_fns:
                transform_groups.append(PaletteGroup(
                    name=f"SQL/{cat_name}",
                    label=cat_name,
                    origin="workspace",
                    functions=cat_fns,
                ))

        # Flat "SQL" group — the sql_transform node type + uncategorised templates
        sql_fns: list[PaletteFunction] = []
        for nt in sql_transform_types:
            configs = [_tmpl_to_config(t) for t in uncategorised if t.node_type == nt.type]
            sql_fns.append(PaletteFunction(
                kind="transform",
                node_type=nt.type,
                label=nt.label,
                description=nt.description,
                tags=nt.tags,
                fixed_params=nt.fixed_params,
                needs_template=nt.needs_template,
                accepts_template_params=nt.accepts_template_params,
                configs=configs,
            ))
        transform_groups.append(PaletteGroup(name="SQL", label="SQL", functions=sql_fns))

    return PaletteResponse(sources=sources, transforms=transform_groups, sinks=sinks)


# ---------------------------------------------------------------------------
# GET /palette/tags
# ---------------------------------------------------------------------------


@router.get("/tags", response_model=list[PaletteTagEntry])
def get_palette_tags(workspace: str | None = None) -> list[PaletteTagEntry]:
    """Return all unique tags across templates and pandas transforms with occurrence counts."""
    all_templates = _gather_templates(workspace)
    pandas_cats = list_pandas_transforms(workspace)

    tag_counts: dict[str, int] = {}
    # Tags from node type registrations
    for nt in NODE_TYPE_SCHEMAS:
        for tag in nt.tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
    # Tags from workspace/common templates (includes SQL front-matter tags)
    for t in all_templates:
        for tag in t.tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
    # Tags from pandas transforms (parsed from docstrings)
    for cat in pandas_cats:
        for entry in cat.transforms:
            for tag in entry.tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1

    return [PaletteTagEntry(tag=tag, count=count) for tag, count in sorted(tag_counts.items())]
