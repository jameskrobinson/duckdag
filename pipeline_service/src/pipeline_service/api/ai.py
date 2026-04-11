"""AI-assisted transform generation endpoint.

POST /ai/generate-transform — takes a plain-English description of what a node
should do, the shapes of its input DataFrames, and context about the workspace's
existing transforms, then calls the Anthropic API to either:

  (a) configure an existing transform with specific params, or
  (b) generate new Python source code for a pandas_transform function.

The endpoint never writes files — file persistence is user-confirmed client-side.
"""
from __future__ import annotations

import json
import os
import re
import textwrap
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from pipeline_service.utils import resolve_transforms_root

router = APIRouter()

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class ColumnSchemaInput(BaseModel):
    name: str
    dtype: str


class GenerateTransformRequest(BaseModel):
    description: str
    input_schemas: dict[str, list[ColumnSchemaInput]]  # node_id → columns
    output_columns: list[str] = []
    pipeline_name: str = ""
    node_id: str = ""
    workspace_transforms: list[str] = []   # existing dotted transform paths
    pipeline_dir: str | None = None
    workspace: str | None = None
    previous_code: str | None = None       # shown on Regenerate so the LLM has context


class GenerateTransformResponse(BaseModel):
    kind: Literal["new", "configure"]
    # kind == "new"
    function_name: str | None = None
    code: str | None = None
    suggested_filename: str | None = None
    # kind == "configure"
    transform: str | None = None
    params: dict[str, Any] = {}
    # both
    explanation: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collect_existing_transforms(pipeline_dir: str | None, workspace: str | None) -> list[str]:
    """Scan pipeline-local and workspace transforms directories and return dotted paths."""
    found: list[str] = []
    roots: list[Path] = []
    if pipeline_dir:
        roots.append(Path(pipeline_dir) / "transforms")
    if workspace:
        roots.append(Path(workspace) / "transforms")
    for root in roots:
        if root.is_dir():
            for f in sorted(root.glob("*.py")):
                stem = f.stem
                try:
                    src = f.read_text(encoding="utf-8")
                    # Pull function names from REGISTRY or def lines
                    fns = re.findall(r"def\s+([a-zA-Z_]\w*)\s*\(", src)
                    for fn in fns:
                        if not fn.startswith("_"):
                            found.append(f"transforms.{stem}.{fn}")
                except Exception:
                    pass
    return found


def _build_prompt(req: GenerateTransformRequest, existing: list[str]) -> str:
    """Construct the LLM prompt."""
    # Input schema block
    schema_lines: list[str] = []
    for node_id, cols in req.input_schemas.items():
        col_desc = ", ".join(f"{c.name} ({c.dtype})" for c in cols)
        schema_lines.append(f"  {node_id}: {col_desc or '(no columns known yet)'}")
    schema_block = "\n".join(schema_lines) if schema_lines else "  (no upstream nodes connected)"

    # Existing transforms block
    if existing or req.workspace_transforms:
        all_transforms = sorted(set(existing + req.workspace_transforms))
        transforms_block = "\n".join(f"  - {t}" for t in all_transforms)
    else:
        transforms_block = "  (none available)"

    # Output columns hint
    output_block = ""
    if req.output_columns:
        output_block = f"\n## Expected output columns\n{', '.join(req.output_columns)}\n"

    # Previous code hint (Regenerate path)
    previous_block = ""
    if req.previous_code:
        previous_block = f"\n## Previously generated code (for reference / improvement)\n```python\n{req.previous_code}\n```\n"

    return textwrap.dedent(f"""
        You are a Python data engineering assistant. Your job is to write a pandas
        transform function that satisfies the user's requirement.

        ## Conventions
        - Functions must follow the pandas_transform signature exactly:
              def function_name(inputs: dict[str, pd.DataFrame], params: dict) -> pd.DataFrame
        - Document params using the pipeline docstring format so the GUI can render a typed form:
              Args:
                  inputs: upstream DataFrames keyed by node ID
                  params:
                      param_name (type): Description. Default: value.
        - Return a single pd.DataFrame.
        - Do not use print(), logging, or side effects.
        - Prefer vectorised pandas/numpy operations over row-by-row loops.
        - Import only: standard library, numpy, pandas, scipy, sklearn (only if genuinely needed).
        - Include a REGISTRY dict at the bottom of the file mapping the function name to itself,
          e.g.: REGISTRY = {{"function_name": function_name}}
        - The generated file is a complete Python module (include all imports at the top).

        ## Available input columns
        {schema_block}

        ## Existing transforms available for reuse
        {transforms_block}
        If one of these can fulfil the requirement with the right params, prefer configuring it
        over writing new code.
        {output_block}{previous_block}
        ## User requirement
        {req.description}

        ## Response format
        Respond with a single JSON object only — no markdown, no commentary outside the JSON:
        {{
          "kind": "new" | "configure",

          // If kind == "new":
          "function_name": "snake_case_name",
          "code": "<complete Python module source>",
          "suggested_filename": "transforms/generated_<slug>.py",

          // If kind == "configure":
          "transform": "transforms.module.function_name",
          "params": {{}},

          // Always:
          "explanation": "<one sentence describing what the transform does>"
        }}
    """).strip()


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post("/generate-transform", response_model=GenerateTransformResponse)
async def generate_transform(req: GenerateTransformRequest) -> GenerateTransformResponse:
    """Generate or configure a pandas_transform using the Anthropic API."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail=(
                "ANTHROPIC_API_KEY environment variable is not set. "
                "Set it to your Anthropic API key to enable AI transform generation."
            ),
        )

    try:
        import anthropic  # type: ignore[import-untyped]
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail=(
                "The 'anthropic' package is not installed in the service environment. "
                "Install it with: pip install anthropic"
            ),
        )

    existing = _collect_existing_transforms(req.pipeline_dir, req.workspace)
    prompt = _build_prompt(req, existing)

    client = anthropic.Anthropic(api_key=api_key)
    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
    except anthropic.APIError as exc:
        raise HTTPException(status_code=502, detail=f"Anthropic API error: {exc}")

    raw = message.content[0].text.strip()

    # Strip markdown code fences if the model wrapped the JSON
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw).strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Model returned non-JSON response: {exc}\n\nRaw output:\n{raw[:500]}",
        )

    kind = data.get("kind")
    if kind not in ("new", "configure"):
        raise HTTPException(
            status_code=502,
            detail=f"Model returned unexpected 'kind' value: {kind!r}",
        )

    if kind == "new":
        # Derive a sensible suggested filename if the model didn't provide one
        fn = data.get("function_name", "generated_transform")
        suggested = data.get("suggested_filename") or f"transforms/generated_{fn}.py"
        return GenerateTransformResponse(
            kind="new",
            function_name=fn,
            code=data.get("code", ""),
            suggested_filename=suggested,
            explanation=data.get("explanation", ""),
        )
    else:
        return GenerateTransformResponse(
            kind="configure",
            transform=data.get("transform", ""),
            params=data.get("params", {}),
            explanation=data.get("explanation", ""),
        )
