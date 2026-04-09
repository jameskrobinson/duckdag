from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

# Matches ${some.dotted.path}
_INTERP_RE = re.compile(r"\$\{([^}]+)\}")


def _get_nested(obj: Any, path: str) -> Any:
    """Resolve a dotted path in a nested structure.

    Raises:
        KeyError: if any segment of the path is missing or the traversal hits
                  a non-dict where a dict is expected.
    """
    parts = path.split(".")
    current = obj
    for part in parts:
        if not isinstance(current, dict):
            raise KeyError(
                f"Cannot resolve '$\u007b{path}\u007d': expected a dict at '{part}', "
                f"got {type(current).__name__}"
            )
        if part not in current:
            raise KeyError(
                f"Cannot resolve '$\u007b{path}\u007d': key '{part}' not found"
            )
        current = current[part]
    return current


def _resolve_value(
    value: Any,
    context: dict[str, Any],
    strict: bool = True,
    warnings: list[str] | None = None,
) -> Any:
    """Recursively resolve ${...} references in a value.

    Two substitution modes:
    - Whole-value reference: ``"${parameters.rc_list}"`` (nothing else in the string)
      → returns the typed value as-is (list, dict, int, …).
    - Embedded reference: ``"${env.paths.data_dir}/foo.duckdb"``
      → string-interpolates each reference and returns a string.

    Non-string scalars, dicts, and lists are traversed recursively;
    non-strings are returned unchanged.

    Args:
        strict: If ``False``, unresolvable references are left as-is (the
                original ``${path}`` placeholder) and a warning is appended to
                *warnings* rather than raising a ``KeyError``.
        warnings: Optional list to collect unresolved reference messages when
                  ``strict=False``.
    """
    if isinstance(value, str):
        matches = _INTERP_RE.findall(value)
        if not matches:
            return value
        # Whole-value substitution — preserve the original type.
        if len(matches) == 1 and value.strip() == f"${{{matches[0]}}}":
            if strict:
                return _get_nested(context, matches[0])
            try:
                return _get_nested(context, matches[0])
            except KeyError as exc:
                if warnings is not None:
                    warnings.append(str(exc))
                return value  # return placeholder unchanged
        # Partial string interpolation — all referenced values become strings.
        def _replace(m: re.Match) -> str:  # noqa: ANN202
            if strict:
                return str(_get_nested(context, m.group(1)))
            try:
                return str(_get_nested(context, m.group(1)))
            except KeyError as exc:
                if warnings is not None:
                    warnings.append(str(exc))
                return m.group(0)  # leave ${path} unchanged

        return _INTERP_RE.sub(_replace, value)
    elif isinstance(value, dict):
        return {k: _resolve_value(v, context, strict=strict, warnings=warnings) for k, v in value.items()}
    elif isinstance(value, list):
        return [_resolve_value(item, context, strict=strict, warnings=warnings) for item in value]
    return value


def load_yaml(path: Path | str) -> dict[str, Any]:
    """Load a YAML file and return its contents as a dict."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def resolve_variables(
    raw: dict[str, Any],
    env: dict[str, Any] | None = None,
    variables: dict[str, Any] | None = None,
    strict: bool = True,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    """Substitute ${...} references throughout a raw pipeline dict.

    Resolution context:
        ``${parameters.*}``  →  the ``parameters`` section of the pipeline YAML.
        ``${variables.*}``   →  the variables YAML (committed defaults, runtime-overridable).
        ``${env.*}``         →  the environment YAML (machine-local, never committed).

    Precedence (highest to lowest): env > variables > pipeline parameters.

    Args:
        raw: The raw pipeline dict as loaded from YAML (pre-validation).
        env: Optional pre-loaded environment dict.
        variables: Optional pre-loaded variables dict.
        strict: If ``False``, unresolvable references are left as-is rather
                than raising ``KeyError``.  Any warnings are appended to *warnings*.
        warnings: Optional list to collect unresolved reference messages when
                  ``strict=False``.

    Returns:
        A new dict with all ${...} references replaced.

    Raises:
        KeyError: If any reference cannot be resolved (only when ``strict=True``).
    """
    # Seed defaults from variable_declarations so pipelines resolve even when
    # no variables.yaml is provided. Explicitly supplied variables take priority.
    decl_defaults: dict[str, Any] = {
        d["name"]: d["default"]
        for d in raw.get("variable_declarations", [])
        if isinstance(d, dict) and "name" in d and d.get("default") is not None
    }
    merged_variables = {**decl_defaults, **(variables or {})}
    context: dict[str, Any] = {
        "parameters": raw.get("parameters", {}),
        "variables": merged_variables,
        "env": env or {},
    }
    return _resolve_value(raw, context, strict=strict, warnings=warnings)
