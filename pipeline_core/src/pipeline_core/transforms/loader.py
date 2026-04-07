"""Transform module loader for workspace and pipeline-local transforms.

Built-in transforms (``pipeline_core.transforms.*``) are always importable via
the normal Python import machinery.  Workspace and pipeline-local transforms
live under ``{root}/transforms/*.py`` and require ``root`` to be on sys.path.

The single entry point is :func:`load_transform`, used by the executor.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any, Callable

import pandas as pd


def load_transform(
    transform_path: str,
    transforms_root: str | None = None,
) -> Callable[[dict[str, pd.DataFrame], dict[str, Any]], pd.DataFrame]:
    """Resolve and return a transform callable.

    Resolution order:
    1. Direct import — works for built-ins (``pipeline_core.transforms.*``) and
       anything already on ``sys.path``.
    2. If *transforms_root* is set and the direct import failed, temporarily
       prepend *transforms_root* to ``sys.path`` and retry.  This covers
       workspace-level (``{workspace}/transforms/``) and pipeline-local
       (``{pipeline_dir}/transforms/``) modules.

    Args:
        transform_path: Fully-qualified dotted path to a callable, e.g.
            ``"transforms.basic.my_fn"`` or
            ``"pipeline_core.transforms.basic.select_rename"``.
        transforms_root: Absolute path to the directory that *contains* the
            ``transforms/`` package (i.e. the workspace or pipeline root), so
            that ``transforms.basic`` resolves to
            ``{transforms_root}/transforms/basic.py``.

    Returns:
        The transform callable.

    Raises:
        ValueError: If *transform_path* is not a dotted ``module.fn`` path.
        ImportError: If the module cannot be found on sys.path or via
            *transforms_root*.
        AttributeError: If the module exists but lacks the named function.
    """
    module_path, _, fn_name = transform_path.rpartition(".")
    if not module_path:
        raise ValueError(
            f"'transform' must be a fully-qualified dotted path, "
            f"e.g. 'mypackage.module.fn_name', got '{transform_path}'"
        )

    # 1. Try direct import (covers built-ins and anything already on sys.path)
    try:
        module = importlib.import_module(module_path)
        return getattr(module, fn_name)
    except ImportError:
        if transforms_root is None:
            raise
    except AttributeError:
        raise

    # 2. Add transforms_root to sys.path and retry
    root_str = str(Path(transforms_root))
    added = root_str not in sys.path
    if added:
        sys.path.insert(0, root_str)
    try:
        # Force a fresh import in case a stale cached module is shadowing
        if module_path in sys.modules:
            del sys.modules[module_path]
        module = importlib.import_module(module_path)
        return getattr(module, fn_name)
    finally:
        if added:
            try:
                sys.path.remove(root_str)
            except ValueError:
                pass
