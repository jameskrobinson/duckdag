"""Built-in transform functions for use with pandas_transform nodes.

Import paths for use in pipeline YAML:
    pipeline_core.transforms.basic.select_rename
    pipeline_core.transforms.basic.map_values
    pipeline_core.transforms.basic.map_multi_key
    pipeline_core.transforms.basic.derive_columns
    pipeline_core.transforms.basic.cast_columns
    pipeline_core.transforms.basic.reorder_columns

TRANSFORM_MODULES maps a display category name to the fully-qualified module path.
The pipeline_service inspect endpoint iterates this dict to populate the palette.
Add new transform modules here to make them discoverable.
"""

TRANSFORM_MODULES: dict[str, str] = {
    "basic": "pipeline_core.transforms.basic",
}
