"""Allow `python -m pipeline_cli` to invoke the CLI.

This enables VSCode debugger breakpoints in transform functions:
  python -m pipeline_cli run pipeline.yaml --verbose
"""
from pipeline_cli.main import cli

cli()
