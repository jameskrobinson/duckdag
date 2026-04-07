from __future__ import annotations

import os


class Settings:
    """Reads configuration from environment variables with sensible defaults."""

    def __init__(self) -> None:
        self.db_path: str = os.getenv("PIPELINE_SERVICE_DB", "pipeline_service.duckdb")


settings = Settings()
