from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from pipeline_service.db import Database
from pipeline_service.settings import settings


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    db = Database(app.state._db_path)
    db.connect()
    app.state.db = db
    yield
    db.close()


def create_app(db_path: str | None = None) -> FastAPI:
    """FastAPI application factory.

    Args:
        db_path: Override the DuckDB path (useful in tests). Defaults to
                 ``settings.db_path`` (env var ``PIPELINE_SERVICE_DB``).
    """
    app = FastAPI(
        title="Pipeline Service",
        description="Pipeline intelligence and mediation layer",
        version="0.1.0",
        lifespan=_lifespan,
    )
    app.state._db_path = db_path or settings.db_path

    from pipeline_service.api import palette, pipelines, runs, sessions, ssas, templates, transforms, workspace

    app.include_router(runs.router, prefix="/runs", tags=["runs"])
    app.include_router(sessions.router, prefix="/sessions", tags=["sessions"])
    app.include_router(pipelines.router, prefix="/pipelines", tags=["pipelines"])
    app.include_router(transforms.router, prefix="/node-types", tags=["node-types"])
    app.include_router(workspace.router, prefix="/workspace", tags=["workspace"])
    app.include_router(templates.router, prefix="/templates", tags=["templates"])
    app.include_router(palette.router, prefix="/palette", tags=["palette"])
    app.include_router(ssas.router, prefix="/ssas", tags=["ssas"])

    return app


# Production singleton — used by uvicorn.
app = create_app()
