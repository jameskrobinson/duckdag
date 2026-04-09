"""SSAS (SQL Server Analysis Services) metadata and Cube Browser endpoints.

Provides:
- POST /ssas/metadata  — connect to SSAS and return cube/dimension/measure schema.
- POST /ssas/members   — return level members for drill-down in the Cube Browser.

These endpoints are used exclusively by the frontend Cube Browser to let users
build MDX queries graphically.  Requires ``pyadomd`` and the ADOMD.NET client
libraries to be installed on the server host.

    pip install pyadomd
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()


# ---------------------------------------------------------------------------
# Shared connection model
# ---------------------------------------------------------------------------

class SSASConnectionParams(BaseModel):
    server: str | None = None
    catalog: str | None = None
    cube: str | None = None
    uid: str | None = None
    pwd: str | None = None
    trusted: bool = True
    connection_string: str | None = None


# ---------------------------------------------------------------------------
# Metadata response models
# ---------------------------------------------------------------------------

class SSASCubeInfo(BaseModel):
    name: str


class SSASLevel(BaseModel):
    name: str
    unique_name: str
    level_number: int = 0


class SSASHierarchy(BaseModel):
    name: str
    unique_name: str
    levels: list[SSASLevel] = []


class SSASDimension(BaseModel):
    name: str
    unique_name: str
    is_measures: bool = False
    hierarchies: list[SSASHierarchy] = []


class SSASMeasure(BaseModel):
    name: str
    unique_name: str
    display_folder: str = ""


class SSASMetadata(BaseModel):
    cubes: list[SSASCubeInfo] = []
    dimensions: list[SSASDimension] = []
    measures: list[SSASMeasure] = []


# ---------------------------------------------------------------------------
# Members response models
# ---------------------------------------------------------------------------

class SSASMembersRequest(BaseModel):
    connection: SSASConnectionParams
    cube: str
    hierarchy_unique_name: str
    level_number: int = 0
    max_members: int = 200


class SSASMember(BaseModel):
    name: str
    unique_name: str
    caption: str


class SSASMembersResponse(BaseModel):
    members: list[SSASMember] = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_conn_str(p: SSASConnectionParams) -> str:
    if p.connection_string:
        return p.connection_string
    parts = ["Provider=MSOLAP"]
    if p.server:
        parts.append(f"Data Source={p.server}")
    if p.catalog:
        parts.append(f"Initial Catalog={p.catalog}")
    if p.trusted:
        parts.append("Integrated Security=SSPI")
    else:
        if p.uid:
            parts.append(f"User ID={p.uid}")
        if p.pwd:
            parts.append(f"Password={p.pwd}")
    return ";".join(parts)


def _get_pyadomd():
    """Import pyadomd, raising a clear error if it is not installed."""
    try:
        from pyadomd import Pyadomd  # type: ignore[import-untyped]
        return Pyadomd
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail=(
                "pyadomd is not installed on this server. "
                "Install it with: pip install pyadomd\n"
                "Also ensure the Microsoft ADOMD.NET client libraries are installed "
                "(included with SQL Server client tools or SSAS Management Studio)."
            ),
        )


def _query_dmv(conn: Any, dmv: str) -> list[dict[str, Any]]:
    """Execute a DMV query and return rows as dicts."""
    cur = conn.cursor()
    cur.execute(dmv)
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# POST /ssas/metadata
# ---------------------------------------------------------------------------

@router.post("/metadata", response_model=SSASMetadata)
def get_ssas_metadata(params: SSASConnectionParams) -> SSASMetadata:
    """Connect to an SSAS instance and return its cube/dimension/measure metadata.

    Used by the Cube Browser to populate the drag-and-drop MDX axis builder.
    The ``cube`` param narrows results to a single cube (recommended).

    Requirements:
    - ``pyadomd`` Python package installed.
    - Microsoft ADOMD.NET client libraries on the host machine.
    """
    Pyadomd = _get_pyadomd()
    conn_str = _build_conn_str(params)

    try:
        conn = Pyadomd(conn_str)
        conn.open()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Cannot connect to SSAS: {exc}")

    try:
        cube_filter = f"AND CUBE_NAME = '{params.cube}'" if params.cube else ""

        # Cubes in this catalog
        cube_rows = _query_dmv(
            conn,
            "SELECT CATALOG_NAME, CUBE_NAME, CUBE_TYPE "
            "FROM $system.MDSCHEMA_CUBES WHERE CUBE_TYPE = 'CUBE'",
        )
        cubes = [SSASCubeInfo(name=r["CUBE_NAME"]) for r in cube_rows if r.get("CUBE_NAME")]

        # Dimensions
        dim_rows = _query_dmv(
            conn,
            f"SELECT DIMENSION_UNIQUE_NAME, DIMENSION_CAPTION, DIMENSION_TYPE "
            f"FROM $system.MDSCHEMA_DIMENSIONS WHERE 1=1 {cube_filter}",
        )
        dimensions: list[SSASDimension] = []
        for dr in dim_rows:
            uname = dr.get("DIMENSION_UNIQUE_NAME", "")
            caption = dr.get("DIMENSION_CAPTION", uname)
            # DIMENSION_TYPE 2 = Measures
            is_measures = str(dr.get("DIMENSION_TYPE", "")) == "2"
            dimensions.append(
                SSASDimension(name=caption, unique_name=uname, is_measures=is_measures)
            )

        # Hierarchies — attach to each dimension
        hier_rows = _query_dmv(
            conn,
            f"SELECT HIERARCHY_UNIQUE_NAME, HIERARCHY_CAPTION, DIMENSION_UNIQUE_NAME "
            f"FROM $system.MDSCHEMA_HIERARCHIES WHERE 1=1 {cube_filter}",
        )
        hier_by_dim: dict[str, list[SSASHierarchy]] = {}
        for hr in hier_rows:
            dim_uname = hr.get("DIMENSION_UNIQUE_NAME", "")
            h = SSASHierarchy(
                name=hr.get("HIERARCHY_CAPTION", ""),
                unique_name=hr.get("HIERARCHY_UNIQUE_NAME", ""),
            )
            hier_by_dim.setdefault(dim_uname, []).append(h)
        for dim in dimensions:
            dim.hierarchies = hier_by_dim.get(dim.unique_name, [])

        # Levels — attach to each hierarchy
        level_rows = _query_dmv(
            conn,
            f"SELECT LEVEL_UNIQUE_NAME, LEVEL_CAPTION, HIERARCHY_UNIQUE_NAME, LEVEL_NUMBER "
            f"FROM $system.MDSCHEMA_LEVELS WHERE 1=1 {cube_filter} "
            f"ORDER BY HIERARCHY_UNIQUE_NAME, LEVEL_NUMBER",
        )
        levels_by_hier: dict[str, list[SSASLevel]] = {}
        for lr in level_rows:
            hier_uname = lr.get("HIERARCHY_UNIQUE_NAME", "")
            levels_by_hier.setdefault(hier_uname, []).append(
                SSASLevel(
                    name=lr.get("LEVEL_CAPTION", ""),
                    unique_name=lr.get("LEVEL_UNIQUE_NAME", ""),
                    level_number=int(lr.get("LEVEL_NUMBER", 0)),
                )
            )
        for dim in dimensions:
            for hier in dim.hierarchies:
                hier.levels = levels_by_hier.get(hier.unique_name, [])

        # Measures
        measure_rows = _query_dmv(
            conn,
            f"SELECT MEASURE_UNIQUE_NAME, MEASURE_CAPTION, MEASURE_DISPLAY_FOLDER "
            f"FROM $system.MDSCHEMA_MEASURES WHERE 1=1 {cube_filter}",
        )
        measures = [
            SSASMeasure(
                name=r.get("MEASURE_CAPTION", ""),
                unique_name=r.get("MEASURE_UNIQUE_NAME", ""),
                display_folder=r.get("MEASURE_DISPLAY_FOLDER", "") or "",
            )
            for r in measure_rows
        ]

        return SSASMetadata(cubes=cubes, dimensions=dimensions, measures=measures)

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"SSAS metadata error: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# POST /ssas/members
# ---------------------------------------------------------------------------

@router.post("/members", response_model=SSASMembersResponse)
def get_ssas_members(body: SSASMembersRequest) -> SSASMembersResponse:
    """Return level members for a hierarchy — used for drill-down in the Cube Browser."""
    Pyadomd = _get_pyadomd()
    conn_str = _build_conn_str(body.connection)

    try:
        conn = Pyadomd(conn_str)
        conn.open()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Cannot connect to SSAS: {exc}")

    try:
        hier_escaped = body.hierarchy_unique_name.replace("'", "''")
        cube_escaped = body.cube.replace("'", "''")
        dmv = (
            f"SELECT MEMBER_UNIQUE_NAME, MEMBER_CAPTION "
            f"FROM $system.MDSCHEMA_MEMBERS "
            f"WHERE CUBE_NAME = '{cube_escaped}' "
            f"AND HIERARCHY_UNIQUE_NAME = '{hier_escaped}' "
            f"AND LEVEL_NUMBER = {body.level_number}"
        )
        rows = _query_dmv(conn, dmv)
        members = [
            SSASMember(
                name=r.get("MEMBER_CAPTION", ""),
                unique_name=r.get("MEMBER_UNIQUE_NAME", ""),
                caption=r.get("MEMBER_CAPTION", ""),
            )
            for r in rows[: body.max_members]
        ]
        return SSASMembersResponse(members=members)

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"SSAS members error: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
