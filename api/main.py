from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import duckdb
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env", override=True)

DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", REPO_ROOT / "data" / "duckdb" / "recipes.duckdb"))

app = FastAPI(
    title="Recipe Data Platform API",
    version="2.0.0",
    description="Lightweight API exposing curated recipe metadata from a local DuckDB demo database.",
)


def get_connection() -> duckdb.DuckDBPyConnection:
    if not DUCKDB_PATH.exists():
        raise RuntimeError(
            f"DuckDB database not found at {DUCKDB_PATH}. "
            "Run `python scripts/build_duckdb_demo.py` first."
        )
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


def table_name(table: str) -> str:
    normalized = table.upper()
    if not normalized.replace("_", "").isalnum():
        raise ValueError(f"Invalid DuckDB table name: {table}")
    return normalized


def rows_as_dicts(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [desc[0].upper() for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


@app.get("/health")
def health():
    try:
        with get_connection() as conn:
            db_path = conn.execute("select current_database()").fetchone()[0]
            recipe_count = conn.execute("select count(*) from GOLD_API_RECIPE_CATALOG").fetchone()[0]
        return {
            "status": "ok",
            "engine": "duckdb",
            "database": db_path,
            "path": str(DUCKDB_PATH),
            "recipe_count": recipe_count,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/recipes")
def list_recipes(
    language: Optional[str] = Query(default=None),
    is_vegetarian: Optional[bool] = Query(default=None),
    cuisine_style: Optional[str] = Query(default=None),
    ingredient: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
):
    catalog_table = table_name("GOLD_API_RECIPE_CATALOG")
    sql = f"""
    SELECT
        ID,
        TITLE,
        URL_TIKTOK,
        LANGUAGE,
        IS_VEGETARIAN,
        CUISINE_STYLE,
        MAIN_INGREDIENT,
        INGREDIENTS,
        FINAL_RECIPE_TEXT,
        RECIPE_STATUS,
        RECIPE_QUALITY_SCORE,
        RECIPE_QUALITY_GRADE,
        CONFIDENCE,
        PROCESSED_AT
    FROM {catalog_table}
    WHERE 1=1
    """
    params: list[Any] = []

    if language:
        sql += " AND LOWER(LANGUAGE) = LOWER(?)"
        params.append(language)

    if is_vegetarian is not None:
        sql += " AND IS_VEGETARIAN = ?"
        params.append(is_vegetarian)

    if cuisine_style:
        sql += " AND LOWER(CUISINE_STYLE) = LOWER(?)"
        params.append(cuisine_style)

    if ingredient:
        sql += " AND LOWER(MAIN_INGREDIENT) = LOWER(?)"
        params.append(ingredient)

    sql += " ORDER BY PROCESSED_AT DESC LIMIT ?"
    params.append(limit)

    try:
        with get_connection() as conn:
            cursor = conn.execute(sql, params)
            rows = rows_as_dicts(cursor)
        return {"count": len(rows), "items": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/recipes/filters")
def get_filters():
    catalog_table = table_name("GOLD_API_RECIPE_CATALOG")
    sql = f"""
    SELECT DISTINCT
        LANGUAGE,
        CUISINE_STYLE,
        MAIN_INGREDIENT
    FROM {catalog_table}
    """

    try:
        with get_connection() as conn:
            rows = conn.execute(sql).fetchall()

        languages = sorted({r[0] for r in rows if r[0]})
        cuisines = sorted({r[1] for r in rows if r[1]})
        ingredients = sorted({r[2] for r in rows if r[2]})

        return {
            "languages": languages,
            "cuisines": cuisines,
            "ingredients": ingredients,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/recipes/{raw_id}")
def get_recipe(raw_id: int):
    catalog_table = table_name("GOLD_API_RECIPE_CATALOG")
    sql = f"""
    SELECT
        ID,
        TITLE,
        URL_TIKTOK,
        LANGUAGE,
        IS_VEGETARIAN,
        CUISINE_STYLE,
        MAIN_INGREDIENT,
        INGREDIENTS,
        FINAL_RECIPE_TITLE,
        FINAL_RECIPE_TEXT,
        FINAL_RECIPE_JSON,
        RECIPE_STATUS,
        RECIPE_QUALITY_SCORE,
        RECIPE_QUALITY_GRADE,
        CONFIDENCE,
        PROCESSED_AT
    FROM {catalog_table}
    WHERE ID = ?
    """

    try:
        with get_connection() as conn:
            cursor = conn.execute(sql, [raw_id])
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Recipe not found")
            columns = [desc[0].upper() for desc in cursor.description]
            return dict(zip(columns, row))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
