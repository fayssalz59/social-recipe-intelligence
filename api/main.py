from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv
import snowflake.connector

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env", override=True)

app = FastAPI(
    title="Recipe Data Platform API",
    version="1.0.0",
    description="Lightweight API exposing curated recipe metadata from Snowflake Gold API layer.",
)


def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "PORTFOLIO_WH"),
        database=os.getenv("SNOWFLAKE_DB", "TIKTOK_PORTFOLIO_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA_GOLD", "GOLD"),
        role=os.getenv("SNOWFLAKE_ROLE", "agent_role"),
    )


@app.get("/health")
def health():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
                row = cur.fetchone()
        return {
            "status": "ok",
            "database": row[0],
            "schema": row[1],
            "warehouse": row[2],
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
    sql = """
    SELECT
        ID,
        TITLE,
        URL_TIKTOK,
        LANGUAGE,
        IS_VEGETARIAN,
        CUISINE_STYLE,
        MAIN_INGREDIENT,
        CONFIDENCE,
        PROCESSED_AT
    FROM GOLD.GOLD_API_RECIPE_CATALOG
    WHERE 1=1
    """
    params = {}

    if language:
        sql += " AND LOWER(LANGUAGE) = LOWER(%(language)s)"
        params["language"] = language

    if is_vegetarian is not None:
        sql += " AND IS_VEGETARIAN = %(is_vegetarian)s"
        params["is_vegetarian"] = is_vegetarian

    if cuisine_style:
        sql += " AND LOWER(CUISINE_STYLE) = LOWER(%(cuisine_style)s)"
        params["cuisine_style"] = cuisine_style

    if ingredient:
        sql += " AND LOWER(MAIN_INGREDIENT) = LOWER(%(ingredient)s)"
        params["ingredient"] = ingredient

    sql += " ORDER BY PROCESSED_AT DESC LIMIT %(limit)s"
    params["limit"] = limit

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"count": len(rows), "items": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))




@app.get("/recipes/filters")
def get_filters():
    sql = """
    SELECT DISTINCT
        LANGUAGE,
        CUISINE_STYLE,
        MAIN_INGREDIENT
    FROM GOLD.GOLD_API_RECIPE_CATALOG
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()

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
    sql = """
    SELECT
        ID,
        TITLE,
        URL_TIKTOK,
        LANGUAGE,
        IS_VEGETARIAN,
        CUISINE_STYLE,
        MAIN_INGREDIENT,
        CONFIDENCE,
        PROCESSED_AT
    FROM GOLD.GOLD_API_RECIPE_CATALOG
    WHERE ID = %(raw_id)s
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"raw_id": raw_id})
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Recipe not found")
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

