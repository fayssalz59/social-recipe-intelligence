"""Enrich Bronze TikTok recipe records using OpenRouter and load to Silver."""
from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, List

import requests
from pydantic import BaseModel, Field, ValidationError

from scripts.common import configure_logging, get_snowflake_connection, parse_json_strict

LOGGER = configure_logging("enrich_silver")
BRONZE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_BRONZE", "BRONZE")
SILVER_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_SILVER", "SILVER")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-001")

_LEGACY_SYSTEM_PROMPT = (
    "Tu es un agent Data Engineer spécialisé dans l'analyse de descriptions de vidéos culinaires TikTok. "
    "Ta tâche est de retourner UNIQUEMENT un objet JSON strictement valide avec ce schéma exact : "
    "{\"lang\": string, \"is_veg\": boolean, \"cuisine\": string, \"ingredient\": string}. "
    "Règles : "
    "1. Aucun texte hors JSON. "
    "2. Si l'information est absente ou ambiguë, mets \"unknown\" pour les champs texte. "
    "3. Pour is_veg, mets false si tu n'as pas assez d'information pour conclure végétarien. "
    "4. Utilise des valeurs simples et courtes. "
    "5. Déduis uniquement à partir du texte fourni, sans inventer une recette complète."
)


SYSTEM_PROMPT = (
    "You are a data engineering agent specialized in analyzing TikTok recipe video descriptions. "
    "Return only one strictly valid JSON object with this exact schema: "
    "{\"lang\": string, \"is_veg\": boolean, \"cuisine\": string, \"ingredient\": string}. "
    "Rules: "
    "1. No text outside JSON. "
    "2. If information is missing or ambiguous, use \"unknown\" for text fields. "
    "3. For is_veg, use false unless the text clearly indicates a vegetarian recipe. "
    "4. Use short, simple values. "
    "5. Infer only from the provided text; do not invent a full recipe."
)


class RecipeEnrichment(BaseModel):
    lang: str = Field(default="unknown")
    is_veg: bool = Field(default=False)
    cuisine: str = Field(default="unknown")
    ingredient: str = Field(default="unknown")


def normalize_llm_enrichment(enrichment_raw: Any) -> RecipeEnrichment:
    """Accept the LLM JSON shapes used in production and return a typed model."""
    if isinstance(enrichment_raw, list):
        if len(enrichment_raw) != 1 or not isinstance(enrichment_raw[0], dict):
            raise ValueError(
                f"Invalid LLM schema: expected dict or single-item list, got {enrichment_raw}"
            )
        enrichment_payload = enrichment_raw[0]
    elif isinstance(enrichment_raw, dict):
        enrichment_payload = enrichment_raw
    else:
        raise ValueError(
            f"Invalid LLM schema: expected dict or single-item list, got {enrichment_raw}"
        )

    normalized_payload = {
        "lang": enrichment_payload.get("lang", enrichment_payload.get("recipe_language", "unknown")),
        "is_veg": enrichment_payload.get("is_veg", enrichment_payload.get("is_vegetarian", False)),
        "cuisine": enrichment_payload.get("cuisine", enrichment_payload.get("cuisine_style", "unknown")),
        "ingredient": enrichment_payload.get("ingredient", enrichment_payload.get("main_ingredient", "unknown")),
    }

    try:
        return RecipeEnrichment.model_validate(normalized_payload)
    except ValidationError as exc:
        raise ValueError(f"Invalid LLM schema: {exc}") from exc


def fetch_unprocessed_rows(limit: int) -> List[Dict[str, Any]]:
    query = f"""
    SELECT
        b.RAW_ID,
        b.TITLE,
        b.DESCRIPTION,
        b.URL_TIKTOK,
        b.RECORD_HASH
    FROM {BRONZE_SCHEMA}.BRONZE_TIKTOK_RECIPES b
    LEFT JOIN {SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES s
        ON b.RAW_ID = s.RAW_ID
    WHERE s.RAW_ID IS NULL
      AND COALESCE(TRIM(b.DESCRIPTION), '') <> ''
    ORDER BY b.INGESTED_AT ASC
    LIMIT %(limit)s
    """
    with get_snowflake_connection(schema=BRONZE_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, {"limit": limit})
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def ask_openrouter(description: str, session: requests.Session) -> Dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY is missing")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": os.getenv("OPENROUTER_HTTP_REFERER", "https://example.com"),
        "X-Title": os.getenv("OPENROUTER_APP_NAME", "TikTok Recipe Intelligence Pipeline"),
    }

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": description},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        response = session.post(
            OPENROUTER_URL,
            headers=headers,
            json=payload,
            timeout=60,
        )

        if response.ok:
            data = response.json()
            message_text = data["choices"][0]["message"]["content"]
            enrichment_raw = parse_json_strict(message_text)

            enrichment = normalize_llm_enrichment(enrichment_raw)
            confidence = 1.0 if enrichment.lang != "unknown" else 0.5

            return {
                "structured": enrichment.model_dump(),
                "raw_response": data,
                "confidence": confidence,
            }

        retry_after = int(response.headers.get("Retry-After", "0") or "0")
        LOGGER.warning(
            "OpenRouter call failed (attempt=%s/%s, status=%s): %s",
            attempt,
            max_attempts,
            response.status_code,
            response.text[:500],
        )

        if attempt == max_attempts:
            response.raise_for_status()

        time.sleep(retry_after or attempt * 2)

    raise RuntimeError("OpenRouter request loop exited unexpectedly")


MERGE_SILVER_SQL = f"""
MERGE INTO {SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES AS target
USING (
    SELECT
        %(raw_id)s AS RAW_ID,
        %(title)s AS ORIGINAL_TITLE,
        %(description)s AS ORIGINAL_DESCRIPTION,
        %(url_tiktok)s AS URL_TIKTOK,
        %(recipe_language)s AS RECIPE_LANGUAGE,
        %(is_vegetarian)s AS IS_VEGETARIAN,
        %(cuisine_style)s AS CUISINE_STYLE,
        %(main_ingredient)s AS MAIN_INGREDIENT,
        %(processing_confidence)s AS PROCESSING_CONFIDENCE,
        %(model_name)s AS MODEL_NAME,
        PARSE_JSON(%(llm_raw_response)s) AS LLM_RAW_RESPONSE,
        %(record_hash)s AS RECORD_HASH
) AS source
ON target.RAW_ID = source.RAW_ID
WHEN MATCHED THEN UPDATE SET
    ORIGINAL_TITLE = source.ORIGINAL_TITLE,
    ORIGINAL_DESCRIPTION = source.ORIGINAL_DESCRIPTION,
    URL_TIKTOK = source.URL_TIKTOK,
    RECIPE_LANGUAGE = source.RECIPE_LANGUAGE,
    IS_VEGETARIAN = source.IS_VEGETARIAN,
    CUISINE_STYLE = source.CUISINE_STYLE,
    MAIN_INGREDIENT = source.MAIN_INGREDIENT,
    PROCESSING_CONFIDENCE = source.PROCESSING_CONFIDENCE,
    MODEL_NAME = source.MODEL_NAME,
    LLM_RAW_RESPONSE = source.LLM_RAW_RESPONSE,
    RECORD_HASH = source.RECORD_HASH,
    PROCESSED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    RAW_ID,
    ORIGINAL_TITLE,
    ORIGINAL_DESCRIPTION,
    URL_TIKTOK,
    RECIPE_LANGUAGE,
    IS_VEGETARIAN,
    CUISINE_STYLE,
    MAIN_INGREDIENT,
    PROCESSING_CONFIDENCE,
    MODEL_NAME,
    LLM_RAW_RESPONSE,
    RECORD_HASH
) VALUES (
    source.RAW_ID,
    source.ORIGINAL_TITLE,
    source.ORIGINAL_DESCRIPTION,
    source.URL_TIKTOK,
    source.RECIPE_LANGUAGE,
    source.IS_VEGETARIAN,
    source.CUISINE_STYLE,
    source.MAIN_INGREDIENT,
    source.PROCESSING_CONFIDENCE,
    source.MODEL_NAME,
    source.LLM_RAW_RESPONSE,
    source.RECORD_HASH
);
"""


def upsert_silver_row(row: Dict[str, Any], enrichment: Dict[str, Any]) -> None:
    structured = enrichment["structured"]

    payload = {
        "raw_id": row["RAW_ID"],
        "title": row["TITLE"],
        "description": row["DESCRIPTION"],
        "url_tiktok": row["URL_TIKTOK"],
        "recipe_language": structured["lang"],
        "is_vegetarian": structured["is_veg"],
        "cuisine_style": structured["cuisine"],
        "main_ingredient": structured["ingredient"],
        "processing_confidence": enrichment["confidence"],
        "model_name": OPENROUTER_MODEL,
        "llm_raw_response": json.dumps(enrichment["raw_response"]),
        "record_hash": row["RECORD_HASH"],
    }

    with get_snowflake_connection(schema=SILVER_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(MERGE_SILVER_SQL, payload)
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich Bronze records and load into Silver.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of records to process.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Call OpenRouter and log results without writing to Silver.",
    )
    args = parser.parse_args()

    rows = fetch_unprocessed_rows(limit=args.limit)
    if not rows:
        LOGGER.info("No unprocessed Bronze records found.")
        return

    LOGGER.info("Found %s Bronze rows to enrich.", len(rows))

    with requests.Session() as session:
        for row in rows:
            try:
                enrichment = ask_openrouter(row["DESCRIPTION"], session=session)

                if args.dry_run:
                    LOGGER.info(
                        "DRY RUN | RAW_ID=%s | URL=%s | RESULT=%s",
                        row["RAW_ID"],
                        row["URL_TIKTOK"],
                        enrichment["structured"],
                    )
                    continue

                upsert_silver_row(row, enrichment)
                LOGGER.info("Processed RAW_ID=%s URL=%s", row["RAW_ID"], row["URL_TIKTOK"])

            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Failed to enrich RAW_ID=%s: %s", row["RAW_ID"], exc)


if __name__ == "__main__":
    main()
