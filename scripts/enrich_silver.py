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
    "{\"lang\": string, \"is_veg\": boolean|null, \"cuisine\": string, "
    "\"ingredient\": string, \"ingredients\": array<string>, \"is_recipe\": boolean, "
    "\"recipe_status\": string, \"has_ingredient_list\": boolean, "
    "\"has_instructions\": boolean, \"caption_completeness_score\": number, "
    "\"rejection_reason\": string}. "
    "Rules: "
    "1. No text outside JSON. "
    "2. Normalize lang to ISO codes only: en, fr, es, it, pt, ar, or unknown. "
    "3. recipe_status must be one of: full_recipe, partial_recipe, food_content, non_recipe. "
    "4. full_recipe means the text contains enough ingredients and cooking instructions to recreate the dish. "
    "5. partial_recipe means it is clearly a recipe but ingredients or steps are incomplete. "
    "6. food_content means food-related but not a usable recipe. "
    "7. non_recipe means not food/recipe content. "
    "8. ingredients must be a list of explicit ingredients found in the text, not guesses. "
    "9. ingredient is the main ingredient only. "
    "10. caption_completeness_score is between 0 and 1. "
    "11. Infer only from the provided text; do not invent missing ingredients or steps."
)


class RecipeEnrichment(BaseModel):
    lang: str = Field(default="unknown")
    is_veg: bool | None = Field(default=False)
    cuisine: str = Field(default="unknown")
    ingredient: str = Field(default="unknown")
    ingredients: list[str] = Field(default_factory=list)
    is_recipe: bool = Field(default=False)
    recipe_status: str = Field(default="food_content")
    has_ingredient_list: bool = Field(default=False)
    has_instructions: bool = Field(default=False)
    caption_completeness_score: float = Field(default=0.0)
    rejection_reason: str = Field(default="")


def coerce_nullable_bool(value: Any) -> bool | None:
    """Normalize common LLM boolean variants without failing the whole row."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "y", "1", "vegetarian", "veg", "végétarien"}:
            return True
        if normalized in {"false", "no", "n", "0", "non-vegetarian", "non vegetarian", "not vegetarian"}:
            return False
        if normalized in {"unknown", "null", "none", "n/a", "na", "unclear", ""}:
            return None
    return None


def coerce_bool(value: Any, default: bool = False) -> bool:
    coerced = coerce_nullable_bool(value)
    return default if coerced is None else coerced


def normalize_language(value: Any) -> str:
    normalized = str(value or "unknown").strip().lower()
    mapping = {
        "english": "en",
        "eng": "en",
        "french": "fr",
        "français": "fr",
        "francais": "fr",
        "spanish": "es",
        "español": "es",
        "italian": "it",
        "italiano": "it",
        "portuguese": "pt",
        "portugues": "pt",
        "português": "pt",
        "arabic": "ar",
        "العربية": "ar",
    }
    normalized = mapping.get(normalized, normalized)
    return normalized if normalized in {"en", "fr", "es", "it", "pt", "ar"} else "unknown"


def normalize_text_field(value: Any) -> str:
    text = str(value or "unknown").strip().lower()
    return text or "unknown"


def normalize_ingredients(value: Any, main_ingredient: str) -> list[str]:
    if isinstance(value, list):
        raw_values = value
    elif isinstance(value, str):
        raw_values = re_split_ingredients(value)
    else:
        raw_values = []

    output: list[str] = []
    for item in raw_values:
        ingredient = str(item or "").strip().lower()
        if ingredient and ingredient not in {"unknown", "none", "n/a", "null"} and ingredient not in output:
            output.append(ingredient)

    if not output and main_ingredient not in {"", "unknown"}:
        output.append(main_ingredient)
    return output


def re_split_ingredients(value: str) -> list[str]:
    separators = [",", ";", "|", "\n"]
    parts = [value]
    for separator in separators:
        parts = [piece for part in parts for piece in part.split(separator)]
    return parts


def normalize_recipe_status(value: Any, is_recipe: bool, completeness: float) -> str:
    status = str(value or "").strip().lower()
    status = status.replace(" ", "_").replace("-", "_")
    if status in {"full_recipe", "partial_recipe", "food_content", "non_recipe"}:
        return status
    if is_recipe and completeness >= 0.75:
        return "full_recipe"
    if is_recipe:
        return "partial_recipe"
    return "food_content"


def coerce_score(value: Any) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, score))


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

    lang = normalize_language(enrichment_payload.get("lang", enrichment_payload.get("recipe_language", "unknown")))
    main_ingredient = normalize_text_field(
        enrichment_payload.get("ingredient", enrichment_payload.get("main_ingredient", "unknown"))
    )
    ingredients = normalize_ingredients(
        enrichment_payload.get("ingredients", enrichment_payload.get("ingredient_list", [])),
        main_ingredient=main_ingredient,
    )
    completeness = coerce_score(enrichment_payload.get("caption_completeness_score", enrichment_payload.get("completeness", 0)))
    has_ingredient_list = coerce_bool(
        enrichment_payload.get("has_ingredient_list", len(ingredients) >= 2),
        default=len(ingredients) >= 2,
    )
    has_instructions = coerce_bool(
        enrichment_payload.get("has_instructions", completeness >= 0.65),
        default=completeness >= 0.65,
    )
    is_recipe = coerce_bool(
        enrichment_payload.get(
            "is_recipe",
            has_ingredient_list or has_instructions or completeness >= 0.5,
        ),
        default=has_ingredient_list or has_instructions or completeness >= 0.5,
    )
    recipe_status = normalize_recipe_status(
        enrichment_payload.get("recipe_status", enrichment_payload.get("status")),
        is_recipe=is_recipe,
        completeness=completeness,
    )

    normalized_payload = {
        "lang": lang,
        "is_veg": coerce_nullable_bool(
            enrichment_payload.get("is_veg", enrichment_payload.get("is_vegetarian", False))
        ),
        "cuisine": normalize_text_field(enrichment_payload.get("cuisine", enrichment_payload.get("cuisine_style", "unknown"))),
        "ingredient": main_ingredient,
        "ingredients": ingredients,
        "is_recipe": is_recipe,
        "recipe_status": recipe_status,
        "has_ingredient_list": has_ingredient_list,
        "has_instructions": has_instructions,
        "caption_completeness_score": completeness,
        "rejection_reason": str(enrichment_payload.get("rejection_reason", "") or ""),
    }

    try:
        return RecipeEnrichment.model_validate(normalized_payload)
    except ValidationError as exc:
        raise ValueError(f"Invalid LLM schema: {exc}") from exc


def fetch_bronze_rows(limit: int, reprocess_all: bool = False) -> List[Dict[str, Any]]:
    silver_filter = "" if reprocess_all else "AND s.RAW_ID IS NULL"
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
    WHERE COALESCE(TRIM(b.DESCRIPTION), '') <> ''
      {silver_filter}
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
            confidence = max(
                0.1,
                min(
                    1.0,
                    (0.25 if enrichment.lang != "unknown" else 0.0)
                    + (0.25 if enrichment.is_recipe else 0.0)
                    + (0.2 if enrichment.has_ingredient_list else 0.0)
                    + (0.2 if enrichment.has_instructions else 0.0)
                    + (0.1 * enrichment.caption_completeness_score),
                ),
            )

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
        PARSE_JSON(%(ingredients)s) AS INGREDIENTS,
        %(is_recipe)s AS IS_RECIPE,
        %(recipe_status)s AS RECIPE_STATUS,
        %(has_ingredient_list)s AS HAS_INGREDIENT_LIST,
        %(has_instructions)s AS HAS_INSTRUCTIONS,
        %(caption_completeness_score)s AS CAPTION_COMPLETENESS_SCORE,
        %(rejection_reason)s AS REJECTION_REASON,
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
    INGREDIENTS = source.INGREDIENTS,
    IS_RECIPE = source.IS_RECIPE,
    RECIPE_STATUS = source.RECIPE_STATUS,
    HAS_INGREDIENT_LIST = source.HAS_INGREDIENT_LIST,
    HAS_INSTRUCTIONS = source.HAS_INSTRUCTIONS,
    CAPTION_COMPLETENESS_SCORE = source.CAPTION_COMPLETENESS_SCORE,
    REJECTION_REASON = source.REJECTION_REASON,
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
    INGREDIENTS,
    IS_RECIPE,
    RECIPE_STATUS,
    HAS_INGREDIENT_LIST,
    HAS_INSTRUCTIONS,
    CAPTION_COMPLETENESS_SCORE,
    REJECTION_REASON,
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
    source.INGREDIENTS,
    source.IS_RECIPE,
    source.RECIPE_STATUS,
    source.HAS_INGREDIENT_LIST,
    source.HAS_INSTRUCTIONS,
    source.CAPTION_COMPLETENESS_SCORE,
    source.REJECTION_REASON,
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
        "ingredients": json.dumps(structured.get("ingredients") or []),
        "is_recipe": structured.get("is_recipe", False),
        "recipe_status": structured.get("recipe_status", "food_content"),
        "has_ingredient_list": structured.get("has_ingredient_list", False),
        "has_instructions": structured.get("has_instructions", False),
        "caption_completeness_score": structured.get("caption_completeness_score", 0),
        "rejection_reason": structured.get("rejection_reason", ""),
        "processing_confidence": enrichment["confidence"],
        "model_name": OPENROUTER_MODEL,
        "llm_raw_response": json.dumps(enrichment["raw_response"]),
        "record_hash": row["RECORD_HASH"],
    }

    with get_snowflake_connection(schema=SILVER_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(MERGE_SILVER_SQL, payload)
        conn.commit()


def ensure_silver_schema() -> None:
    statements = [
        f"ALTER TABLE {SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES ADD COLUMN IF NOT EXISTS INGREDIENTS VARIANT",
        f"ALTER TABLE {SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES ADD COLUMN IF NOT EXISTS IS_RECIPE BOOLEAN DEFAULT TRUE",
        f"ALTER TABLE {SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES ADD COLUMN IF NOT EXISTS RECIPE_STATUS STRING DEFAULT 'partial_recipe'",
        f"ALTER TABLE {SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES ADD COLUMN IF NOT EXISTS HAS_INGREDIENT_LIST BOOLEAN DEFAULT FALSE",
        f"ALTER TABLE {SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES ADD COLUMN IF NOT EXISTS HAS_INSTRUCTIONS BOOLEAN DEFAULT FALSE",
        f"ALTER TABLE {SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES ADD COLUMN IF NOT EXISTS CAPTION_COMPLETENESS_SCORE FLOAT DEFAULT 0",
        f"ALTER TABLE {SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES ADD COLUMN IF NOT EXISTS REJECTION_REASON STRING",
    ]
    with get_snowflake_connection(schema=SILVER_SCHEMA) as conn:
        with conn.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich Bronze records and load into Silver.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of records to process.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Call OpenRouter and log results without writing to Silver.",
    )
    parser.add_argument(
        "--reprocess-all",
        action="store_true",
        help="Re-enrich Bronze rows even if they already exist in Silver. The Silver MERGE updates existing rows.",
    )
    args = parser.parse_args()

    ensure_silver_schema()
    rows = fetch_bronze_rows(limit=args.limit, reprocess_all=args.reprocess_all)
    if not rows:
        LOGGER.info("No Bronze records found for the selected mode.")
        return

    LOGGER.info(
        "Found %s Bronze rows to enrich. reprocess_all=%s",
        len(rows),
        args.reprocess_all,
    )

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
