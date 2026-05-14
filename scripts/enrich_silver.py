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
from scripts.recipe_evidence_scoring import compute_recipe_evidence_score

try:
    from tqdm.auto import tqdm
except ModuleNotFoundError:  # pragma: no cover - tqdm is a convenience dependency
    def tqdm(iterable, **_: Any):  # type: ignore[no-redef]
        return iterable

LOGGER = configure_logging("enrich_silver")
BRONZE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_BRONZE", "BRONZE")
SILVER_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_SILVER", "SILVER")
CONTROL_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_CONTROL", "CONTROL")
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
    "\"rejection_reason\": string, \"final_recipe_title\": string, "
    "\"final_recipe_text\": string, \"final_recipe_json\": object, "
    "\"missing_recipe_info\": array<string>, \"final_recipe_confidence\": number, "
    "\"final_recipe_language\": string}. "
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
    "11. final_recipe_text is a clean user-facing recipe in Markdown using only provided evidence. "
    "12. Do not invent missing quantities, timings, ingredients, or steps. Use 'not specified' when needed. "
    "13. final_recipe_json must contain title, ingredients, steps, notes, and missing_info arrays. "
    "14. If the evidence is not a recipe, keep final_recipe_text empty and explain in rejection_reason. "
    "15. Infer only from the provided text; do not invent missing ingredients or steps."
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
    final_recipe_title: str = Field(default="")
    final_recipe_text: str = Field(default="")
    final_recipe_json: dict[str, Any] = Field(default_factory=dict)
    missing_recipe_info: list[str] = Field(default_factory=list)
    final_recipe_confidence: float = Field(default=0.0)
    final_recipe_language: str = Field(default="unknown")


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


def normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        raw_values = value
    elif isinstance(value, str):
        raw_values = re_split_ingredients(value)
    else:
        raw_values = []

    output: list[str] = []
    for item in raw_values:
        text = str(item or "").strip()
        if text and text.lower() not in {"unknown", "none", "n/a", "null"} and text not in output:
            output.append(text)
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


def normalize_final_recipe_json(value: Any, title: str, ingredients: list[str], missing_info: list[str]) -> dict[str, Any]:
    if isinstance(value, dict):
        output = dict(value)
    else:
        output = {}
    output.setdefault("title", title or "Untitled recipe")
    output.setdefault("ingredients", ingredients)
    output.setdefault("steps", [])
    output.setdefault("notes", [])
    output.setdefault("missing_info", missing_info)
    return output


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
    final_recipe_language = normalize_language(
        enrichment_payload.get("final_recipe_language", enrichment_payload.get("lang", lang))
    )
    final_recipe_title = str(enrichment_payload.get("final_recipe_title", enrichment_payload.get("recipe_title", "")) or "").strip()
    final_recipe_text = str(enrichment_payload.get("final_recipe_text", enrichment_payload.get("recipe_text", "")) or "").strip()
    missing_recipe_info = normalize_string_list(
        enrichment_payload.get("missing_recipe_info", enrichment_payload.get("missing_info", []))
    )
    final_recipe_confidence = coerce_score(
        enrichment_payload.get("final_recipe_confidence", enrichment_payload.get("recipe_confidence", completeness))
    )
    if not is_recipe:
        final_recipe_title = ""
        final_recipe_text = ""
        final_recipe_confidence = 0.0
    final_recipe_json = normalize_final_recipe_json(
        enrichment_payload.get("final_recipe_json", enrichment_payload.get("recipe_json", {})),
        title=final_recipe_title,
        ingredients=ingredients,
        missing_info=missing_recipe_info,
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
        "final_recipe_title": final_recipe_title,
        "final_recipe_text": final_recipe_text,
        "final_recipe_json": final_recipe_json,
        "missing_recipe_info": missing_recipe_info,
        "final_recipe_confidence": final_recipe_confidence,
        "final_recipe_language": final_recipe_language,
    }

    try:
        return RecipeEnrichment.model_validate(normalized_payload)
    except ValidationError as exc:
        raise ValueError(f"Invalid LLM schema: {exc}") from exc


def fetch_bronze_rows(
    limit: int,
    reprocess_all: bool = False,
    only_recovered: bool = False,
) -> List[Dict[str, Any]]:
    if reprocess_all:
        silver_filter = ""
    elif only_recovered:
        silver_filter = """
        AND e.LATEST_EVIDENCE_AT IS NOT NULL
        AND (
            s.RAW_ID IS NULL
            OR e.LATEST_EVIDENCE_AT >= COALESCE(s.PROCESSED_AT, '1900-01-01'::TIMESTAMP_NTZ)
        )
        """
    else:
        silver_filter = "AND s.RAW_ID IS NULL"
    query = f"""
    WITH evidence AS (
        SELECT
            RAW_ID,
            LISTAGG(EVIDENCE_TEXT, '\n\n') WITHIN GROUP (
                ORDER BY EVIDENCE_QUALITY_SCORE DESC, CREATED_AT DESC
            ) AS RECOVERY_TEXT,
            MAX(EVIDENCE_QUALITY_SCORE) AS EVIDENCE_QUALITY_SCORE,
            MAX(CREATED_AT) AS LATEST_EVIDENCE_AT
        FROM {SILVER_SCHEMA}.SILVER_RECIPE_EVIDENCE
        WHERE COALESCE(EVIDENCE_LENGTH, 0) > 0
          AND COALESCE(EVIDENCE_QUALITY_SCORE, 0) >= 0.20
        GROUP BY RAW_ID
    )
    SELECT
        b.RAW_ID,
        b.TITLE,
        b.DESCRIPTION,
        COALESCE(NULLIF(b.RAW_PAYLOAD:original_description::STRING, ''), b.DESCRIPTION) AS ORIGINAL_DESCRIPTION,
        NULLIF(
            CONCAT_WS(
                '\n\n',
                NULLIF(b.RAW_PAYLOAD:recovered_text::STRING, ''),
                NULLIF(e.RECOVERY_TEXT, '')
            ),
            ''
        ) AS RECOVERED_TEXT,
        CONCAT_WS(
            '\n\n',
            COALESCE(NULLIF(b.RAW_PAYLOAD:original_description::STRING, ''), b.DESCRIPTION),
            NULLIF(b.RAW_PAYLOAD:recovered_text::STRING, ''),
            NULLIF(e.RECOVERY_TEXT, '')
        ) AS EVIDENCE_TEXT,
        COALESCE(e.EVIDENCE_QUALITY_SCORE, 0) AS EVIDENCE_QUALITY_SCORE,
        e.LATEST_EVIDENCE_AT,
        b.URL_TIKTOK,
        b.RECORD_HASH
    FROM {BRONZE_SCHEMA}.BRONZE_TIKTOK_RECIPES b
    LEFT JOIN {SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES s
        ON b.RAW_ID = s.RAW_ID
    LEFT JOIN evidence e
        ON b.RAW_ID = e.RAW_ID
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


def build_evidence_prompt(row: Dict[str, Any]) -> str:
    original_description = str(row.get("ORIGINAL_DESCRIPTION") or row.get("DESCRIPTION") or "").strip()
    recovered_text = str(row.get("RECOVERED_TEXT") or "").strip()
    evidence_text = str(row.get("EVIDENCE_TEXT") or row.get("DESCRIPTION") or "").strip()
    payload = {
        "source": "social_recipe_video",
        "original_caption": original_description,
        "recovered_text": recovered_text,
        "evidence_text": evidence_text,
        "evidence_quality_score": row.get("EVIDENCE_QUALITY_SCORE", 0),
        "instruction": (
            "Analyze the evidence and produce structured metadata plus a clean final recipe. "
            "Only use facts present in original_caption, recovered_text, or evidence_text."
        ),
    }
    return json.dumps(payload, ensure_ascii=False)


def ask_openrouter(row: Dict[str, Any], session: requests.Session) -> Dict[str, Any]:
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
            {"role": "user", "content": build_evidence_prompt(row)},
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
        %(original_description)s AS ORIGINAL_DESCRIPTION,
        %(recovered_text)s AS RECOVERED_TEXT,
        %(evidence_text)s AS EVIDENCE_TEXT,
        %(evidence_quality_score)s AS EVIDENCE_QUALITY_SCORE,
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
        %(final_recipe_title)s AS FINAL_RECIPE_TITLE,
        %(final_recipe_text)s AS FINAL_RECIPE_TEXT,
        PARSE_JSON(%(final_recipe_json)s) AS FINAL_RECIPE_JSON,
        PARSE_JSON(%(missing_recipe_info)s) AS MISSING_RECIPE_INFO,
        %(final_recipe_confidence)s AS FINAL_RECIPE_CONFIDENCE,
        %(final_recipe_language)s AS FINAL_RECIPE_LANGUAGE,
        %(processing_confidence)s AS PROCESSING_CONFIDENCE,
        %(model_name)s AS MODEL_NAME,
        PARSE_JSON(%(llm_raw_response)s) AS LLM_RAW_RESPONSE,
        %(record_hash)s AS RECORD_HASH
) AS source
ON target.RAW_ID = source.RAW_ID
WHEN MATCHED THEN UPDATE SET
    ORIGINAL_TITLE = source.ORIGINAL_TITLE,
    ORIGINAL_DESCRIPTION = source.ORIGINAL_DESCRIPTION,
    RECOVERED_TEXT = source.RECOVERED_TEXT,
    EVIDENCE_TEXT = source.EVIDENCE_TEXT,
    EVIDENCE_QUALITY_SCORE = source.EVIDENCE_QUALITY_SCORE,
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
    FINAL_RECIPE_TITLE = source.FINAL_RECIPE_TITLE,
    FINAL_RECIPE_TEXT = source.FINAL_RECIPE_TEXT,
    FINAL_RECIPE_JSON = source.FINAL_RECIPE_JSON,
    MISSING_RECIPE_INFO = source.MISSING_RECIPE_INFO,
    FINAL_RECIPE_CONFIDENCE = source.FINAL_RECIPE_CONFIDENCE,
    FINAL_RECIPE_LANGUAGE = source.FINAL_RECIPE_LANGUAGE,
    PROCESSING_CONFIDENCE = source.PROCESSING_CONFIDENCE,
    MODEL_NAME = source.MODEL_NAME,
    LLM_RAW_RESPONSE = source.LLM_RAW_RESPONSE,
    RECORD_HASH = source.RECORD_HASH,
    PROCESSED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    RAW_ID,
    ORIGINAL_TITLE,
    ORIGINAL_DESCRIPTION,
    RECOVERED_TEXT,
    EVIDENCE_TEXT,
    EVIDENCE_QUALITY_SCORE,
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
    FINAL_RECIPE_TITLE,
    FINAL_RECIPE_TEXT,
    FINAL_RECIPE_JSON,
    MISSING_RECIPE_INFO,
    FINAL_RECIPE_CONFIDENCE,
    FINAL_RECIPE_LANGUAGE,
    PROCESSING_CONFIDENCE,
    MODEL_NAME,
    LLM_RAW_RESPONSE,
    RECORD_HASH
) VALUES (
    source.RAW_ID,
    source.ORIGINAL_TITLE,
    source.ORIGINAL_DESCRIPTION,
    source.RECOVERED_TEXT,
    source.EVIDENCE_TEXT,
    source.EVIDENCE_QUALITY_SCORE,
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
    source.FINAL_RECIPE_TITLE,
    source.FINAL_RECIPE_TEXT,
    source.FINAL_RECIPE_JSON,
    source.MISSING_RECIPE_INFO,
    source.FINAL_RECIPE_CONFIDENCE,
    source.FINAL_RECIPE_LANGUAGE,
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
        "original_description": row.get("ORIGINAL_DESCRIPTION") or row["DESCRIPTION"],
        "recovered_text": row.get("RECOVERED_TEXT") or "",
        "evidence_text": row.get("EVIDENCE_TEXT") or row["DESCRIPTION"],
        "evidence_quality_score": row.get("EVIDENCE_QUALITY_SCORE")
        or compute_recipe_evidence_score(row.get("EVIDENCE_TEXT") or row["DESCRIPTION"]),
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
        "final_recipe_title": structured.get("final_recipe_title", ""),
        "final_recipe_text": structured.get("final_recipe_text", ""),
        "final_recipe_json": json.dumps(structured.get("final_recipe_json") or {}),
        "missing_recipe_info": json.dumps(structured.get("missing_recipe_info") or []),
        "final_recipe_confidence": structured.get("final_recipe_confidence", 0),
        "final_recipe_language": structured.get("final_recipe_language", structured.get("lang", "unknown")),
        "processing_confidence": enrichment["confidence"],
        "model_name": OPENROUTER_MODEL,
        "llm_raw_response": json.dumps(enrichment["raw_response"]),
        "record_hash": row["RECORD_HASH"],
    }

    with get_snowflake_connection(schema=SILVER_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(MERGE_SILVER_SQL, payload)
        conn.commit()
    update_processing_queue_after_enrichment(row["RAW_ID"], structured)


def update_processing_queue_after_enrichment(raw_id: Any, structured: dict[str, Any]) -> None:
    recipe_status = structured.get("recipe_status", "food_content")
    if recipe_status == "full_recipe":
        status = "extracted_success"
    elif recipe_status == "partial_recipe":
        status = "extracted_partial"
    elif structured.get("is_recipe", False):
        status = "low_quality_rejected"
    else:
        status = "failed"

    query = f"""
    UPDATE {CONTROL_SCHEMA}.RECIPE_PROCESSING_QUEUE
    SET
        STATUS = %(status)s,
        UPDATED_AT = CURRENT_TIMESTAMP(),
        LAST_ERROR = IFF(%(status)s = 'failed', %(reason)s, NULL)
    WHERE RAW_ID = %(raw_id)s
    """
    try:
        with get_snowflake_connection(schema=CONTROL_SCHEMA) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    query,
                    {
                        "raw_id": raw_id,
                        "status": status,
                        "reason": structured.get("rejection_reason", ""),
                    },
                )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("Processing queue update skipped for RAW_ID=%s: %s", raw_id, exc)


def ensure_silver_schema() -> None:
    table_name = f"{SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES"
    columns = {
        "INGREDIENTS": "VARIANT",
        "RECOVERED_TEXT": "STRING",
        "EVIDENCE_TEXT": "STRING",
        "EVIDENCE_QUALITY_SCORE": "FLOAT DEFAULT 0",
        "IS_RECIPE": "BOOLEAN DEFAULT TRUE",
        "RECIPE_STATUS": "STRING DEFAULT 'partial_recipe'",
        "HAS_INGREDIENT_LIST": "BOOLEAN DEFAULT FALSE",
        "HAS_INSTRUCTIONS": "BOOLEAN DEFAULT FALSE",
        "CAPTION_COMPLETENESS_SCORE": "FLOAT DEFAULT 0",
        "REJECTION_REASON": "STRING",
        "FINAL_RECIPE_TITLE": "STRING",
        "FINAL_RECIPE_TEXT": "STRING",
        "FINAL_RECIPE_JSON": "VARIANT",
        "MISSING_RECIPE_INFO": "VARIANT",
        "FINAL_RECIPE_CONFIDENCE": "FLOAT DEFAULT 0",
        "FINAL_RECIPE_LANGUAGE": "STRING",
    }
    with get_snowflake_connection(schema=SILVER_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DESC TABLE {table_name}")
            existing_columns = {str(row[0]).upper() for row in cursor.fetchall()}
            for column_name, column_type in columns.items():
                if column_name not in existing_columns:
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
        conn.commit()


def ensure_recovery_schema() -> None:
    table_name = f"{SILVER_SCHEMA}.RECIPE_CONTENT_RECOVERY"
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        RECOVERY_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
        RAW_ID NUMBER,
        URL_TIKTOK STRING,
        CONTENT_ID STRING,
        METHOD STRING,
        RECOVERED_TEXT STRING,
        TEXT_LENGTH NUMBER,
        LANGUAGE_HINT STRING,
        CONFIDENCE FLOAT,
        ENGINE STRING,
        STATUS STRING,
        ERROR_MESSAGE STRING,
        SOURCE_DETAILS VARIANT,
        RECORD_HASH STRING,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    with get_snowflake_connection(schema=SILVER_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        conn.commit()


def ensure_evidence_schema() -> None:
    table_name = f"{SILVER_SCHEMA}.SILVER_RECIPE_EVIDENCE"
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        EVIDENCE_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
        RAW_ID NUMBER,
        CONTENT_ID STRING,
        URL_TIKTOK STRING,
        SOURCE_TYPE STRING,
        SOURCE_NAME STRING,
        EVIDENCE_TEXT STRING,
        EVIDENCE_LENGTH NUMBER,
        EVIDENCE_QUALITY_SCORE FLOAT,
        EVIDENCE_QUALITY_CLASS STRING,
        IS_RECIPE_SIGNAL BOOLEAN,
        SOURCE_DETAILS VARIANT,
        RECORD_HASH STRING,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    with get_snowflake_connection(schema=SILVER_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
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
    parser.add_argument(
        "--only-recovered",
        action="store_true",
        help="Only re-enrich rows with new Silver evidence collected after the last Silver processing time.",
    )
    args = parser.parse_args()

    ensure_silver_schema()
    ensure_recovery_schema()
    ensure_evidence_schema()
    rows = fetch_bronze_rows(
        limit=args.limit,
        reprocess_all=args.reprocess_all,
        only_recovered=args.only_recovered,
    )
    if not rows:
        LOGGER.info("No Bronze records found for the selected mode.")
        return

    LOGGER.info(
        "Found %s Bronze rows to enrich. reprocess_all=%s only_recovered=%s",
        len(rows),
        args.reprocess_all,
        args.only_recovered,
    )

    with requests.Session() as session:
        for row in tqdm(rows, desc="Enriching Silver", unit="row"):
            try:
                enrichment = ask_openrouter(row, session=session)

                if args.dry_run:
                    LOGGER.info(
                        "DRY RUN | RAW_ID=%s | URL=%s | RESULT=%s",
                        row["RAW_ID"],
                        row["URL_TIKTOK"],
                        enrichment["structured"],
                    )
                    continue

                upsert_silver_row(row, enrichment)

            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Failed to enrich RAW_ID=%s: %s", row["RAW_ID"], exc)


if __name__ == "__main__":
    main()
