"""Recover missing recipe evidence from comments, pages, audio, and video frames."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

import requests

from scripts.common import configure_logging, get_snowflake_connection
from scripts.recipe_evidence_scoring import (
    classify_evidence_quality,
    compute_recipe_evidence_score,
    is_usable_ocr,
    normalize_evidence_text,
)
from scripts.tiktok_recipe_discovery import (
    fetch_web_caption,
    infer_language,
    is_recipe_caption,
    normalize_caption,
)

try:
    from TikTokApi import TikTokApi
except ModuleNotFoundError:  # pragma: no cover - optional in Airflow image
    TikTokApi = None

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:  # pragma: no cover - optional recovery dependency
    BeautifulSoup = None

try:
    from tqdm.auto import tqdm
except ModuleNotFoundError:  # pragma: no cover - tqdm is a convenience dependency
    def tqdm(iterable, **_: Any):  # type: ignore[no-redef]
        return iterable


LOGGER = configure_logging("recover_recipe_content")
BRONZE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_BRONZE", "BRONZE")
SILVER_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_SILVER", "SILVER")
GOLD_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_GOLD", "GOLD")
CONTROL_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_CONTROL", "CONTROL")

RECOVERY_METHODS = {
    "web_caption",
    "comments",
    "external_url",
    "audio_transcript",
    "ocr",
}

RECIPE_COMMENT_SIGNALS = [
    "recipe",
    "ingredients",
    "ingredient",
    "cups",
    "tbsp",
    "tsp",
    "grams",
    "recette",
    "ingredients",
    "receta",
    "ingredientes",
    "ricetta",
    "ingredienti",
    "receita",
    "ingredientes",
    "\u0645\u0643\u0648\u0646\u0627\u062a",
    "\u0648\u0635\u0641\u0629",
]

SOURCE_TYPE_BY_METHOD = {
    "web_caption": "web_metadata",
    "comments": "comments",
    "external_url": "external_url",
    "audio_transcript": "audio_transcript",
    "ocr": "video_ocr",
}

ARABIC_RE = re.compile(r"[\u0600-\u06ff]")

SUCCESS_STATUSES = {
    "attempted_success",
    "technical_success",
    "usable_recipe_text",
}


def contains_arabic(text: str) -> bool:
    return bool(ARABIC_RE.search(text or ""))


def row_contains_arabic(row: dict[str, Any]) -> bool:
    evidence = " ".join(
        str(row.get(name) or "")
        for name in ["ORIGINAL_DESCRIPTION", "RECOVERED_TEXT", "EVIDENCE_TEXT", "BRONZE_DESCRIPTION"]
    )
    return contains_arabic(evidence) or infer_language(evidence) == "ar"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recover richer recipe evidence for weak Silver records.")
    parser.add_argument(
        "--method",
        choices=["adaptive", "all", *sorted(RECOVERY_METHODS)],
        default="adaptive",
        help="'adaptive' runs only the next useful method when the current evidence score is too low.",
    )
    parser.add_argument("--limit", type=int, default=25, help="Maximum candidate records to process.")
    parser.add_argument("--min-score", type=float, default=0.65, help="Recover rows below this completeness score.")
    parser.add_argument("--target-score", type=float, default=0.70, help="Stop recovery for a row once this evidence score is reached.")
    parser.add_argument("--audio-threshold", type=float, default=0.70, help="Run speech-to-text when caption evidence is below this score.")
    parser.add_argument("--ocr-threshold", type=float, default=0.55, help="Run OCR when caption+audio evidence is still below this score.")
    parser.add_argument("--timeout", type=float, default=18.0, help="HTTP/browser timeout in seconds.")
    parser.add_argument("--comment-count", type=int, default=40, help="TikTok comments to inspect per video.")
    parser.add_argument("--frame-count", type=int, default=6, help="Frames to sample for OCR.")
    parser.add_argument("--whisper-model", default="tiny", help="faster-whisper model name for local ASR.")
    parser.add_argument("--ocr-engine", choices=["easyocr", "tesseract", "auto"], default="auto")
    parser.add_argument("--dry-run", action="store_true", help="Log recoveries without writing Snowflake.")
    parser.add_argument("--force-retry", action="store_true", help="Retry methods even if they failed recently.")
    parser.add_argument("--retry-failed-after-hours", type=int, default=24, help="Skip failed/empty methods retried within this window.")
    parser.add_argument(
        "--media-cache-dir",
        default=os.getenv("RECOVERY_MEDIA_CACHE_DIR", "/root/.cache/recipe-content-recovery/media"),
        help="Directory used to cache downloaded video/audio media by content id.",
    )
    parser.add_argument("--enable-comments", action="store_true", help="Try TikTok comments. Off by default because this source is often blocked.")
    parser.add_argument("--skip-audio", action="store_true", help="Disable automatic speech-to-text in adaptive mode.")
    parser.add_argument("--skip-ocr", action="store_true", help="Disable automatic OCR in adaptive mode.")
    parser.add_argument("--verbose-results", action="store_true", help="Log every recovery result instead of only failures and summary.")
    return parser.parse_args()


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


def ensure_processing_queue_schema() -> None:
    table_name = f"{CONTROL_SCHEMA}.RECIPE_PROCESSING_QUEUE"
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        RAW_ID NUMBER,
        CONTENT_ID STRING,
        URL_TIKTOK STRING,
        CREATOR_USERNAME STRING,
        STATUS STRING,
        PRIORITY NUMBER DEFAULT 5,
        LAST_ERROR STRING,
        DISCOVERED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        CONSTRAINT UQ_RECIPE_PROCESSING_QUEUE UNIQUE (RAW_ID)
    )
    """
    with get_snowflake_connection(schema=CONTROL_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        conn.commit()


def ensure_creator_quality_schema() -> None:
    table_name = f"{CONTROL_SCHEMA}.CREATOR_QUALITY_SCORE"
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        CREATOR_USERNAME STRING,
        VIDEOS_SCANNED NUMBER DEFAULT 0,
        VIDEOS_ACCEPTED NUMBER DEFAULT 0,
        RECIPES_EXTRACTED NUMBER DEFAULT 0,
        FULL_RECIPES NUMBER DEFAULT 0,
        AVG_QUALITY_SCORE FLOAT DEFAULT 0,
        YIELD_RATE FLOAT DEFAULT 0,
        LAST_SCANNED_AT TIMESTAMP_NTZ,
        STATUS STRING DEFAULT 'active',
        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        CONSTRAINT UQ_CREATOR_QUALITY_SCORE UNIQUE (CREATOR_USERNAME)
    )
    """
    with get_snowflake_connection(schema=CONTROL_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        conn.commit()


def fetch_candidates(limit: int, min_score: float) -> list[dict[str, Any]]:
    query = f"""
    WITH evidence AS (
        SELECT
            RAW_ID,
            MAX(EVIDENCE_QUALITY_SCORE) AS BEST_EVIDENCE_QUALITY_SCORE
        FROM {SILVER_SCHEMA}.SILVER_RECIPE_EVIDENCE
        GROUP BY RAW_ID
    )
    SELECT
        b.RAW_ID,
        b.URL_TIKTOK,
        b.CONTENT_ID,
        b.CREATOR_USERNAME,
        b.DESCRIPTION AS BRONZE_DESCRIPTION,
        COALESCE(s.ORIGINAL_DESCRIPTION, b.DESCRIPTION) AS ORIGINAL_DESCRIPTION,
        COALESCE(s.RECOVERED_TEXT, '') AS RECOVERED_TEXT,
        COALESCE(s.EVIDENCE_TEXT, b.DESCRIPTION) AS EVIDENCE_TEXT,
        COALESCE(s.CAPTION_COMPLETENESS_SCORE, 0) AS CAPTION_COMPLETENESS_SCORE,
        COALESCE(e.BEST_EVIDENCE_QUALITY_SCORE, 0) AS BEST_EVIDENCE_QUALITY_SCORE,
        COALESCE(s.RECIPE_STATUS, 'unknown') AS RECIPE_STATUS,
        COALESCE(s.IS_RECIPE, TRUE) AS IS_RECIPE
    FROM {BRONZE_SCHEMA}.BRONZE_TIKTOK_RECIPES b
    LEFT JOIN {SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES s
        ON b.RAW_ID = s.RAW_ID
    LEFT JOIN evidence e
        ON b.RAW_ID = e.RAW_ID
    WHERE b.URL_TIKTOK IS NOT NULL
      AND COALESCE(TRIM(b.DESCRIPTION), '') <> ''
      AND (
        s.RAW_ID IS NULL
        OR (
            GREATEST(
                COALESCE(s.CAPTION_COMPLETENESS_SCORE, 0),
                COALESCE(e.BEST_EVIDENCE_QUALITY_SCORE, 0)
            ) < %(min_score)s
            AND COALESCE(s.RECIPE_STATUS, 'unknown') IN ('partial_recipe', 'food_content', 'unknown')
        )
      )
    ORDER BY
        COALESCE(s.CAPTION_COMPLETENESS_SCORE, 0) ASC,
        b.INGESTED_AT ASC
    LIMIT %(limit)s
    """
    with get_snowflake_connection(schema=BRONZE_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, {"limit": limit, "min_score": min_score})
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def hash_recovery(raw_id: Any, method: str, text: str) -> str:
    payload = f"{raw_id}|{method}|{text.strip()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def make_result(
    row: dict[str, Any],
    method: str,
    status: str,
    text: str = "",
    confidence: float = 0.0,
    engine: str = "",
    error: str = "",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    text = normalize_evidence_text(normalize_caption(text))
    score = compute_recipe_evidence_score(
        text,
        creator=str(row.get("CREATOR_USERNAME") or ""),
    ) if text else 0.0
    if method == "ocr" and text and not is_usable_ocr(text):
        status = "rejected_gibberish"
        confidence = min(confidence, score)
    elif text and status == "attempted_success":
        status = "usable_recipe_text" if score >= 0.30 else "technical_success"
        confidence = max(confidence, score)
    return {
        "raw_id": row["RAW_ID"],
        "url_tiktok": row["URL_TIKTOK"],
        "content_id": row.get("CONTENT_ID") or "",
        "method": method,
        "recovered_text": text,
        "text_length": len(text),
        "language_hint": infer_language(text) if text else "",
        "confidence": confidence,
        "engine": engine,
        "status": status,
        "error_message": error[:1000],
        "source_details": json.dumps(
            {
                **(details or {}),
                "evidence_quality_score": score,
                "evidence_quality_class": classify_evidence_quality(score),
            }
        ),
        "record_hash": hash_recovery(row["RAW_ID"], method, text or error or status),
    }


MERGE_RECOVERY_SQL = f"""
MERGE INTO {SILVER_SCHEMA}.RECIPE_CONTENT_RECOVERY AS target
USING (
    SELECT
        %(raw_id)s AS RAW_ID,
        %(url_tiktok)s AS URL_TIKTOK,
        %(content_id)s AS CONTENT_ID,
        %(method)s AS METHOD,
        %(recovered_text)s AS RECOVERED_TEXT,
        %(text_length)s AS TEXT_LENGTH,
        %(language_hint)s AS LANGUAGE_HINT,
        %(confidence)s AS CONFIDENCE,
        %(engine)s AS ENGINE,
        %(status)s AS STATUS,
        %(error_message)s AS ERROR_MESSAGE,
        PARSE_JSON(%(source_details)s) AS SOURCE_DETAILS,
        %(record_hash)s AS RECORD_HASH
) AS source
ON target.RAW_ID = source.RAW_ID
   AND target.METHOD = source.METHOD
   AND target.RECORD_HASH = source.RECORD_HASH
WHEN MATCHED THEN UPDATE SET
    URL_TIKTOK = source.URL_TIKTOK,
    CONTENT_ID = source.CONTENT_ID,
    RECOVERED_TEXT = source.RECOVERED_TEXT,
    TEXT_LENGTH = source.TEXT_LENGTH,
    LANGUAGE_HINT = source.LANGUAGE_HINT,
    CONFIDENCE = source.CONFIDENCE,
    ENGINE = source.ENGINE,
    STATUS = source.STATUS,
    ERROR_MESSAGE = source.ERROR_MESSAGE,
    SOURCE_DETAILS = source.SOURCE_DETAILS
WHEN NOT MATCHED THEN INSERT (
    RAW_ID,
    URL_TIKTOK,
    CONTENT_ID,
    METHOD,
    RECOVERED_TEXT,
    TEXT_LENGTH,
    LANGUAGE_HINT,
    CONFIDENCE,
    ENGINE,
    STATUS,
    ERROR_MESSAGE,
    SOURCE_DETAILS,
    RECORD_HASH
) VALUES (
    source.RAW_ID,
    source.URL_TIKTOK,
    source.CONTENT_ID,
    source.METHOD,
    source.RECOVERED_TEXT,
    source.TEXT_LENGTH,
    source.LANGUAGE_HINT,
    source.CONFIDENCE,
    source.ENGINE,
    source.STATUS,
    source.ERROR_MESSAGE,
    source.SOURCE_DETAILS,
    source.RECORD_HASH
)
"""


def upsert_recovery(result: dict[str, Any]) -> None:
    with get_snowflake_connection(schema=SILVER_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(MERGE_RECOVERY_SQL, result)
        conn.commit()


MERGE_EVIDENCE_SQL = f"""
MERGE INTO {SILVER_SCHEMA}.SILVER_RECIPE_EVIDENCE AS target
USING (
    SELECT
        %(raw_id)s AS RAW_ID,
        %(content_id)s AS CONTENT_ID,
        %(url_tiktok)s AS URL_TIKTOK,
        %(source_type)s AS SOURCE_TYPE,
        %(source_name)s AS SOURCE_NAME,
        %(evidence_text)s AS EVIDENCE_TEXT,
        %(evidence_length)s AS EVIDENCE_LENGTH,
        %(evidence_quality_score)s AS EVIDENCE_QUALITY_SCORE,
        %(evidence_quality_class)s AS EVIDENCE_QUALITY_CLASS,
        %(is_recipe_signal)s AS IS_RECIPE_SIGNAL,
        PARSE_JSON(%(source_details)s) AS SOURCE_DETAILS,
        %(record_hash)s AS RECORD_HASH
) AS source
ON target.RAW_ID = source.RAW_ID
   AND target.SOURCE_TYPE = source.SOURCE_TYPE
   AND target.RECORD_HASH = source.RECORD_HASH
WHEN MATCHED THEN UPDATE SET
    CONTENT_ID = source.CONTENT_ID,
    URL_TIKTOK = source.URL_TIKTOK,
    SOURCE_NAME = source.SOURCE_NAME,
    EVIDENCE_TEXT = source.EVIDENCE_TEXT,
    EVIDENCE_LENGTH = source.EVIDENCE_LENGTH,
    EVIDENCE_QUALITY_SCORE = source.EVIDENCE_QUALITY_SCORE,
    EVIDENCE_QUALITY_CLASS = source.EVIDENCE_QUALITY_CLASS,
    IS_RECIPE_SIGNAL = source.IS_RECIPE_SIGNAL,
    SOURCE_DETAILS = source.SOURCE_DETAILS
WHEN NOT MATCHED THEN INSERT (
    RAW_ID,
    CONTENT_ID,
    URL_TIKTOK,
    SOURCE_TYPE,
    SOURCE_NAME,
    EVIDENCE_TEXT,
    EVIDENCE_LENGTH,
    EVIDENCE_QUALITY_SCORE,
    EVIDENCE_QUALITY_CLASS,
    IS_RECIPE_SIGNAL,
    SOURCE_DETAILS,
    RECORD_HASH
) VALUES (
    source.RAW_ID,
    source.CONTENT_ID,
    source.URL_TIKTOK,
    source.SOURCE_TYPE,
    source.SOURCE_NAME,
    source.EVIDENCE_TEXT,
    source.EVIDENCE_LENGTH,
    source.EVIDENCE_QUALITY_SCORE,
    source.EVIDENCE_QUALITY_CLASS,
    source.IS_RECIPE_SIGNAL,
    source.SOURCE_DETAILS,
    source.RECORD_HASH
)
"""


def evidence_payload_from_recovery(result: dict[str, Any]) -> dict[str, Any] | None:
    text = normalize_evidence_text(result.get("recovered_text") or "")
    if not text or result.get("status") not in SUCCESS_STATUSES:
        return None
    if result.get("method") == "ocr" and not is_usable_ocr(text):
        return None

    source_details = json.loads(result.get("source_details") or "{}")
    score = compute_recipe_evidence_score(text)
    if score < 0.20:
        return None

    return {
        "raw_id": result["raw_id"],
        "content_id": result.get("content_id") or "",
        "url_tiktok": result["url_tiktok"],
        "source_type": SOURCE_TYPE_BY_METHOD.get(result["method"], result["method"]),
        "source_name": result.get("engine") or result["method"],
        "evidence_text": text,
        "evidence_length": len(text),
        "evidence_quality_score": score,
        "evidence_quality_class": classify_evidence_quality(score),
        "is_recipe_signal": score >= 0.30,
        "source_details": json.dumps(source_details),
        "record_hash": hash_recovery(result["raw_id"], result["method"], text),
    }


def result_quality_score(result: dict[str, Any]) -> float:
    try:
        source_details = json.loads(result.get("source_details") or "{}")
        return float(source_details.get("evidence_quality_score") or 0)
    except (TypeError, ValueError, json.JSONDecodeError):
        return 0.0


def fetch_recent_attempts(raw_id: Any, retry_hours: int) -> dict[str, str]:
    query = f"""
    SELECT METHOD, STATUS
    FROM {SILVER_SCHEMA}.RECIPE_CONTENT_RECOVERY
    WHERE RAW_ID = %(raw_id)s
      AND CREATED_AT >= DATEADD(hour, -%(retry_hours)s, CURRENT_TIMESTAMP())
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY METHOD
        ORDER BY CREATED_AT DESC
    ) = 1
    """
    with get_snowflake_connection(schema=SILVER_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, {"raw_id": raw_id, "retry_hours": retry_hours})
            return {str(method): str(status) for method, status in cursor.fetchall()}


def should_skip_recent_attempt(method: str, recent_attempts: dict[str, str], args: argparse.Namespace) -> bool:
    if args.force_retry:
        return False
    status = recent_attempts.get(method)
    if not status:
        return False
    return status in {"attempted_failed", "attempted_empty", "rejected_gibberish", "technical_success"}


def upsert_evidence(result: dict[str, Any]) -> None:
    payload = evidence_payload_from_recovery(result)
    if payload is None:
        return
    with get_snowflake_connection(schema=SILVER_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(MERGE_EVIDENCE_SQL, payload)
        conn.commit()


MERGE_QUEUE_SQL = f"""
MERGE INTO {CONTROL_SCHEMA}.RECIPE_PROCESSING_QUEUE AS target
USING (
    SELECT
        %(raw_id)s AS RAW_ID,
        %(content_id)s AS CONTENT_ID,
        %(url_tiktok)s AS URL_TIKTOK,
        %(creator_username)s AS CREATOR_USERNAME,
        %(status)s AS STATUS,
        %(priority)s AS PRIORITY,
        %(last_error)s AS LAST_ERROR
) AS source
ON target.RAW_ID = source.RAW_ID
WHEN MATCHED THEN UPDATE SET
    CONTENT_ID = source.CONTENT_ID,
    URL_TIKTOK = source.URL_TIKTOK,
    CREATOR_USERNAME = source.CREATOR_USERNAME,
    STATUS = source.STATUS,
    PRIORITY = source.PRIORITY,
    LAST_ERROR = source.LAST_ERROR,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    RAW_ID,
    CONTENT_ID,
    URL_TIKTOK,
    CREATOR_USERNAME,
    STATUS,
    PRIORITY,
    LAST_ERROR
) VALUES (
    source.RAW_ID,
    source.CONTENT_ID,
    source.URL_TIKTOK,
    source.CREATOR_USERNAME,
    source.STATUS,
    source.PRIORITY,
    source.LAST_ERROR
)
"""


def queue_status_from_result(result: dict[str, Any]) -> tuple[str, int]:
    if evidence_payload_from_recovery(result) is not None:
        if float(result.get("confidence") or 0) >= 0.45:
            return "ready_for_llm_extraction", 2
        return "ready_for_llm_classification", 3
    if result["status"] in {"rejected_gibberish", "technical_success"}:
        return "low_quality_rejected", 8
    if result["method"] == "ocr" and result["status"] in {"attempted_empty", "attempted_failed"}:
        return "needs_audio", 6
    if result["method"] in {"web_caption", "external_url", "comments"} and result["status"] in {"attempted_empty", "attempted_failed"}:
        return "needs_ocr", 5
    return "failed", 9


def upsert_processing_queue(row: dict[str, Any], result: dict[str, Any]) -> None:
    status, priority = queue_status_from_result(result)
    payload = {
        "raw_id": result["raw_id"],
        "content_id": result.get("content_id") or row.get("CONTENT_ID") or "",
        "url_tiktok": result["url_tiktok"],
        "creator_username": row.get("CREATOR_USERNAME") or "",
        "status": status,
        "priority": priority,
        "last_error": result.get("error_message", ""),
    }
    with get_snowflake_connection(schema=CONTROL_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(MERGE_QUEUE_SQL, payload)
        conn.commit()


def refresh_creator_quality_scores() -> None:
    merge_sql = f"""
    MERGE INTO {CONTROL_SCHEMA}.CREATOR_QUALITY_SCORE AS target
    USING (
        WITH bronze AS (
            SELECT
                LOWER(CREATOR_USERNAME) AS CREATOR_USERNAME,
                COUNT(*) AS VIDEOS_SCANNED,
                MAX(INGESTED_AT) AS LAST_SCANNED_AT
            FROM {BRONZE_SCHEMA}.BRONZE_TIKTOK_RECIPES
            WHERE COALESCE(CREATOR_USERNAME, '') <> ''
            GROUP BY LOWER(CREATOR_USERNAME)
        ),
        silver AS (
            SELECT
                LOWER(b.CREATOR_USERNAME) AS CREATOR_USERNAME,
                COUNT_IF(s.IS_RECIPE) AS RECIPES_EXTRACTED,
                COUNT_IF(s.RECIPE_STATUS = 'full_recipe') AS FULL_RECIPES,
                AVG(
                    GREATEST(
                        COALESCE(s.FINAL_RECIPE_CONFIDENCE, 0),
                        COALESCE(s.CAPTION_COMPLETENESS_SCORE, 0),
                        COALESCE(s.PROCESSING_CONFIDENCE, 0)
                    )
                ) AS AVG_QUALITY_SCORE
            FROM {BRONZE_SCHEMA}.BRONZE_TIKTOK_RECIPES b
            LEFT JOIN {SILVER_SCHEMA}.SILVER_TIKTOK_RECIPES s
                ON b.RAW_ID = s.RAW_ID
            WHERE COALESCE(b.CREATOR_USERNAME, '') <> ''
            GROUP BY LOWER(b.CREATOR_USERNAME)
        )
        SELECT
            bronze.CREATOR_USERNAME,
            bronze.VIDEOS_SCANNED,
            COALESCE(silver.RECIPES_EXTRACTED, 0) AS VIDEOS_ACCEPTED,
            COALESCE(silver.RECIPES_EXTRACTED, 0) AS RECIPES_EXTRACTED,
            COALESCE(silver.FULL_RECIPES, 0) AS FULL_RECIPES,
            COALESCE(silver.AVG_QUALITY_SCORE, 0) AS AVG_QUALITY_SCORE,
            COALESCE(silver.FULL_RECIPES, 0) / NULLIF(bronze.VIDEOS_SCANNED, 0) AS YIELD_RATE,
            bronze.LAST_SCANNED_AT,
            IFF(COALESCE(silver.FULL_RECIPES, 0) / NULLIF(bronze.VIDEOS_SCANNED, 0) >= 0.25, 'high_yield', 'active') AS STATUS
        FROM bronze
        LEFT JOIN silver
            ON bronze.CREATOR_USERNAME = silver.CREATOR_USERNAME
    ) AS source
    ON target.CREATOR_USERNAME = source.CREATOR_USERNAME
    WHEN MATCHED THEN UPDATE SET
        VIDEOS_SCANNED = source.VIDEOS_SCANNED,
        VIDEOS_ACCEPTED = source.VIDEOS_ACCEPTED,
        RECIPES_EXTRACTED = source.RECIPES_EXTRACTED,
        FULL_RECIPES = source.FULL_RECIPES,
        AVG_QUALITY_SCORE = source.AVG_QUALITY_SCORE,
        YIELD_RATE = source.YIELD_RATE,
        LAST_SCANNED_AT = source.LAST_SCANNED_AT,
        STATUS = source.STATUS,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        CREATOR_USERNAME,
        VIDEOS_SCANNED,
        VIDEOS_ACCEPTED,
        RECIPES_EXTRACTED,
        FULL_RECIPES,
        AVG_QUALITY_SCORE,
        YIELD_RATE,
        LAST_SCANNED_AT,
        STATUS
    ) VALUES (
        source.CREATOR_USERNAME,
        source.VIDEOS_SCANNED,
        source.VIDEOS_ACCEPTED,
        source.RECIPES_EXTRACTED,
        source.FULL_RECIPES,
        source.AVG_QUALITY_SCORE,
        source.YIELD_RATE,
        source.LAST_SCANNED_AT,
        source.STATUS
    )
    """
    try:
        with get_snowflake_connection(schema=CONTROL_SCHEMA) as conn:
            with conn.cursor() as cursor:
                cursor.execute(merge_sql)
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Creator quality score refresh skipped: %s", exc)


def recover_web_caption(row: dict[str, Any], timeout: float) -> dict[str, Any]:
    existing = str(row.get("EVIDENCE_TEXT") or row.get("BRONZE_DESCRIPTION") or "")
    recovered = fetch_web_caption(
        row["URL_TIKTOK"],
        str(row.get("CONTENT_ID") or ""),
        existing,
        timeout,
    )
    if not recovered:
        return make_result(row, "web_caption", "attempted_empty", engine="tiktok_web_metadata")
    text, source = recovered
    return make_result(
        row,
        "web_caption",
        "attempted_success",
        text=text,
        confidence=0.65 if is_recipe_caption(text) else 0.45,
        engine=source,
        details={"source": source},
    )


def extract_urls(text: str) -> list[str]:
    urls = re.findall(r"https?://[^\s)>\]]+", text or "")
    output: list[str] = []
    for url in urls:
        clean = url.rstrip(".,;:!?")
        if "tiktok.com" in clean.lower():
            continue
        if clean not in output:
            output.append(clean)
    return output


def extract_page_text(html_text: str) -> str:
    if BeautifulSoup is None:
        paragraphs = re.findall(r"<(?:p|li|h1|h2|h3)[^>]*>(.*?)</(?:p|li|h1|h2|h3)>", html_text, flags=re.I | re.S)
        return normalize_caption(" ".join(re.sub(r"<[^>]+>", " ", part) for part in paragraphs))
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    chunks = [node.get_text(" ", strip=True) for node in soup.find_all(["h1", "h2", "h3", "p", "li"])]
    return normalize_caption("\n".join(chunk for chunk in chunks if chunk))


def recover_external_url(row: dict[str, Any], timeout: float) -> dict[str, Any]:
    evidence = " ".join(
        str(row.get(name) or "")
        for name in ["ORIGINAL_DESCRIPTION", "RECOVERED_TEXT", "EVIDENCE_TEXT", "BRONZE_DESCRIPTION"]
    )
    urls = extract_urls(evidence)
    if not urls:
        return make_result(row, "external_url", "attempted_empty", engine="url_extractor")

    headers = {
        "User-Agent": os.getenv(
            "RECOVERY_HTTP_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
    }
    best_text = ""
    best_url = ""
    for url in urls[:3]:
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if not response.ok or "text/html" not in response.headers.get("content-type", ""):
                continue
            text = extract_page_text(response.text)
            if len(text) > len(best_text):
                best_text = text
                best_url = url
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("External URL recovery failed url=%s error=%s", url, exc)

    if not best_text:
        return make_result(row, "external_url", "attempted_empty", engine="html_text_extractor", details={"urls": urls})
    return make_result(
        row,
        "external_url",
        "attempted_success",
        text=best_text[:8000],
        confidence=0.75 if is_recipe_caption(best_text) else 0.45,
        engine="html_text_extractor",
        details={"url": best_url, "candidate_urls": urls},
    )


async def recover_comments(row: dict[str, Any], comment_count: int, timeout: float) -> dict[str, Any]:
    if TikTokApi is None:
        return make_result(row, "comments", "attempted_failed", engine="TikTokApi", error="TikTokApi is not installed")

    ms_token = os.getenv("TIKTOK_MS_TOKEN") or os.getenv("ms_token")
    ms_tokens = [ms_token] if ms_token else None
    browser = os.getenv("TIKTOK_BROWSER", "chromium")
    comments: list[str] = []
    try:
        async with TikTokApi() as api:
            await asyncio.wait_for(
                api.create_sessions(
                    ms_tokens=ms_tokens,
                    num_sessions=1,
                    sleep_after=5,
                    browser=browser,
                    headless=False,
                ),
                timeout=timeout,
            )
            async for comment in api.video(url=row["URL_TIKTOK"]).comments(count=comment_count):
                data = getattr(comment, "as_dict", {}) or {}
                text = normalize_caption(
                    data.get("text")
                    or data.get("comment")
                    or data.get("share_info", {}).get("desc")
                    or ""
                )
                if text:
                    comments.append(text)
    except Exception as exc:  # noqa: BLE001
        return make_result(row, "comments", "attempted_failed", engine="TikTokApi", error=str(exc))

    if not comments:
        return make_result(row, "comments", "attempted_empty", engine="TikTokApi")

    comments.sort(
        key=lambda text: (
            any(signal in text.lower() for signal in RECIPE_COMMENT_SIGNALS),
            len(text),
        ),
        reverse=True,
    )
    selected = comments[:8]
    text = "\n".join(f"- {comment}" for comment in selected)
    return make_result(
        row,
        "comments",
        "attempted_success",
        text=text,
        confidence=0.65 if any(signal in text.lower() for signal in RECIPE_COMMENT_SIGNALS) else 0.35,
        engine="TikTokApi.comments",
        details={"comments_scanned": len(comments), "comments_used": len(selected)},
    )


def require_command(command: str) -> None:
    if shutil.which(command) is None:
        raise RuntimeError(f"Required command is missing: {command}")


def media_cache_key(row: dict[str, Any]) -> str:
    raw_key = str(row.get("CONTENT_ID") or row.get("URL_TIKTOK") or row.get("RAW_ID"))
    safe_key = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw_key).strip("_")
    if safe_key:
        return safe_key[:120]
    return hashlib.sha256(str(row["URL_TIKTOK"]).encode("utf-8")).hexdigest()[:32]


def find_cached_media(cache_dir: Path, cache_key: str, audio_only: bool) -> Path | None:
    suffix = "audio" if audio_only else "video"
    matches = list(cache_dir.glob(f"{cache_key}.{suffix}.*"))
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_size)


def download_media(row: dict[str, Any], output_template: Path, audio_only: bool, cache_dir: str = "") -> Path:
    require_command("yt-dlp")
    cache_path = Path(cache_dir).expanduser() if cache_dir else None
    cache_key = media_cache_key(row)
    if cache_path:
        cache_path.mkdir(parents=True, exist_ok=True)
        cached = find_cached_media(cache_path, cache_key, audio_only)
        if cached and cached.exists() and cached.stat().st_size > 0:
            return cached

    command = [
        "yt-dlp",
        "--no-playlist",
        "--quiet",
        "--no-warnings",
        "-o",
        str(output_template),
    ]
    if audio_only:
        command.extend(["-x", "--audio-format", "wav"])
    command.append(row["URL_TIKTOK"])
    subprocess.run(command, check=True, capture_output=True, text=True)
    matches = list(output_template.parent.glob(output_template.name.replace("%(ext)s", "*")))
    if not matches:
        raise RuntimeError("yt-dlp completed but no media file was produced")
    media_path = max(matches, key=lambda path: path.stat().st_size)
    if cache_path:
        cached_path = cache_path / f"{cache_key}.{'audio' if audio_only else 'video'}{media_path.suffix}"
        shutil.copy2(media_path, cached_path)
        return cached_path
    return media_path


def recover_audio_transcript(row: dict[str, Any], model_name: str, cache_dir: str = "") -> dict[str, Any]:
    try:
        from faster_whisper import WhisperModel
    except ModuleNotFoundError:
        return make_result(
            row,
            "audio_transcript",
            "attempted_failed",
            engine="faster-whisper",
            error="faster-whisper is not installed",
        )

    with tempfile.TemporaryDirectory(prefix="recipe_audio_") as tmp:
        tmp_path = Path(tmp)
        try:
            audio_path = download_media(row, tmp_path / "audio.%(ext)s", audio_only=True, cache_dir=cache_dir)
            model = WhisperModel(model_name, device="cpu", compute_type="int8")
            segments, info = model.transcribe(str(audio_path), beam_size=1)
            transcript = normalize_caption(" ".join(segment.text.strip() for segment in segments))
            if not transcript:
                return make_result(row, "audio_transcript", "attempted_empty", engine=f"faster-whisper-{model_name}")
            confidence = max(0.2, min(0.9, 1.0 - float(getattr(info, "language_probability", 0) or 0)))
            confidence = float(getattr(info, "language_probability", confidence) or confidence)
            return make_result(
                row,
                "audio_transcript",
                "attempted_success",
                text=transcript[:10000],
                confidence=confidence,
                engine=f"faster-whisper-{model_name}",
                details={"language": getattr(info, "language", None), "duration": getattr(info, "duration", None)},
            )
        except Exception as exc:  # noqa: BLE001
            return make_result(row, "audio_transcript", "attempted_failed", engine=f"faster-whisper-{model_name}", error=str(exc))


def sample_video_frames(video_path: Path, output_dir: Path, frame_count: int) -> list[Path]:
    require_command("ffmpeg")
    output_dir.mkdir(parents=True, exist_ok=True)
    pattern = output_dir / "frame_%03d.jpg"
    fps = max(0.05, frame_count / 60)
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(video_path),
        "-vf",
        f"fps={fps},scale=960:-1",
        "-frames:v",
        str(frame_count),
        str(pattern),
    ]
    subprocess.run(command, check=True, capture_output=True, text=True)
    return sorted(output_dir.glob("frame_*.jpg"))


def easyocr_language_profiles(row: dict[str, Any]) -> list[list[str]]:
    latin_profile = ["en", "fr", "es", "it", "pt"]
    arabic_profile = ["ar", "en"]
    if row_contains_arabic(row):
        return [arabic_profile, latin_profile]
    return [latin_profile]


def run_easyocr(frames: list[Path], row: dict[str, Any]) -> tuple[str, str]:
    import easyocr

    errors: list[str] = []
    for languages in easyocr_language_profiles(row):
        try:
            reader = easyocr.Reader(languages, gpu=False)
            texts: list[str] = []
            for frame in frames:
                results = reader.readtext(str(frame), detail=1, paragraph=True)
                for result in results:
                    text = result[1] if isinstance(result, (list, tuple)) and len(result) > 1 else ""
                    if text:
                        texts.append(normalize_caption(text))
            return "\n".join(dict.fromkeys(texts)), f"easyocr:{','.join(languages)}"
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{','.join(languages)}={exc}")
            continue
    raise RuntimeError("; ".join(errors))


def run_tesseract(frames: list[Path]) -> tuple[str, str]:
    import pytesseract
    from PIL import Image

    texts: list[str] = []
    for frame in frames:
        text = pytesseract.image_to_string(Image.open(frame))
        if text.strip():
            texts.append(normalize_caption(text))
    return "\n".join(dict.fromkeys(texts)), "pytesseract"


def recover_ocr(row: dict[str, Any], frame_count: int, engine: str, cache_dir: str = "") -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="recipe_ocr_") as tmp:
        tmp_path = Path(tmp)
        try:
            video_path = download_media(row, tmp_path / "video.%(ext)s", audio_only=False, cache_dir=cache_dir)
            frames = sample_video_frames(video_path, tmp_path / "frames", frame_count)
            if not frames:
                return make_result(row, "ocr", "attempted_empty", engine="ffmpeg")

            if engine in {"auto", "easyocr"}:
                try:
                    text, used_engine = run_easyocr(frames, row)
                except Exception as exc:  # noqa: BLE001
                    if engine == "easyocr":
                        raise
                    LOGGER.info("EasyOCR unavailable/failed, falling back to pytesseract: %s", exc)
                    text, used_engine = run_tesseract(frames)
            else:
                text, used_engine = run_tesseract(frames)

            if not text:
                return make_result(row, "ocr", "attempted_empty", engine=used_engine)
            return make_result(
                row,
                "ocr",
                "attempted_success",
                text=text[:10000],
                confidence=0.6 if is_recipe_caption(text) else 0.35,
                engine=used_engine,
                details={"frames_sampled": len(frames)},
            )
        except Exception as exc:  # noqa: BLE001
            return make_result(row, "ocr", "attempted_failed", engine=engine, error=str(exc))


async def recover_one(row: dict[str, Any], method: str, args: argparse.Namespace) -> dict[str, Any]:
    if method == "web_caption":
        return recover_web_caption(row, args.timeout)
    if method == "external_url":
        return recover_external_url(row, args.timeout)
    if method == "comments":
        return await recover_comments(row, args.comment_count, args.timeout)
    if method == "audio_transcript":
        return await asyncio.to_thread(recover_audio_transcript, row, args.whisper_model, args.media_cache_dir)
    if method == "ocr":
        return await asyncio.to_thread(recover_ocr, row, args.frame_count, args.ocr_engine, args.media_cache_dir)
    raise ValueError(f"Unsupported recovery method: {method}")


def has_external_url_candidate(row: dict[str, Any]) -> bool:
    evidence = " ".join(
        str(row.get(name) or "")
        for name in ["ORIGINAL_DESCRIPTION", "RECOVERED_TEXT", "EVIDENCE_TEXT", "BRONZE_DESCRIPTION"]
    )
    return bool(extract_urls(evidence))


def methods_to_run(args: argparse.Namespace, row: dict[str, Any] | None = None) -> list[str]:
    if args.method not in {"all", "adaptive"}:
        return [args.method]

    methods = ["web_caption"]
    if not args.skip_audio:
        methods.append("audio_transcript")
    if not args.skip_ocr:
        methods.append("ocr")
    if row is None or has_external_url_candidate(row):
        methods.append("external_url")
    if args.enable_comments:
        methods.append("comments")
    return methods


def should_run_next_method(method: str, current_score: float, row: dict[str, Any], args: argparse.Namespace) -> bool:
    if method == "web_caption":
        return True
    if method == "audio_transcript":
        return current_score < args.audio_threshold
    if method == "ocr":
        return current_score < args.ocr_threshold
    if method == "external_url":
        return current_score < args.target_score and has_external_url_candidate(row)
    if method == "comments":
        return args.enable_comments and current_score < args.ocr_threshold
    return True


def write_result(row: dict[str, Any], result: dict[str, Any], args: argparse.Namespace) -> bool:
    wrote_evidence = evidence_payload_from_recovery(result) is not None

    if args.verbose_results or result["status"] in {"attempted_failed", "rejected_gibberish"}:
        LOGGER.info(
            "Recovery raw_id=%s method=%s status=%s length=%s confidence=%.2f score=%.2f",
            result["raw_id"],
            result["method"],
            result["status"],
            result["text_length"],
            result["confidence"],
            result_quality_score(result),
        )

    if args.dry_run:
        if result["recovered_text"] and args.verbose_results:
            LOGGER.info("DRY RUN recovered text preview: %s", result["recovered_text"][:500])
        return wrote_evidence

    upsert_recovery(result)
    upsert_evidence(result)
    upsert_processing_queue(row, result)
    return wrote_evidence


async def recover_row_adaptive(row: dict[str, Any], args: argparse.Namespace) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    best_score = float(row.get("BEST_EVIDENCE_QUALITY_SCORE") or 0)
    recent_attempts = fetch_recent_attempts(row["RAW_ID"], args.retry_failed_after_hours)

    for method in methods_to_run(args, row):
        if best_score >= args.target_score:
            break
        if should_skip_recent_attempt(method, recent_attempts, args):
            continue
        if not should_run_next_method(method, best_score, row, args):
            continue

        result = await recover_one(row, method, args)
        results.append(result)
        write_result(row, result, args)
        best_score = max(best_score, result_quality_score(result))

    return results


async def run_recovery(args: argparse.Namespace) -> None:
    ensure_recovery_schema()
    ensure_evidence_schema()
    ensure_processing_queue_schema()
    ensure_creator_quality_schema()
    candidates = fetch_candidates(args.limit, args.min_score)
    LOGGER.info("Found %s candidate rows for recovery.", len(candidates))
    if not candidates:
        return

    summary: Counter[str] = Counter()
    evidence_rows = 0

    for row in tqdm(candidates, desc="Recovering recipe evidence", unit="video"):
        if args.method in {"adaptive", "all"}:
            results = await recover_row_adaptive(row, args)
        else:
            recent_attempts = fetch_recent_attempts(row["RAW_ID"], args.retry_failed_after_hours)
            if should_skip_recent_attempt(args.method, recent_attempts, args):
                results = []
            else:
                result = await recover_one(row, args.method, args)
                write_result(row, result, args)
                results = [result]

        for result in results:
            summary[f"{result['method']}:{result['status']}"] += 1
            if evidence_payload_from_recovery(result) is not None:
                evidence_rows += 1

    LOGGER.info("Recovery summary:")
    for key, value in summary.most_common():
        LOGGER.info("  %s = %s", key, value)
    LOGGER.info("  evidence_rows_written_or_detected = %s", evidence_rows)
    refresh_creator_quality_scores()


def main() -> None:
    args = parse_args()
    asyncio.run(run_recovery(args))


if __name__ == "__main__":
    main()
