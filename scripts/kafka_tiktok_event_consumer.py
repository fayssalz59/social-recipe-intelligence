from __future__ import annotations

import argparse
import hashlib
import json
import os
from typing import Any

from kafka import KafkaConsumer

from scripts.common import configure_logging, get_snowflake_connection

LOGGER = configure_logging("kafka_tiktok_event_consumer")

DB_NAME = os.getenv("SNOWFLAKE_DB", "TIKTOK_PORTFOLIO_DB")
BRONZE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_BRONZE", "BRONZE")
BRONZE_TABLE = f"{DB_NAME}.{BRONZE_SCHEMA}.BRONZE_TIKTOK_RECIPES"

MERGE_BRONZE_SQL = f"""
MERGE INTO {BRONZE_TABLE} AS tgt
USING (
    SELECT
        %(platform)s AS PLATFORM,
        %(content_id)s AS CONTENT_ID,
        %(creator_username)s AS CREATOR_USERNAME,
        %(title)s AS TITLE,
        %(description)s AS DESCRIPTION,
        %(description_is_partial)s AS DESCRIPTION_IS_PARTIAL,
        %(url_tiktok)s AS URL_TIKTOK,
        %(source_file)s AS SOURCE_FILE,
        PARSE_JSON(%(raw_payload)s) AS RAW_PAYLOAD,
        %(record_hash)s AS RECORD_HASH
) AS src
ON tgt.RECORD_HASH = src.RECORD_HASH
WHEN MATCHED THEN UPDATE SET
    PLATFORM = src.PLATFORM,
    CONTENT_ID = src.CONTENT_ID,
    CREATOR_USERNAME = src.CREATOR_USERNAME,
    TITLE = src.TITLE,
    DESCRIPTION = src.DESCRIPTION,
    DESCRIPTION_IS_PARTIAL = src.DESCRIPTION_IS_PARTIAL,
    URL_TIKTOK = src.URL_TIKTOK,
    SOURCE_FILE = src.SOURCE_FILE,
    RAW_PAYLOAD = src.RAW_PAYLOAD
WHEN NOT MATCHED THEN INSERT (
    PLATFORM,
    CONTENT_ID,
    CREATOR_USERNAME,
    TITLE,
    DESCRIPTION,
    DESCRIPTION_IS_PARTIAL,
    URL_TIKTOK,
    SOURCE_FILE,
    RAW_PAYLOAD,
    RECORD_HASH
) VALUES (
    src.PLATFORM,
    src.CONTENT_ID,
    src.CREATOR_USERNAME,
    src.TITLE,
    src.DESCRIPTION,
    src.DESCRIPTION_IS_PARTIAL,
    src.URL_TIKTOK,
    src.SOURCE_FILE,
    src.RAW_PAYLOAD,
    src.RECORD_HASH
);
"""


def build_record_hash(content_id: str, url_tiktok: str) -> str:
    payload = f"{content_id.strip()}|{url_tiktok.strip()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    creator_username = str(event.get("creator_username", "")).strip()
    content_id = str(event.get("content_id", "") or event.get("video_id", "")).strip()
    title = str(event.get("title", "")).strip()
    description = str(event.get("description", "")).strip()
    url_tiktok = str(event.get("url_tiktok", "") or event.get("url", "")).strip()
    platform = str(event.get("platform", "tiktok")).strip() or "tiktok"
    description_is_partial = bool(event.get("description_is_partial", False))
    raw_payload = event.get("raw_payload", {}) or {}

    if not content_id or not url_tiktok:
        raise ValueError(f"Invalid event, missing content_id/video_id or url_tiktok: {event}")

    source_file = f"kafka_{platform}_{creator_username or 'unknown_creator'}"

    return {
        "platform": platform,
        "creator_username": creator_username,
        "content_id": content_id,
        "title": title,
        "description": description,
        "description_is_partial": description_is_partial,
        "url_tiktok": url_tiktok,
        "source_file": source_file,
        "raw_payload": raw_payload,
    }


def insert_into_bronze(payload: dict[str, Any]) -> None:
    bronze_payload = {
        "platform": payload["platform"],
        "content_id": payload["content_id"],
        "creator_username": payload["creator_username"],
        "title": payload["title"],
        "description": payload["description"],
        "description_is_partial": payload["description_is_partial"],
        "url_tiktok": payload["url_tiktok"],
        "source_file": payload["source_file"],
        "raw_payload": json.dumps(payload["raw_payload"], ensure_ascii=False),
        "record_hash": build_record_hash(
            payload["content_id"],
            payload["url_tiktok"],
        ),
    }

    with get_snowflake_connection(schema=BRONZE_SCHEMA) as conn:
        with conn.cursor() as cur:
            cur.execute(MERGE_BRONZE_SQL, bronze_payload)
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consume TikTok detection events from Kafka and load them directly to Bronze."
    )
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC_NEW_VIDEOS", "new_tiktok_video_detected"),
    )
    parser.add_argument("--group-id", default="tiktok-video-event-consumer")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=args.group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    LOGGER.info("Listening to Kafka topic=%s bootstrap=%s", args.topic, args.bootstrap_server)

    for message in consumer:
        try:
            event = normalize_event(message.value)
            insert_into_bronze(event)
            LOGGER.info(
                "Inserted TikTok event into Bronze creator=%s content_id=%s url=%s",
                event["creator_username"],
                event["content_id"],
                event["url_tiktok"],
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed processing TikTok Kafka event: %s", exc)


if __name__ == "__main__":
    main()