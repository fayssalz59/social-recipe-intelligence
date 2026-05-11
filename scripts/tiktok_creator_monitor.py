from __future__ import annotations

import asyncio
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer

from scripts.common import configure_logging, get_snowflake_connection
from scripts.content_source_types import SourceContentItem
from scripts.tiktok_client import TikTokClient

LOGGER = configure_logging("tiktok_creator_monitor")

DB_NAME = os.getenv("SNOWFLAKE_DB", "TIKTOK_PORTFOLIO_DB")
CONTROL_SCHEMA = "CONTROL"
SEEN_TABLE = f"{DB_NAME}.{CONTROL_SCHEMA}.SEEN_TIKTOK_VIDEOS"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_NEW_VIDEOS", "new_tiktok_video_detected")

CREATORS_CONFIG = Path("config/creators.json")


def load_creators() -> list[str]:
    with CREATORS_CONFIG.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("creators", [])


def content_exists(platform: str, content_id: str) -> bool:
    query = f"""
    SELECT 1
    FROM {SEEN_TABLE}
    WHERE VIDEO_ID = %(content_id)s
    LIMIT 1
    """
    with get_snowflake_connection(schema=CONTROL_SCHEMA) as conn:
        with conn.cursor() as cur:
            cur.execute(query, {"content_id": content_id})
            return cur.fetchone() is not None


def mark_content_seen(item: SourceContentItem) -> None:
    merge_sql = f"""
    MERGE INTO {SEEN_TABLE} AS tgt
    USING (
        SELECT
            %(video_id)s AS VIDEO_ID,
            %(creator_username)s AS CREATOR_USERNAME,
            %(url_tiktok)s AS URL_TIKTOK
    ) AS src
    ON tgt.VIDEO_ID = src.VIDEO_ID
    WHEN MATCHED THEN UPDATE SET
        LAST_SEEN_AT = CURRENT_TIMESTAMP(),
        CREATOR_USERNAME = src.CREATOR_USERNAME,
        URL_TIKTOK = src.URL_TIKTOK
    WHEN NOT MATCHED THEN INSERT (
        VIDEO_ID,
        CREATOR_USERNAME,
        URL_TIKTOK
    ) VALUES (
        src.VIDEO_ID,
        src.CREATOR_USERNAME,
        src.URL_TIKTOK
    );
    """
    with get_snowflake_connection(schema=CONTROL_SCHEMA) as conn:
        with conn.cursor() as cur:
            cur.execute(
                merge_sql,
                {
                    "video_id": item.content_id,
                    "creator_username": item.creator_username,
                    "url_tiktok": item.url,
                },
            )
        conn.commit()


def build_event(item: SourceContentItem) -> dict:
    return {
        "event_type": "new_tiktok_video_detected",
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "source_system": "tiktok_creator_monitor",
        "platform": item.platform,
        "creator_username": item.creator_username,
        "video_id": item.content_id,
        "content_id": item.content_id,
        "published_at": item.published_at,
        "title": item.title,
        "description": item.description,
        "url_tiktok": item.url,
        "url": item.url,
        "language_hint": item.language_hint,
        "description_is_partial": item.description_is_partial,
        "raw_payload": item.raw_payload,
    }


async def async_main() -> None:
    creators = load_creators()
    if not creators:
        LOGGER.info("No creators configured.")
        return

    startup_sleep = random.randint(5, 10)
    LOGGER.info("Sleeping %s seconds before monitor run.", startup_sleep)
    time.sleep(startup_sleep)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    total_new = 0

    async with TikTokClient() as client:
        for creator in creators:
            LOGGER.info("Checking creator=%s", creator)
            try:
                items = await client.fetch_recent_videos_for_creator(creator, count=5)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Failed fetching creator=%s: %s", creator, exc)
                continue

            for item in items:
                if content_exists(item.platform, item.content_id):
                    continue

                event = build_event(item)
                producer.send(KAFKA_TOPIC, event)
                producer.flush()

                mark_content_seen(item)
                total_new += 1
                LOGGER.info(
                    "Published new content event platform=%s creator=%s content_id=%s",
                    item.platform,
                    item.creator_username,
                    item.content_id,
                )

    LOGGER.info("Monitor completed. new_events=%s", total_new)


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()