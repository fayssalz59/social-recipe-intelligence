from __future__ import annotations

import argparse
import hashlib
import json
import os
from typing import Any

from kafka import KafkaConsumer

from scripts.common import configure_logging, get_snowflake_connection

LOGGER = configure_logging("kafka_consumer")
BRONZE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_BRONZE", "BRONZE")
DB_NAME = os.getenv("SNOWFLAKE_DB", "TIKTOK_PORTFOLIO_DB")

MERGE_SQL = f"""
MERGE INTO {DB_NAME}.{BRONZE_SCHEMA}.BRONZE_TIKTOK_RECIPES AS tgt
USING (
    SELECT
        %(title)s AS TITLE,
        %(description)s AS DESCRIPTION,
        %(url_tiktok)s AS URL_TIKTOK,
        %(source_file)s AS SOURCE_FILE,
        %(record_hash)s AS RECORD_HASH
) AS src
ON tgt.RECORD_HASH = src.RECORD_HASH
WHEN NOT MATCHED THEN INSERT (
    TITLE,
    DESCRIPTION,
    URL_TIKTOK,
    SOURCE_FILE,
    RECORD_HASH
) VALUES (
    src.TITLE,
    src.DESCRIPTION,
    src.URL_TIKTOK,
    src.SOURCE_FILE,
    src.RECORD_HASH
);
"""


def build_record_hash(title: str, description: str, url_tiktok: str) -> str:
    payload = f"{title.strip()}|{description.strip()}|{url_tiktok.strip()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    title = str(event.get("TITLE", "")).strip()
    description = str(event.get("DESCRIPTION", "")).strip()
    url_tiktok = str(event.get("URL_TIKTOK", "")).strip()
    source_file = str(event.get("SOURCE_FILE", "kafka_event")).strip() or "kafka_event"

    if not title or not url_tiktok:
        raise ValueError(f"Invalid event, TITLE and URL_TIKTOK required: {event}")

    return {
        "title": title,
        "description": description,
        "url_tiktok": url_tiktok,
        "source_file": source_file,
        "record_hash": build_record_hash(title, description, url_tiktok),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume recipe events from Kafka and load to Bronze.")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="recipes_raw")
    parser.add_argument("--group-id", default="recipe-platform-consumer")
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

    with get_snowflake_connection(schema=BRONZE_SCHEMA) as conn:
        with conn.cursor() as cursor:
            for message in consumer:
                try:
                    payload = normalize_event(message.value)
                    cursor.execute(MERGE_SQL, payload)
                    conn.commit()
                    LOGGER.info("Inserted/merged Kafka event url=%s", payload["url_tiktok"])
                except Exception as exc:  # noqa: BLE001
                    LOGGER.exception("Failed to process Kafka message: %s", exc)


if __name__ == "__main__":
    main()