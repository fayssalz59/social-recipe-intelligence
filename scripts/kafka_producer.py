"""Kafka producer to simulate recipe events."""
from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Iterable

from kafka import KafkaProducer

SAMPLE_EVENTS = [
    {
        "TITLE": "Creamy mushroom pasta",
        "DESCRIPTION": "Easy vegetarian creamy mushroom pasta with garlic and parmesan.",
        "URL_TIKTOK": "https://www.tiktok.com/@demo/video/1",
        "SOURCE_FILE": "kafka_sample",
    },
    {
        "TITLE": "Chicken shawarma bowl",
        "DESCRIPTION": "Middle Eastern chicken bowl with rice, cucumber and garlic sauce.",
        "URL_TIKTOK": "https://www.tiktok.com/@demo/video/2",
        "SOURCE_FILE": "kafka_sample",
    },
]


def load_events_from_csv(csv_path: Path) -> list[dict]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"TITLE", "DESCRIPTION", "URL_TIKTOK"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

        rows = []
        for row in reader:
            rows.append(
                {
                    "TITLE": (row.get("TITLE") or "").strip(),
                    "DESCRIPTION": (row.get("DESCRIPTION") or "").strip(),
                    "URL_TIKTOK": (row.get("URL_TIKTOK") or "").strip(),
                    "SOURCE_FILE": csv_path.name,
                }
            )
        return rows


def iter_events(events: list[dict], loop: bool) -> Iterable[dict]:
    if loop:
        while True:
            for event in events:
                yield event
    else:
        for event in events:
            yield event


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce recipe events to Kafka.")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="recipes_raw")
    parser.add_argument("--csv", default=None, help="Optional CSV file with TITLE, DESCRIPTION, URL_TIKTOK")
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()

    events = SAMPLE_EVENTS if not args.csv else load_events_from_csv(Path(args.csv))

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    for event in iter_events(events, loop=args.loop):
        producer.send(args.topic, event)
        producer.flush()
        print(f"Sent: {event}")
        time.sleep(args.delay_seconds)


if __name__ == "__main__":
    main()