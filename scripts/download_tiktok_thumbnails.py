"""Download TikTok thumbnails for Tastagram cards.

The script can read URLs from local CSV files committed in data/raw or from the
Bronze Snowflake table. It writes jpg files to tastagram/static/thumbnails using
the TikTok video id as filename.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Iterable

from scripts.common import configure_logging, get_snowflake_connection

LOGGER = configure_logging("download_tiktok_thumbnails")
BRONZE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA_BRONZE", "BRONZE")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download TikTok thumbnails for Tastagram.")
    parser.add_argument("--source", choices=["csv", "snowflake"], default="csv")
    parser.add_argument("--input-dir", default="data/raw")
    parser.add_argument("--pattern", default="*.csv")
    parser.add_argument("--output-dir", default="tastagram/static/thumbnails")
    parser.add_argument("--limit", type=int, default=0, help="0 means no limit.")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def tiktok_video_id(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"/video/(\d+)", url)
    if match:
        return match.group(1)
    match = re.search(r"/(\d+)(?:\?|$)", url)
    if match:
        return match.group(1)
    return None


def read_csv_urls(input_dir: Path, pattern: str) -> list[str]:
    urls: list[str] = []
    for file_path in sorted(input_dir.glob(pattern)):
        with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                lowered = {str(key).lower(): value for key, value in row.items()}
                url = lowered.get("url_tiktok") or lowered.get("source_platform_url") or lowered.get("url")
                if url and "tiktok.com" in url:
                    urls.append(url)
    return list(dict.fromkeys(urls))


def read_snowflake_urls() -> list[str]:
    query = f"""
    SELECT DISTINCT URL_TIKTOK
    FROM {BRONZE_SCHEMA}.BRONZE_TIKTOK_RECIPES
    WHERE URL_TIKTOK IS NOT NULL
    """
    with get_snowflake_connection(schema=BRONZE_SCHEMA) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall() if row and row[0]]


def require_ytdlp() -> None:
    if shutil.which("yt-dlp") is None:
        raise RuntimeError("yt-dlp is required. Run this inside recipe-content-recovery or install yt-dlp locally.")


def download_thumbnail(url: str, output_dir: Path, force: bool) -> bool:
    video_id = tiktok_video_id(url)
    if not video_id:
        LOGGER.warning("Could not extract TikTok video id from URL=%s", url)
        return False

    output_file = output_dir / f"{video_id}.jpg"
    if output_file.exists() and output_file.stat().st_size > 0 and not force:
        return False

    with tempfile.TemporaryDirectory(prefix="tastagram_thumb_") as tmp:
        tmp_path = Path(tmp)
        output_template = tmp_path / f"{video_id}.%(ext)s"
        command = [
            "yt-dlp",
            "--skip-download",
            "--write-thumbnail",
            "--convert-thumbnails",
            "jpg",
            "--no-playlist",
            "--quiet",
            "--no-warnings",
            "-o",
            str(output_template),
            url,
        ]
        subprocess.run(command, check=True, capture_output=True, text=True)
        candidates = sorted(tmp_path.glob(f"{video_id}.*"))
        if not candidates:
            LOGGER.warning("No thumbnail produced for URL=%s", url)
            return False
        output_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(candidates[0], output_file)
        return True


def limited(values: Iterable[str], limit: int) -> list[str]:
    unique_values = list(dict.fromkeys(values))
    return unique_values[:limit] if limit > 0 else unique_values


def main() -> None:
    args = parse_args()
    require_ytdlp()

    if args.source == "snowflake":
        urls = read_snowflake_urls()
    else:
        urls = read_csv_urls(Path(args.input_dir), args.pattern)

    urls = limited(urls, args.limit)
    LOGGER.info("Found %s TikTok URL(s) for thumbnail download.", len(urls))

    downloaded = 0
    for url in urls:
        try:
            if download_thumbnail(url, Path(args.output_dir), args.force):
                downloaded += 1
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Thumbnail download failed url=%s error=%s", url, exc)

    LOGGER.info("Downloaded %s new thumbnail(s).", downloaded)


if __name__ == "__main__":
    main()
