from __future__ import annotations

import argparse
import asyncio
import csv
import html
import json
import logging
import os
import random
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - convenience for local dry-runs
    load_dotenv = None

try:
    from TikTokApi import TikTokApi
except ModuleNotFoundError:  # pragma: no cover - checked at runtime for real discovery
    TikTokApi = None

try:
    from playwright.async_api import async_playwright
except ModuleNotFoundError:  # pragma: no cover - optional heavy caption fallback
    async_playwright = None


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = REPO_ROOT / "data" / "raw" / "tiktok_recipe_discovery.csv"
DEFAULT_REJECTS_OUTPUT = REPO_ROOT / "data" / "raw" / "tiktok_recipe_discovery_rejected_debug.csv"
DEFAULT_STATE_PATH = REPO_ROOT / "data" / "state" / "tiktok_creator_scrape_state.json"

CSV_COLUMNS = [
    "TITLE",
    "DESCRIPTION",
    "URL_TIKTOK",
    "PLATFORM",
    "CONTENT_ID",
    "CREATOR_USERNAME",
    "SOURCE_PLATFORM_URL",
    "RECIPE_LANGUAGE_HINT",
    "CUISINE_HINT",
    "MAIN_INGREDIENT_HINT",
    "DESCRIPTION_IS_PARTIAL",
    "DATA_ORIGIN",
    "VERIFICATION_SOURCE_URL",
    "DESCRIPTION_SOURCE",
    "DESCRIPTION_LENGTH",
    "DESCRIPTION_ENRICHED",
    "ORIGINAL_DESCRIPTION",
    "RECOVERED_TEXT",
    "EVIDENCE_TEXT",
]

DEFAULT_CREATOR_USERNAMES = [
    "thegoldenbalance",
    "cjeatsrecipes",
    "fitwaffle",
    "wishbonekitchen",
    "tinekeyounger",
    "the_pastaqueen",
    "louloukitchen",
    "hervecuisine",
    "marmiton_org",
    "kiwilimon",
    "chefjosera",
    "giallozafferano",
    "fattoincasadabenedetta",
    "cucinabotanica",
    "tastemadebr",
    "panelinha",
    "receiteria",
]

DEFAULT_USER_SEARCH_TERMS = [
    "recipe creator",
    "easy recipes",
    "healthy recipes",
    "meal prep recipes",
    "food recipes",
    "recette cuisine",
    "recettes faciles",
    "cuisine maison",
    "receta cocina",
    "recetas faciles",
    "cocina casera",
    "ricetta cucina",
    "ricette facili",
    "receita cozinha",
    "receitas faceis",
    "\u0648\u0635\u0641\u0627\u062a \u0637\u0628\u062e",
    "\u0627\u0643\u0644\u0627\u062a \u0633\u0647\u0644\u0629",
]

DEFAULT_HASHTAGS = [
    "recipe",
    "recipesoftiktok",
    "easyrecipe",
    "foodtok",
    "cooking",
    "mealprep",
    "healthyrecipes",
    "dinnerideas",
    "recette",
    "recettesfaciles",
    "cuisine",
    "receta",
    "recetasfaciles",
    "cocina",
    "ricetta",
    "ricettefacili",
    "receita",
    "receitasfaceis",
    "\u0648\u0635\u0641\u0627\u062a",
    "\u0637\u0628\u062e",
]

LANGUAGE_HINTS = {
    "fr": ["recette", "recettes", "cuisine", "poulet", "pates", "ingrédients", "ingredients"],
    "es": ["receta", "recetas", "cocina", "ingredientes", "pollo", "comida"],
    "it": ["ricetta", "ricette", "cucina", "ingredienti", "pollo", "pasta"],
    "pt": ["receita", "receitas", "cozinha", "ingredientes", "frango"],
    "ar": ["\u0648\u0635\u0641\u0629", "\u0648\u0635\u0641\u0627\u062a", "\u0637\u0628\u062e", "\u0645\u0643\u0648\u0646\u0627\u062a"],
}

RECIPE_SIGNALS = [
    "recipe",
    "recipes",
    "ingredient",
    "ingredients",
    "how to make",
    "cook",
    "cooking",
    "bake",
    "meal prep",
    "dinner",
    "lunch",
    "breakfast",
    "recette",
    "ingrédient",
    "ingredients",
    "cuisine",
    "receta",
    "ingredientes",
    "cocina",
    "ricetta",
    "ingredienti",
    "cucina",
    "receita",
    "ingredientes",
    "cozinha",
    "\u0648\u0635\u0641\u0629",
    "\u0648\u0635\u0641\u0627\u062a",
    "\u0645\u0643\u0648\u0646\u0627\u062a",
    "\u0637\u0628\u062e",
]


LOGGER = logging.getLogger("tiktok_recipe_discovery")


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover public TikTok recipe videos and export a Bronze-compatible CSV."
    )
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV path.")
    parser.add_argument(
        "--rejects-output",
        default=str(DEFAULT_REJECTS_OUTPUT),
        help="Optional debug CSV path for rejected captions.",
    )
    parser.add_argument("--max-rows", type=int, default=250, help="Maximum accepted videos.")
    parser.add_argument("--per-creator", type=int, default=20, help="Videos to fetch per known creator.")
    parser.add_argument("--per-hashtag", type=int, default=40, help="Videos to fetch per hashtag.")
    parser.add_argument("--per-user-search", type=int, default=10, help="Users to fetch per search term.")
    parser.add_argument("--per-searched-user", type=int, default=8, help="Videos to fetch per discovered user.")
    parser.add_argument("--sleep-min", type=float, default=2.5, help="Minimum sleep between groups.")
    parser.add_argument("--sleep-max", type=float, default=6.0, help="Maximum sleep between groups.")
    parser.add_argument("--num-sessions", type=int, default=1, help="TikTokApi browser sessions.")
    parser.add_argument(
        "--session-timeout",
        type=float,
        default=90.0,
        help="Seconds before failing TikTokApi session creation.",
    )
    parser.add_argument("--headless", action="store_true", help="Run Playwright headless.")
    parser.add_argument("--skip-hashtags", action="store_true", help="Skip hashtag collection.")
    parser.add_argument("--skip-user-search", action="store_true", help="Skip TikTok user search.")
    parser.add_argument("--skip-creators", action="store_true", help="Skip known creator collection.")
    parser.add_argument(
        "--disable-caption-filter",
        action="store_true",
        help="Accept every TikTok video with a non-empty caption. Useful to debug empty runs.",
    )
    parser.add_argument(
        "--debug-rejects",
        action="store_true",
        help="Write rejected captions to rejects-output for inspection.",
    )
    parser.add_argument("--creators-file", default=None, help="Optional newline or JSON list of creators.")
    parser.add_argument("--hashtags-file", default=None, help="Optional newline or JSON list of hashtags.")
    parser.add_argument("--queries-file", default=None, help="Optional newline or JSON list of user search terms.")
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH), help="Creator scrape state JSON path.")
    parser.add_argument(
        "--daily-skip-hours",
        type=float,
        default=24.0,
        help="Skip a creator completed less than this many hours ago.",
    )
    parser.add_argument("--force-rescrape", action="store_true", help="Ignore creator daily skip state.")
    parser.add_argument("--overwrite-output", action="store_true", help="Start a fresh output CSV instead of appending.")
    parser.add_argument(
        "--caption-enrichment",
        choices=["off", "metadata", "browser"],
        default="metadata",
        help=(
            "Try to improve captions after TikTokApi collection. "
            "'metadata' uses TikTok web/oEmbed JSON. 'browser' also allows future Playwright DOM fallback."
        ),
    )
    parser.add_argument(
        "--caption-min-length",
        type=int,
        default=160,
        help="Only enrich captions shorter than this many characters unless --caption-enrich-all is set.",
    )
    parser.add_argument(
        "--caption-enrich-all",
        action="store_true",
        help="Try caption enrichment for every accepted video, even when TikTokApi already returned a long caption.",
    )
    parser.add_argument(
        "--caption-request-timeout",
        type=float,
        default=12.0,
        help="HTTP timeout in seconds for web metadata caption enrichment.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print planned inputs and exit.")
    return parser.parse_args()


def load_terms(path: str | None, defaults: list[str]) -> list[str]:
    if not path:
        return defaults

    content = Path(path).read_text(encoding="utf-8").strip()
    if not content:
        return []

    if content.startswith("[") or content.startswith("{"):
        payload = json.loads(content)
        if isinstance(payload, list):
            return [str(item).strip() for item in payload if str(item).strip()]
        if isinstance(payload, dict):
            creators = payload.get("creators")
            if isinstance(creators, list):
                return [str(item).strip() for item in creators if str(item).strip()]
            grouped = payload.get("creators_by_language")
            if isinstance(grouped, dict):
                flattened: list[str] = []
                for values in grouped.values():
                    if isinstance(values, list):
                        flattened.extend(str(item).strip() for item in values if str(item).strip())
                return list(dict.fromkeys(flattened))
        return []

    return [
        line.strip().lstrip("#").strip()
        for line in content.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def load_existing_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            {column: (row.get(column) or "") for column in CSV_COLUMNS}
            for row in reader
            if row.get("URL_TIKTOK")
        ]


def load_scrape_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"creators": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("Could not parse state file %s; starting with empty state.", path)
        return {"creators": {}}


def save_scrape_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def parse_state_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def creator_completed_recently(
    state: dict[str, Any],
    username: str,
    skip_hours: float,
) -> bool:
    creator_state = state.get("creators", {}).get(username, {})
    if creator_state.get("status") != "completed":
        return False
    completed_at = parse_state_timestamp(creator_state.get("completed_at"))
    if completed_at is None:
        return False
    return datetime.now(timezone.utc) - completed_at < timedelta(hours=skip_hours)


def order_creators_for_resume(creators: list[str], state: dict[str, Any], skip_hours: float) -> list[str]:
    return sorted(
        dict.fromkeys(creators),
        key=lambda username: (
            creator_completed_recently(state, username, skip_hours),
            state.get("creators", {}).get(username, {}).get("completed_at") or "",
            username,
        ),
    )


def mark_creator_state(
    state: dict[str, Any],
    username: str,
    status: str,
    scanned: int = 0,
    accepted: int = 0,
    error: str | None = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    state.setdefault("creators", {})[username] = {
        "status": status,
        "last_run_at": now,
        "completed_at": now if status == "completed" else None,
        "scanned": scanned,
        "accepted": accepted,
        "error": error,
    }


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_caption(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"\\u([0-9a-fA-F]{4})", lambda match: chr(int(match.group(1), 16)), text)
    text = text.replace("\\n", "\n").replace("\\/", "/")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def infer_language(text: str) -> str:
    lowered = text.lower()
    for language, markers in LANGUAGE_HINTS.items():
        if any(marker.lower() in lowered for marker in markers):
            return language
    return "en"


def is_recipe_caption(text: str) -> bool:
    lowered = text.lower()
    if any(signal.lower() in lowered for signal in RECIPE_SIGNALS):
        return True
    hashtag_tokens = re.findall(r"#([\w\u0600-\u06ff]+)", lowered)
    return any(token in DEFAULT_HASHTAGS for token in hashtag_tokens)


def extract_video_fields(
    data: dict[str, Any],
    fallback_creator: str | None = None,
    disable_caption_filter: bool = False,
) -> dict[str, str] | None:
    video_id = normalize_text(data.get("id"))
    description = normalize_caption(data.get("desc"))
    if not video_id or not description:
        return None
    if not disable_caption_filter and not is_recipe_caption(description):
        return None

    author_data = data.get("author") or {}
    creator = normalize_text(
        author_data.get("uniqueId")
        or author_data.get("unique_id")
        or author_data.get("nickname")
        or fallback_creator
        or "unknown_creator"
    )

    url = f"https://www.tiktok.com/@{creator}/video/{video_id}"
    create_time = data.get("createTime")
    published_at = None
    if create_time:
        try:
            published_at = datetime.fromtimestamp(int(create_time), tz=timezone.utc).isoformat()
        except Exception:
            published_at = None

    return {
        "TITLE": description[:120],
        "DESCRIPTION": description,
        "ORIGINAL_DESCRIPTION": description,
        "RECOVERED_TEXT": "",
        "EVIDENCE_TEXT": description,
        "URL_TIKTOK": url,
        "PLATFORM": "tiktok",
        "CONTENT_ID": video_id,
        "CREATOR_USERNAME": creator,
        "SOURCE_PLATFORM_URL": f"https://www.tiktok.com/@{creator}",
        "RECIPE_LANGUAGE_HINT": infer_language(description),
        "CUISINE_HINT": "",
        "MAIN_INGREDIENT_HINT": "",
        "DESCRIPTION_IS_PARTIAL": "true",
        "DATA_ORIGIN": "tiktokapi_discovery",
        "VERIFICATION_SOURCE_URL": url if not published_at else f"{url}?published_at={published_at}",
        "DESCRIPTION_SOURCE": "tiktokapi_desc",
        "DESCRIPTION_LENGTH": str(len(description)),
        "DESCRIPTION_ENRICHED": "false",
    }


def collect_caption_candidates(payload: Any, video_id: str) -> list[str]:
    candidates: list[str] = []
    if isinstance(payload, dict):
        id_hint = str(
            payload.get("id")
            or payload.get("itemId")
            or payload.get("aweme_id")
            or payload.get("video_id")
            or ""
        )
        for key, value in payload.items():
            lowered_key = str(key).lower()
            if lowered_key in {"desc", "description", "title", "caption"} and isinstance(value, str):
                if not id_hint or not video_id or id_hint == video_id or len(value) >= 80:
                    candidates.append(normalize_caption(value))
            candidates.extend(collect_caption_candidates(value, video_id))
    elif isinstance(payload, list):
        for item in payload:
            candidates.extend(collect_caption_candidates(item, video_id))
    return candidates


def strip_html_tags(value: str) -> str:
    without_scripts = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", value, flags=re.DOTALL | re.IGNORECASE)
    without_tags = re.sub(r"<[^>]+>", " ", without_scripts)
    return normalize_caption(without_tags)


def extract_json_script(html_text: str, script_id: str) -> Any | None:
    pattern = rf'<script[^>]+id=["\']{re.escape(script_id)}["\'][^>]*>(.*?)</script>'
    match = re.search(pattern, html_text, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        return None
    raw = html.unescape(match.group(1)).strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def extract_meta_content(html_text: str) -> list[str]:
    candidates: list[str] = []
    patterns = [
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
        r'<meta[^>]+content=["\'](.*?)["\'][^>]+property=["\']og:description["\']',
        r'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']description["\']',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, html_text, flags=re.DOTALL | re.IGNORECASE):
            candidates.append(normalize_caption(match.group(1)))
    return candidates


def looks_like_real_caption(candidate: str, existing: str) -> bool:
    if not candidate:
        return False
    lowered = candidate.lower()
    boilerplate = [
        "tiktok video",
        "watch the latest videos",
        "discover videos related",
        "log in to follow creators",
    ]
    if any(text in lowered for text in boilerplate):
        return False
    if existing and candidate == existing:
        return False
    return len(candidate) >= max(40, len(existing) + 15)


def choose_best_caption(candidates: Iterable[str], existing: str) -> tuple[str, str] | None:
    cleaned = []
    seen: set[str] = set()
    for candidate in candidates:
        caption = normalize_caption(candidate)
        if not looks_like_real_caption(caption, existing) or caption in seen:
            continue
        seen.add(caption)
        cleaned.append(caption)
    if not cleaned:
        return None
    cleaned.sort(
        key=lambda caption: (
            is_recipe_caption(caption),
            len(caption),
        ),
        reverse=True,
    )
    return cleaned[0], "web_metadata"


def fetch_oembed_caption(url: str, timeout: float) -> list[str]:
    endpoint = "https://www.tiktok.com/oembed"
    headers = {
        "User-Agent": os.getenv(
            "TIKTOK_HTTP_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        )
    }
    try:
        response = requests.get(endpoint, params={"url": url}, headers=headers, timeout=timeout)
        if not response.ok:
            return []
        data = response.json()
    except Exception:
        return []
    return [
        normalize_caption(data.get("title")),
        strip_html_tags(str(data.get("html") or "")),
    ]


def fetch_web_caption(url: str, video_id: str, existing: str, timeout: float) -> tuple[str, str] | None:
    headers = {
        "User-Agent": os.getenv(
            "TIKTOK_HTTP_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ),
        "Accept-Language": "en-US,en;q=0.9,fr;q=0.8,es;q=0.7,it;q=0.6,pt;q=0.5,ar;q=0.4",
    }
    candidates: list[str] = []

    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.ok:
            page = response.text
            candidates.extend(extract_meta_content(page))
            for script_id in ("SIGI_STATE", "__UNIVERSAL_DATA_FOR_REHYDRATION__"):
                payload = extract_json_script(page, script_id)
                if payload is not None:
                    candidates.extend(collect_caption_candidates(payload, video_id))
    except Exception:
        LOGGER.debug("Web caption fetch failed for url=%s", url, exc_info=True)

    candidates.extend(fetch_oembed_caption(url, timeout=timeout))
    return choose_best_caption(candidates, existing)


async def fetch_browser_caption(
    url: str,
    video_id: str,
    existing: str,
    timeout: float,
    headless: bool,
) -> tuple[str, str] | None:
    if async_playwright is None:
        return None

    candidates: list[str] = []
    browser = None
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=headless)
            page = await browser.new_page(
                user_agent=os.getenv(
                    "TIKTOK_HTTP_USER_AGENT",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                ),
                locale="en-US",
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=int(timeout * 1000))
            await page.wait_for_timeout(2500)
            page_html = await page.content()
            candidates.extend(extract_meta_content(page_html))
            for script_id in ("SIGI_STATE", "__UNIVERSAL_DATA_FOR_REHYDRATION__"):
                payload = extract_json_script(page_html, script_id)
                if payload is not None:
                    candidates.extend(collect_caption_candidates(payload, video_id))

            dom_text_candidates = await page.locator('[data-e2e*="browse-video-desc"], [data-e2e*="video-desc"]').all_inner_texts()
            candidates.extend(dom_text_candidates)
    except Exception:
        LOGGER.debug("Browser caption fetch failed for url=%s", url, exc_info=True)
    finally:
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass

    chosen = choose_best_caption(candidates, existing)
    if chosen:
        return chosen[0], "browser_dom"
    return None


async def enrich_row_caption(row: dict[str, str], args: argparse.Namespace) -> dict[str, str]:
    if args.caption_enrichment == "off":
        return row
    existing = row.get("DESCRIPTION", "")
    should_enrich = args.caption_enrich_all or len(existing) < args.caption_min_length
    if not should_enrich:
        return row

    result = await asyncio.to_thread(
        fetch_web_caption,
        row["URL_TIKTOK"],
        row.get("CONTENT_ID", ""),
        existing,
        args.caption_request_timeout,
    )
    if not result:
        if args.caption_enrichment == "browser":
            result = await fetch_browser_caption(
                row["URL_TIKTOK"],
                row.get("CONTENT_ID", ""),
                existing,
                args.caption_request_timeout,
                args.headless,
            )
    if not result:
        return row

    caption, source = result
    if len(caption) <= len(existing):
        return row

    LOGGER.info(
        "Caption enriched content_id=%s old_len=%s new_len=%s source=%s",
        row.get("CONTENT_ID"),
        len(existing),
        len(caption),
        source,
    )
    original_description = row.get("ORIGINAL_DESCRIPTION") or existing
    evidence_parts = [original_description]
    if caption and caption != original_description:
        evidence_parts.append(caption)
    evidence_text = "\n\n".join(part for part in evidence_parts if part)

    row["DESCRIPTION"] = caption
    row["TITLE"] = caption[:120]
    row["ORIGINAL_DESCRIPTION"] = original_description
    row["RECOVERED_TEXT"] = caption if caption != original_description else ""
    row["EVIDENCE_TEXT"] = evidence_text
    row["RECIPE_LANGUAGE_HINT"] = infer_language(caption)
    row["DESCRIPTION_IS_PARTIAL"] = "false" if len(caption) >= args.caption_min_length else "true"
    row["DATA_ORIGIN"] = f"{row.get('DATA_ORIGIN') or 'tiktokapi_discovery'}+caption_enrichment"
    row["DESCRIPTION_SOURCE"] = source
    row["DESCRIPTION_LENGTH"] = str(len(caption))
    row["DESCRIPTION_ENRICHED"] = "true"
    return row


async def sleep_between(args: argparse.Namespace) -> None:
    delay = random.uniform(args.sleep_min, args.sleep_max)
    LOGGER.info("Sleeping %.1fs to reduce request pressure", delay)
    await asyncio.sleep(delay)


async def collect_creator_videos(
    api: TikTokApi,
    username: str,
    count: int,
    args: argparse.Namespace,
) -> tuple[list[dict[str, str]], list[dict[str, str]], int]:
    rows: list[dict[str, str]] = []
    rejects: list[dict[str, str]] = []
    scanned = 0
    LOGGER.info("Fetching creator videos username=%s count=%s", username, count)
    async for video in api.user(username=username).videos(count=count):
        scanned += 1
        data = getattr(video, "as_dict", {}) or {}
        row = extract_video_fields(
            data,
            fallback_creator=username,
            disable_caption_filter=True,
        )
        if row:
            row = await enrich_row_caption(row, args)
        if row and (args.disable_caption_filter or is_recipe_caption(row["DESCRIPTION"])):
            rows.append(row)
        elif args.debug_rejects:
            rejects.append(reject_row("creator", username, data))
    LOGGER.info("Creator done username=%s scanned=%s accepted=%s rejected=%s", username, scanned, len(rows), scanned - len(rows))
    return rows, rejects, scanned


async def collect_hashtag_videos(
    api: TikTokApi,
    hashtag: str,
    count: int,
    args: argparse.Namespace,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    rows: list[dict[str, str]] = []
    rejects: list[dict[str, str]] = []
    scanned = 0
    clean_hashtag = hashtag.lstrip("#")
    LOGGER.info("Fetching hashtag videos hashtag=%s count=%s", clean_hashtag, count)
    async for video in api.hashtag(name=clean_hashtag).videos(count=count):
        scanned += 1
        data = getattr(video, "as_dict", {}) or {}
        row = extract_video_fields(data, disable_caption_filter=True)
        if row:
            row = await enrich_row_caption(row, args)
        if row and (args.disable_caption_filter or is_recipe_caption(row["DESCRIPTION"])):
            rows.append(row)
        elif args.debug_rejects:
            rejects.append(reject_row("hashtag", clean_hashtag, data))
    LOGGER.info("Hashtag done hashtag=%s scanned=%s accepted=%s rejected=%s", clean_hashtag, scanned, len(rows), scanned - len(rows))
    return rows, rejects


def reject_row(source_type: str, source_value: str, data: dict[str, Any]) -> dict[str, str]:
    author_data = data.get("author") or {}
    creator = normalize_text(
        author_data.get("uniqueId")
        or author_data.get("unique_id")
        or author_data.get("nickname")
        or "unknown_creator"
    )
    return {
        "SOURCE_TYPE": source_type,
        "SOURCE_VALUE": source_value,
        "CONTENT_ID": normalize_text(data.get("id")),
        "CREATOR_USERNAME": creator,
        "DESCRIPTION": normalize_text(data.get("desc")),
        "REJECT_REASON": "missing_recipe_signal_or_required_fields",
    }


async def discover_users(api: TikTokApi, query: str, count: int) -> list[str]:
    usernames: list[str] = []
    LOGGER.info("Searching users query=%s count=%s", query, count)
    async for user in api.search.users(query, count=count):
        data = getattr(user, "as_dict", {}) or {}
        username = normalize_text(
            data.get("uniqueId")
            or data.get("unique_id")
            or data.get("user", {}).get("uniqueId")
            or getattr(user, "username", None)
        )
        if username:
            usernames.append(username)
    return usernames


def dedupe_rows(rows: Iterable[dict[str, str]], max_rows: int) -> list[dict[str, str]]:
    seen: set[str] = set()
    output: list[dict[str, str]] = []
    for row in rows:
        key = row["URL_TIKTOK"]
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
        if len(output) >= max_rows:
            break
    return output


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def checkpoint_rows(args: argparse.Namespace, rows: list[dict[str, str]]) -> None:
    write_rows(Path(args.output), rows)
    LOGGER.info("Checkpoint wrote %s accepted rows to %s", len(rows), args.output)


def write_rejects(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "SOURCE_TYPE",
                "SOURCE_VALUE",
                "CONTENT_ID",
                "CREATOR_USERNAME",
                "DESCRIPTION",
                "REJECT_REASON",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


async def run_discovery(args: argparse.Namespace) -> list[dict[str, str]]:
    state_path = Path(args.state_path)
    state = load_scrape_state(state_path)
    creators = order_creators_for_resume(
        load_terms(args.creators_file, DEFAULT_CREATOR_USERNAMES),
        state,
        args.daily_skip_hours,
    )
    hashtags = load_terms(args.hashtags_file, DEFAULT_HASHTAGS)
    queries = load_terms(args.queries_file, DEFAULT_USER_SEARCH_TERMS)

    if args.dry_run:
        LOGGER.info("Creators: %s", creators)
        LOGGER.info("Hashtags: %s", hashtags)
        LOGGER.info("User search terms: %s", queries)
        return []

    if TikTokApi is None:
        raise RuntimeError(
            "TikTokApi is not installed. Run inside the tiktok-monitor container "
            "or install requirements-tiktok-monitor.txt."
        )

    ms_token = os.getenv("TIKTOK_MS_TOKEN") or os.getenv("ms_token")
    ms_tokens = [ms_token] if ms_token else None
    browser = os.getenv("TIKTOK_BROWSER", "chromium")

    rows: list[dict[str, str]] = [] if args.overwrite_output else load_existing_rows(Path(args.output))
    rows = dedupe_rows(rows, args.max_rows)
    if rows:
        LOGGER.info("Loaded %s existing output rows from %s", len(rows), args.output)
    if len(rows) >= args.max_rows:
        LOGGER.info("Output already has max_rows=%s rows; nothing to collect.", args.max_rows)
        return rows

    rejects: list[dict[str, str]] = []
    async with TikTokApi() as api:
        if not ms_tokens:
            LOGGER.warning(
                "TIKTOK_MS_TOKEN is not set. TikTokApi may hang, return empty data, "
                "or be blocked by TikTok."
            )

        LOGGER.info(
            "Creating TikTokApi sessions browser=%s headless=%s sessions=%s timeout=%ss",
            browser,
            args.headless,
            args.num_sessions,
            args.session_timeout,
        )
        await asyncio.wait_for(
            api.create_sessions(
                ms_tokens=ms_tokens,
                num_sessions=args.num_sessions,
                sleep_after=8,
                browser=browser,
                headless=args.headless,
            ),
            timeout=args.session_timeout,
        )
        LOGGER.info("TikTokApi sessions created successfully")

        if not args.skip_creators:
            for username in creators:
                if not args.force_rescrape and creator_completed_recently(state, username, args.daily_skip_hours):
                    LOGGER.info(
                        "Skipping creator username=%s because it was completed within %.1f hours.",
                        username,
                        args.daily_skip_hours,
                    )
                    continue

                scanned = 0
                accepted = 0
                try:
                    found, rejected, scanned = await collect_creator_videos(api, username, args.per_creator, args)
                    accepted = len(found)
                    rows.extend(found)
                    rejects.extend(rejected)
                    mark_creator_state(state, username, "completed", scanned=scanned, accepted=accepted)
                except Exception as exc:
                    LOGGER.exception("Creator collection failed username=%s", username)
                    mark_creator_state(state, username, "failed", scanned=scanned, accepted=accepted, error=str(exc))
                save_scrape_state(state_path, state)
                rows = dedupe_rows(rows, args.max_rows)
                checkpoint_rows(args, rows)
                if len(rows) >= args.max_rows:
                    return rows
                await sleep_between(args)

        if not args.skip_hashtags:
            for hashtag in hashtags:
                try:
                    found, rejected = await collect_hashtag_videos(api, hashtag, args.per_hashtag, args)
                    rows.extend(found)
                    rejects.extend(rejected)
                except Exception:
                    LOGGER.exception("Hashtag collection failed hashtag=%s", hashtag)
                rows = dedupe_rows(rows, args.max_rows)
                checkpoint_rows(args, rows)
                if len(rows) >= args.max_rows:
                    return rows
                await sleep_between(args)

        if not args.skip_user_search:
            discovered: list[str] = []
            for query in queries:
                try:
                    discovered.extend(await discover_users(api, query, args.per_user_search))
                except Exception:
                    LOGGER.exception("User search failed query=%s", query)
                await sleep_between(args)

            for username in dict.fromkeys(discovered):
                if not args.force_rescrape and creator_completed_recently(state, username, args.daily_skip_hours):
                    LOGGER.info(
                        "Skipping discovered creator username=%s because it was completed within %.1f hours.",
                        username,
                        args.daily_skip_hours,
                    )
                    continue

                scanned = 0
                accepted = 0
                try:
                    found, rejected, scanned = await collect_creator_videos(api, username, args.per_searched_user, args)
                    accepted = len(found)
                    rows.extend(found)
                    rejects.extend(rejected)
                    mark_creator_state(state, username, "completed", scanned=scanned, accepted=accepted)
                except Exception as exc:
                    LOGGER.exception("Discovered creator collection failed username=%s", username)
                    mark_creator_state(state, username, "failed", scanned=scanned, accepted=accepted, error=str(exc))
                save_scrape_state(state_path, state)
                rows = dedupe_rows(rows, args.max_rows)
                checkpoint_rows(args, rows)
                if len(rows) >= args.max_rows:
                    return rows
                await sleep_between(args)

    if args.debug_rejects:
        write_rejects(Path(args.rejects_output), rejects)
        LOGGER.info("Wrote %s rejected debug rows to %s", len(rejects), args.rejects_output)

    return dedupe_rows(rows, args.max_rows)


def main() -> None:
    if load_dotenv is not None:
        load_dotenv(REPO_ROOT / ".env", override=True)
    configure_logging()
    args = parse_args()

    LOGGER.info("Starting TikTok recipe discovery")
    rows = asyncio.run(run_discovery(args))
    if args.dry_run:
        LOGGER.info("Dry-run completed; no CSV written.")
        return

    output = Path(args.output)
    write_rows(output, rows)
    LOGGER.info("Wrote %s rows to %s", len(rows), output)


if __name__ == "__main__":
    main()
