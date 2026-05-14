from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import random
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - convenience for local dry-runs
    load_dotenv = None

try:
    from TikTokApi import TikTokApi
except ModuleNotFoundError:  # pragma: no cover - checked at runtime for real discovery
    TikTokApi = None


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
    parser.add_argument("--dry-run", action="store_true", help="Print planned inputs and exit.")
    return parser.parse_args()


def load_terms(path: str | None, defaults: list[str]) -> list[str]:
    if not path:
        return defaults

    content = Path(path).read_text(encoding="utf-8").strip()
    if not content:
        return []

    if content.startswith("["):
        return [str(item).strip() for item in json.loads(content) if str(item).strip()]

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
    description = normalize_text(data.get("desc"))
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
    }


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
            disable_caption_filter=args.disable_caption_filter,
        )
        if row:
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
        row = extract_video_fields(data, disable_caption_filter=args.disable_caption_filter)
        if row:
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
