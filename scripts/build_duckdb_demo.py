from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
DUCKDB_DIR = REPO_ROOT / "data" / "duckdb"
DUCKDB_PATH = DUCKDB_DIR / "recipes.duckdb"


def first_existing(paths: list[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError("No source CSV found for the DuckDB demo build.")


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def recipe_status(description: str) -> str:
    text = description.lower()
    if any(token in text for token in ["recipe:", "ingredients:", "ingredient", "المكونات", "مقادير"]):
        return "full_recipe"
    if len(text) > 140 and any(token in text for token in ["cup", "tbsp", "tsp", "salt", "oil", "sauce"]):
        return "partial_recipe"
    return "food_content"


def language_hint(value: object, description: str) -> str:
    hint = clean_text(value).lower()
    if hint in {"en", "fr", "es", "it", "pt", "ar"}:
        return hint
    if re.search(r"[\u0600-\u06ff]", description):
        return "ar"
    return hint or "en"


def quality_score(status: str, description: str) -> float:
    base = {"full_recipe": 0.86, "partial_recipe": 0.68, "food_content": 0.42}.get(status, 0.35)
    length_bonus = min(len(description) / 1200, 0.12)
    return round(min(base + length_bonus, 0.98), 3)


def grade(score: float) -> str:
    if score >= 0.80:
        return "A"
    if score >= 0.60:
        return "B"
    if score >= 0.40:
        return "C"
    return "D"


def ingredient_count(description: str) -> int:
    matches = re.findall(r"\b(?:cup|cups|tbsp|tsp|g|kg|oz|lb|lbs|clove|cloves|spoon|salt|oil|sugar|flour|water)\b", description.lower())
    return max(2, min(len(matches), 12)) if matches else 0


def build_catalog() -> pd.DataFrame:
    source = first_existing(
        [
            RAW_DIR / "tiktok_recipe_discovery.csv",
            RAW_DIR / "social_recipe_verified_real_videos.csv",
            RAW_DIR / "tiktok_food_exact_captions_strict_22_rows_with_louloukitchen.csv",
        ]
    )
    df = pd.read_csv(source, encoding="utf-8-sig").fillna("")
    now = datetime.now(timezone.utc).isoformat()
    records = []

    for idx, row in df.iterrows():
        title = clean_text(row.get("TITLE")) or "Untitled recipe"
        description = clean_text(row.get("DESCRIPTION") or row.get("ORIGINAL_DESCRIPTION") or row.get("EVIDENCE_TEXT"))
        url = clean_text(row.get("URL_TIKTOK"))
        content_id = clean_text(row.get("CONTENT_ID"))
        raw_id = int(content_id) if content_id.isdigit() and len(content_id) < 18 else idx + 1
        status = recipe_status(description)
        score = quality_score(status, description)
        lang = language_hint(row.get("RECIPE_LANGUAGE_HINT"), description)
        cuisine = clean_text(row.get("CUISINE_HINT")) or "Unspecified"
        main_ingredient = clean_text(row.get("MAIN_INGREDIENT_HINT")) or "Mixed"
        count = ingredient_count(description)
        is_recipe = status in {"full_recipe", "partial_recipe"}

        records.append(
            {
                "RAW_ID": raw_id,
                "CONTENT_KEY": content_id or url or str(raw_id),
                "DISPLAY_TITLE": title,
                "ORIGINAL_DESCRIPTION": description,
                "RECOVERED_TEXT": clean_text(row.get("RECOVERED_TEXT")),
                "EVIDENCE_TEXT": clean_text(row.get("EVIDENCE_TEXT")) or description,
                "BEST_EVIDENCE_TEXT": clean_text(row.get("EVIDENCE_TEXT")) or description,
                "URL_TIKTOK": url,
                "RECIPE_LANGUAGE": lang,
                "IS_VEGETARIAN": bool(re.search(r"\b(vegan|vegetarian|veggie|plant based)\b", description.lower())),
                "CUISINE_STYLE": cuisine,
                "MAIN_INGREDIENT": main_ingredient,
                "INGREDIENTS": description,
                "INGREDIENT_COUNT": count,
                "IS_RECIPE": is_recipe,
                "RECIPE_STATUS": status,
                "HAS_INGREDIENT_LIST": count >= 2,
                "HAS_INSTRUCTIONS": len(description) > 120,
                "CAPTION_COMPLETENESS_SCORE": score,
                "EVIDENCE_QUALITY_SCORE": score,
                "BEST_EVIDENCE_QUALITY_SCORE": score,
                "AVG_EVIDENCE_QUALITY_SCORE": score,
                "EVIDENCE_SOURCE_COUNT": 1,
                "OCR_SOURCE_COUNT": 0,
                "AUDIO_SOURCE_COUNT": 0,
                "COMMENT_SOURCE_COUNT": 0,
                "RECIPE_SIGNAL_COUNT": 1 if is_recipe else 0,
                "REJECTION_REASON": "" if is_recipe else "not enough recipe structure for demo catalog",
                "FINAL_RECIPE_TITLE": title,
                "FINAL_RECIPE_TEXT": description,
                "FINAL_RECIPE_JSON": json.dumps({"title": title, "text": description}, ensure_ascii=False),
                "STEP_COUNT": 1 if len(description) > 80 else 0,
                "MISSING_RECIPE_INFO": "",
                "MISSING_INFO_COUNT": 0 if count >= 2 else 1,
                "FINAL_RECIPE_CONFIDENCE": score,
                "FINAL_RECIPE_LANGUAGE": lang,
                "RECIPE_QUALITY_SCORE": score,
                "RECIPE_QUALITY_GRADE": grade(score),
                "PROCESSING_CONFIDENCE": score,
                "MODEL_NAME": "duckdb-demo-rule-builder",
                "PROCESSED_AT": now,
            }
        )

    catalog = pd.DataFrame(records)
    catalog = catalog.sort_values(["RECIPE_QUALITY_SCORE", "RAW_ID"], ascending=[False, True])
    catalog = catalog.drop_duplicates(subset=["CONTENT_KEY"], keep="first")
    return catalog


def build() -> None:
    DUCKDB_DIR.mkdir(parents=True, exist_ok=True)
    if DUCKDB_PATH.exists():
        DUCKDB_PATH.unlink()

    catalog = build_catalog()
    api_catalog = catalog[
        (catalog["IS_RECIPE"])
        & (catalog["RECIPE_STATUS"].isin(["full_recipe", "partial_recipe"]))
        & (catalog["RECIPE_QUALITY_SCORE"] >= 0.60)
    ].copy()
    api_catalog = api_catalog.rename(
        columns={
            "RAW_ID": "ID",
            "DISPLAY_TITLE": "TITLE",
            "FINAL_RECIPE_LANGUAGE": "LANGUAGE",
            "PROCESSING_CONFIDENCE": "CONFIDENCE",
        }
    )[
        [
            "ID",
            "TITLE",
            "URL_TIKTOK",
            "LANGUAGE",
            "IS_VEGETARIAN",
            "CUISINE_STYLE",
            "MAIN_INGREDIENT",
            "INGREDIENTS",
            "FINAL_RECIPE_TITLE",
            "FINAL_RECIPE_TEXT",
            "FINAL_RECIPE_JSON",
            "RECIPE_STATUS",
            "RECIPE_QUALITY_SCORE",
            "RECIPE_QUALITY_GRADE",
            "CONFIDENCE",
            "PROCESSED_AT",
        ]
    ]

    silver = catalog.rename(
        columns={
            "DISPLAY_TITLE": "ORIGINAL_TITLE",
            "RECIPE_LANGUAGE": "RECIPE_LANGUAGE",
        }
    )
    evidence = catalog[
        [
            "RAW_ID",
            "CONTENT_KEY",
            "URL_TIKTOK",
            "EVIDENCE_TEXT",
            "EVIDENCE_QUALITY_SCORE",
            "RECIPE_SIGNAL_COUNT",
            "PROCESSED_AT",
        ]
    ].copy()
    evidence["EVIDENCE_ID"] = range(1, len(evidence) + 1)
    evidence["CONTENT_ID"] = evidence["CONTENT_KEY"]
    evidence["SOURCE_TYPE"] = "caption_original"
    evidence["SOURCE_NAME"] = "duckdb_demo_csv"
    evidence["EVIDENCE_LENGTH"] = evidence["EVIDENCE_TEXT"].str.len()
    evidence["EVIDENCE_QUALITY_CLASS"] = "usable"
    evidence["IS_RECIPE_SIGNAL"] = evidence["RECIPE_SIGNAL_COUNT"] > 0
    evidence["SOURCE_DETAILS"] = "{}"
    evidence["RECORD_HASH"] = evidence["RAW_ID"].astype(str)
    evidence["CREATED_AT"] = evidence["PROCESSED_AT"]

    quality = pd.DataFrame(
        [
            {
                "RUN_DATE": datetime.now(timezone.utc).date().isoformat(),
                "BRONZE_ROWS": len(catalog),
                "SILVER_ROWS": len(catalog),
                "GOLD_ROWS": len(api_catalog),
                "RECIPE_RATE": round(float(catalog["IS_RECIPE"].mean()), 3),
                "FULL_RECIPE_RATE": round(float((catalog["RECIPE_STATUS"] == "full_recipe").mean()), 3),
                "PARTIAL_RECIPE_RATE": round(float((catalog["RECIPE_STATUS"] == "partial_recipe").mean()), 3),
                "REJECTED_RATE": round(float((~catalog["IS_RECIPE"]).mean()), 3),
                "AVG_RECIPE_QUALITY_SCORE": round(float(catalog["RECIPE_QUALITY_SCORE"].mean()), 3),
                "MISSING_FINAL_JSON_RATE": 0,
                "DUPLICATE_URL_COUNT": int(catalog["URL_TIKTOK"].duplicated().sum()),
                "USABLE_OCR_RATE": 0,
            }
        ]
    )

    analytics_summary = pd.DataFrame(
        [
            {"METRIC": "Catalog recipes", "VALUE": len(api_catalog)},
            {"METRIC": "Average quality score", "VALUE": round(float(catalog["RECIPE_QUALITY_SCORE"].mean()), 3)},
            {"METRIC": "Full recipe rows", "VALUE": int((catalog["RECIPE_STATUS"] == "full_recipe").sum())},
        ]
    )

    with duckdb.connect(str(DUCKDB_PATH)) as conn:
        conn.register("catalog_df", catalog)
        conn.execute("create table GOLD_STREAMLIT_RECIPE_CATALOG as select * from catalog_df")
        conn.execute("create table GOLD_TIKTOK_RECIPE_CATALOG as select * from catalog_df")
        conn.execute("create table GOLD_INTERNAL_RECIPE_DEBUG as select * from catalog_df")
        conn.register("api_catalog_df", api_catalog)
        conn.execute("create table GOLD_API_RECIPE_CATALOG as select * from api_catalog_df")
        conn.register("silver_df", silver)
        conn.execute("create table SILVER_TIKTOK_RECIPES as select * from silver_df")
        conn.register("evidence_df", evidence)
        conn.execute("create table SILVER_RECIPE_EVIDENCE as select * from evidence_df")
        conn.register("quality_df", quality)
        conn.execute("create table GOLD_DATA_QUALITY_DAILY as select * from quality_df")
        conn.register("analytics_summary_df", analytics_summary)
        conn.execute("create table RECIPE_ANALYTICS_SUMMARY as select * from analytics_summary_df")
        conn.execute(
            "create table RECIPE_ANALYTICS_BY_CUISINE as "
            "select CUISINE_STYLE, count(*) as RECIPE_COUNT from catalog_df group by CUISINE_STYLE order by RECIPE_COUNT desc"
        )
        conn.execute(
            "create table RECIPE_ANALYTICS_BY_INGREDIENT as "
            "select MAIN_INGREDIENT, count(*) as RECIPE_COUNT from catalog_df group by MAIN_INGREDIENT order by RECIPE_COUNT desc"
        )
        conn.execute(
            "create table RECIPE_ANALYTICS_BY_LANGUAGE as "
            "select RECIPE_LANGUAGE, count(*) as RECIPE_COUNT from catalog_df group by RECIPE_LANGUAGE order by RECIPE_COUNT desc"
        )
        conn.execute(
            "create table RECIPE_ANALYTICS_BY_MODEL as "
            "select MODEL_NAME, count(*) as RECIPE_COUNT from catalog_df group by MODEL_NAME order by RECIPE_COUNT desc"
        )

    print(f"Built {DUCKDB_PATH} with {len(catalog)} catalog rows and {len(api_catalog)} API rows.")


if __name__ == "__main__":
    build()
