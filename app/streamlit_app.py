"""Streamlit application for the TikTok recipe catalog."""
from __future__ import annotations

import os
from typing import Any

import pandas as pd
import duckdb
from pathlib import Path
import streamlit as st

st.set_page_config(page_title="TikTok Recipe Intelligence", layout="wide")


REPO_ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", REPO_ROOT / "data" / "duckdb" / "recipes.duckdb"))


def gold_table_name(table: str) -> str:
    normalized = table.upper()
    if not normalized.replace("_", "").isalnum():
        raise ValueError(f"Invalid DuckDB table name: {table}")
    return normalized


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [column.upper() for column in df.columns]
    return df


def query_duckdb(query: str) -> pd.DataFrame:
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB database not found at {DUCKDB_PATH}. Run `python scripts/build_duckdb_demo.py` first."
        )
    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as conn:
        return normalize_columns(conn.execute(query).fetchdf())


def safe_scalar(value: Any, fallback: str = "unknown") -> str:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return fallback
    return str(value)


def is_true(value: Any) -> bool:
    return bool(value) if not pd.isna(value) else False


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                linear-gradient(180deg, #f7fafc 0%, #eef3f8 42%, #f8fafc 100%);
        }
        .block-container {
            padding-top: 1.3rem;
            padding-bottom: 2rem;
            max-width: 1440px;
        }
        h1, h2, h3 {
            letter-spacing: 0;
        }
        [data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #d9e2ec;
            border-radius: 8px;
            padding: 14px 16px;
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05);
        }
        [data-testid="stMetricLabel"] {
            color: #486174;
        }
        [data-testid="stMetricValue"] {
            color: #102a43;
            font-size: 1.65rem;
        }
        div[data-testid="stVerticalBlockBorderWrapper"] {
            border-color: #d9e2ec;
            border-radius: 8px;
            background: #ffffff;
        }
        .recipe-title {
            color: #102a43;
            font-size: 1.02rem;
            font-weight: 700;
            line-height: 1.35;
            margin-bottom: 0.25rem;
        }
        .recipe-meta {
            color: #486174;
            font-size: 0.9rem;
            line-height: 1.55;
        }
        .pill {
            display: inline-block;
            border: 1px solid #bcccdc;
            border-radius: 999px;
            color: #243b53;
            background: #f8fafc;
            padding: 2px 9px;
            margin-right: 6px;
            margin-top: 6px;
            font-size: 0.78rem;
            font-weight: 600;
        }
        .status-note {
            color: #627d98;
            font-size: 0.9rem;
        }
        .hero {
            background: #ffffff;
            border: 1px solid #d9e2ec;
            border-radius: 8px;
            padding: 30px 34px;
            box-shadow: 0 12px 30px rgba(15, 23, 42, 0.06);
            margin-bottom: 18px;
        }
        .hero-kicker {
            color: #486174;
            font-size: 0.84rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 10px;
        }
        .hero-title {
            color: #102a43;
            font-size: 2.55rem;
            line-height: 1.08;
            font-weight: 800;
            margin-bottom: 12px;
        }
        .hero-copy {
            color: #334e68;
            font-size: 1.05rem;
            line-height: 1.65;
            max-width: 940px;
        }
        .section-card {
            background: #ffffff;
            border: 1px solid #d9e2ec;
            border-radius: 8px;
            padding: 18px 20px;
            min-height: 150px;
            box-shadow: 0 8px 22px rgba(15, 23, 42, 0.04);
        }
        .section-card-title {
            color: #102a43;
            font-weight: 800;
            font-size: 1.02rem;
            margin-bottom: 8px;
        }
        .section-card-copy {
            color: #486174;
            font-size: 0.92rem;
            line-height: 1.55;
        }
        .pipeline-step {
            background: #ffffff;
            border: 1px solid #d9e2ec;
            border-radius: 8px;
            padding: 13px 14px;
            color: #243b53;
            min-height: 92px;
        }
        .pipeline-step strong {
            color: #102a43;
            display: block;
            margin-bottom: 4px;
        }
        .search-shell {
            background: #ffffff;
            border: 1px solid #d9e2ec;
            border-radius: 8px;
            padding: 24px 26px;
            box-shadow: 0 12px 30px rgba(15, 23, 42, 0.06);
            margin-bottom: 18px;
        }
        .search-title {
            color: #102a43;
            font-size: 2.2rem;
            line-height: 1.12;
            font-weight: 800;
            margin-bottom: 8px;
        }
        .search-subtitle {
            color: #486174;
            font-size: 1rem;
            line-height: 1.55;
            margin-bottom: 2px;
        }
        .result-title {
            color: #102a43;
            font-size: 1.12rem;
            line-height: 1.35;
            font-weight: 800;
            margin-bottom: 5px;
        }
        .result-url {
            color: #2f80ed;
            font-size: 0.82rem;
            overflow-wrap: anywhere;
            margin-bottom: 10px;
        }
        .result-meta {
            color: #486174;
            font-size: 0.88rem;
            line-height: 1.55;
            margin-top: 8px;
        }
        .result-count {
            color: #486174;
            font-size: 0.92rem;
            margin-bottom: 10px;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has(.result-title) {
            border-color: #c9d8e8;
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05);
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has(.result-title):hover {
            border-color: #8fb4d8;
            box-shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
        }
        .search-shell + div [data-testid="stTextInput"] input {
            border-radius: 8px;
            border-color: #9fb3c8;
            min-height: 48px;
            font-size: 1rem;
        }
        div[data-testid="stButton"] button,
        div[data-testid="stLinkButton"] a {
            border-radius: 8px;
            font-weight: 700;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(ttl=300)
def load_catalog() -> pd.DataFrame:
    query = f"""
    SELECT
        RAW_ID,
        CONTENT_KEY,
        DISPLAY_TITLE,
        ORIGINAL_DESCRIPTION,
        RECOVERED_TEXT,
        EVIDENCE_TEXT,
        BEST_EVIDENCE_TEXT,
        URL_TIKTOK,
        RECIPE_LANGUAGE,
        IS_VEGETARIAN,
        CUISINE_STYLE,
        MAIN_INGREDIENT,
        INGREDIENTS,
        INGREDIENT_COUNT,
        IS_RECIPE,
        RECIPE_STATUS,
        HAS_INGREDIENT_LIST,
        HAS_INSTRUCTIONS,
        CAPTION_COMPLETENESS_SCORE,
        EVIDENCE_QUALITY_SCORE,
        BEST_EVIDENCE_QUALITY_SCORE,
        AVG_EVIDENCE_QUALITY_SCORE,
        EVIDENCE_SOURCE_COUNT,
        OCR_SOURCE_COUNT,
        AUDIO_SOURCE_COUNT,
        COMMENT_SOURCE_COUNT,
        RECIPE_SIGNAL_COUNT,
        REJECTION_REASON,
        FINAL_RECIPE_TITLE,
        FINAL_RECIPE_TEXT,
        FINAL_RECIPE_JSON,
        STEP_COUNT,
        MISSING_RECIPE_INFO,
        MISSING_INFO_COUNT,
        FINAL_RECIPE_CONFIDENCE,
        FINAL_RECIPE_LANGUAGE,
        RECIPE_QUALITY_SCORE,
        RECIPE_QUALITY_GRADE,
        PROCESSING_CONFIDENCE,
        MODEL_NAME,
        PROCESSED_AT
    FROM {gold_table_name("GOLD_STREAMLIT_RECIPE_CATALOG")}
    ORDER BY PROCESSED_AT DESC
    """
    return query_duckdb(query)


@st.cache_data(ttl=300)
def load_optional_table(table: str) -> pd.DataFrame:
    try:
        return query_duckdb(f"SELECT * FROM {gold_table_name(table)}")
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_layer_counts() -> pd.DataFrame:
    database = os.getenv("SNOWFLAKE_DB", "TIKTOK_PORTFOLIO_DB")
    bronze = os.getenv("SNOWFLAKE_SCHEMA_BRONZE", "BRONZE")
    silver = os.getenv("SNOWFLAKE_SCHEMA_SILVER", "SILVER")
    gold = os.getenv("SNOWFLAKE_SCHEMA_GOLD", "GOLD")
    query = f"""
    SELECT 'Bronze raw recipes' AS LAYER, COUNT(*) AS RECORDS
    FROM {database}.{bronze}.BRONZE_TIKTOK_RECIPES
    UNION ALL
    SELECT 'Silver enriched recipes' AS LAYER, COUNT(*) AS RECORDS
    FROM {database}.{silver}.SILVER_TIKTOK_RECIPES
    UNION ALL
    SELECT 'Gold Streamlit catalog' AS LAYER, COUNT(*) AS RECORDS
    FROM {database}.{gold}.GOLD_STREAMLIT_RECIPE_CATALOG
    """
    try:
        return query_duckdb(query)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_recovery_counts() -> pd.DataFrame:
    database = os.getenv("SNOWFLAKE_DB", "TIKTOK_PORTFOLIO_DB")
    silver = os.getenv("SNOWFLAKE_SCHEMA_SILVER", "SILVER")
    query = f"""
    SELECT
        SOURCE_TYPE AS METHOD,
        EVIDENCE_QUALITY_CLASS AS STATUS,
        COUNT(*) AS RECORDS,
        AVG(EVIDENCE_QUALITY_SCORE) AS AVG_CONFIDENCE,
        AVG(EVIDENCE_LENGTH) AS AVG_TEXT_LENGTH
    FROM {database}.{silver}.SILVER_RECIPE_EVIDENCE
    GROUP BY SOURCE_TYPE, EVIDENCE_QUALITY_CLASS
    ORDER BY RECORDS DESC
    """
    try:
        return query_duckdb(query)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_data_quality_daily() -> pd.DataFrame:
    try:
        return query_duckdb(f"SELECT * FROM {gold_table_name('GOLD_DATA_QUALITY_DAILY')}")
    except Exception:
        return pd.DataFrame()


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Catalog filters")
    search = st.sidebar.text_input("Search title, cuisine, ingredient", value="")

    language_options = sorted(df["RECIPE_LANGUAGE"].dropna().astype(str).unique().tolist())
    cuisine_options = sorted(df["CUISINE_STYLE"].dropna().astype(str).unique().tolist())
    ingredient_options = sorted(df["MAIN_INGREDIENT"].dropna().astype(str).unique().tolist())
    model_options = sorted(df["MODEL_NAME"].dropna().astype(str).unique().tolist())
    status_options = sorted(df.get("RECIPE_STATUS", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())

    selected_languages = st.sidebar.multiselect("Language", language_options, default=language_options)
    selected_cuisines = st.sidebar.multiselect("Cuisine", cuisine_options, default=cuisine_options)
    selected_ingredients = st.sidebar.multiselect("Main ingredient", ingredient_options, default=[])
    selected_models = st.sidebar.multiselect("LLM model", model_options, default=model_options)
    selected_statuses = st.sidebar.multiselect("Recipe status", status_options, default=status_options)

    vegetarian_mode = st.sidebar.radio(
        "Vegetarian",
        options=["All", "Vegetarian only", "Non-vegetarian only"],
        horizontal=False,
    )

    confidence_min = float(df["PROCESSING_CONFIDENCE"].fillna(0).min())
    confidence_max = float(df["PROCESSING_CONFIDENCE"].fillna(1).max())
    min_confidence = st.sidebar.slider(
        "Minimum confidence",
        min_value=0.0,
        max_value=1.0,
        value=max(0.0, round(confidence_min, 2)),
        step=0.05,
    )

    filtered = df.copy()
    filtered = filtered[filtered["RECIPE_LANGUAGE"].astype(str).isin(selected_languages)]
    filtered = filtered[filtered["CUISINE_STYLE"].astype(str).isin(selected_cuisines)]
    filtered = filtered[filtered["MODEL_NAME"].astype(str).isin(selected_models)]
    filtered = filtered[filtered["PROCESSING_CONFIDENCE"].fillna(0) >= min_confidence]
    if selected_statuses and "RECIPE_STATUS" in filtered:
        filtered = filtered[filtered["RECIPE_STATUS"].astype(str).isin(selected_statuses)]

    if selected_ingredients:
        filtered = filtered[filtered["MAIN_INGREDIENT"].astype(str).isin(selected_ingredients)]

    if vegetarian_mode == "Vegetarian only":
        filtered = filtered[filtered["IS_VEGETARIAN"].fillna(False) == True]  # noqa: E712
    elif vegetarian_mode == "Non-vegetarian only":
        filtered = filtered[filtered["IS_VEGETARIAN"].fillna(False) == False]  # noqa: E712

    if search.strip():
        pattern = search.strip().lower()
        searchable = (
            filtered["DISPLAY_TITLE"].fillna("").astype(str)
            + " "
            + filtered["CUISINE_STYLE"].fillna("").astype(str)
            + " "
            + filtered["MAIN_INGREDIENT"].fillna("").astype(str)
            + " "
            + filtered.get("INGREDIENTS", pd.Series([""] * len(filtered), index=filtered.index)).fillna("").astype(str)
            + " "
            + filtered.get("FINAL_RECIPE_TEXT", pd.Series([""] * len(filtered), index=filtered.index)).fillna("").astype(str)
            + " "
            + filtered.get("ORIGINAL_DESCRIPTION", pd.Series([""] * len(filtered), index=filtered.index)).fillna("").astype(str)
        ).str.lower()
        filtered = filtered[searchable.str.contains(pattern, regex=False)]

    st.sidebar.divider()
    st.sidebar.metric("Visible recipes", len(filtered))
    st.sidebar.caption(f"Confidence range in catalog: {confidence_min:.2f} - {confidence_max:.2f}")
    return filtered


def render_sidebar_navigation() -> str:
    st.sidebar.title("Recipe Intelligence")
    st.sidebar.caption("DuckDB-backed recipe analytics platform")
    page = st.sidebar.radio(
        "Navigation",
        options=["Search", "Analytics", "Catalog", "Data quality", "Platform"],
        index=0,
    )
    st.sidebar.divider()
    return page


def value_counts(df: pd.DataFrame, column: str, top_n: int = 12) -> pd.DataFrame:
    if df.empty or column not in df:
        return pd.DataFrame(columns=[column, "RECIPE_COUNT"])
    counts = (
        df[column]
        .fillna("unknown")
        .astype(str)
        .replace("", "unknown")
        .value_counts()
        .head(top_n)
        .rename_axis(column)
        .reset_index(name="RECIPE_COUNT")
    )
    return counts


def render_metrics(df: pd.DataFrame, filtered: pd.DataFrame) -> None:
    total_recipes = len(df)
    visible_recipes = len(filtered)
    veg_pct = filtered["IS_VEGETARIAN"].fillna(False).mean() * 100 if visible_recipes else 0
    avg_confidence = filtered["PROCESSING_CONFIDENCE"].mean() if visible_recipes else 0
    distinct_cuisines = filtered["CUISINE_STYLE"].nunique() if visible_recipes else 0
    distinct_ingredients = filtered["MAIN_INGREDIENT"].nunique() if visible_recipes else 0

    metric_1, metric_2, metric_3, metric_4, metric_5 = st.columns(5)
    metric_1.metric("Catalog size", f"{total_recipes:,}")
    metric_2.metric("Visible records", f"{visible_recipes:,}")
    metric_3.metric("Vegetarian share", f"{veg_pct:.1f}%")
    metric_4.metric("Avg confidence", f"{avg_confidence:.2f}")
    metric_5.metric("Cuisine / ingredient spread", f"{distinct_cuisines} / {distinct_ingredients}")


def render_home(df: pd.DataFrame, filtered: pd.DataFrame) -> None:
    st.markdown(
        """
        <div class="hero">
            <div class="hero-kicker">Portfolio data platform</div>
            <div class="hero-title">TikTok Recipe Intelligence</div>
            <div class="hero-copy">
                An end-to-end data engineering project that turns unstructured social recipe content into
                curated, searchable, and analytics-ready data. The platform combines ingestion, DuckDB
                medallion modeling, LLM enrichment, dbt serving views, DuckDB analytics, Airflow orchestration,
                a FastAPI service, and this Streamlit data product.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    render_metrics(df, filtered)

    st.subheader("Explore the app")
    card_1, card_2, card_3 = st.columns(3)
    with card_1:
        st.markdown(
            """
            <div class="section-card">
              <div class="section-card-title">Analytics</div>
              <div class="section-card-copy">
                Inspect cuisine, ingredient, language, model, confidence, and Spark aggregate outputs.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with card_2:
        st.markdown(
            """
            <div class="section-card">
              <div class="section-card-title">Catalog</div>
              <div class="section-card-copy">
                Browse enriched recipe records, open TikTok sources, and filter by semantic metadata.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with card_3:
        st.markdown(
            """
            <div class="section-card">
              <div class="section-card-title">Data Quality</div>
              <div class="section-card-copy">
                Review unknown fields, missing values, low-confidence records, and warehouse layer counts.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.subheader("Pipeline shape")
    step_1, step_2, step_3, step_4, step_5 = st.columns(5)
    steps = [
        ("Bronze", "Raw TikTok or CSV records land in DuckDB with source metadata."),
        ("Silver", "OpenRouter enriches recipe text into validated structured fields."),
        ("Gold", "dbt creates curated serving views for API and dashboard consumers."),
        ("Spark", "Batch analytics are written back to DuckDB aggregate tables."),
        ("Products", "FastAPI and Streamlit expose the curated recipe intelligence layer."),
    ]
    for column, (title, copy) in zip([step_1, step_2, step_3, step_4, step_5], steps):
        with column:
            st.markdown(
                f"""
                <div class="pipeline-step">
                    <strong>{title}</strong>
                    {copy}
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.subheader("Latest enriched recipes")
    latest_columns = [
        "DISPLAY_TITLE",
        "CUISINE_STYLE",
        "MAIN_INGREDIENT",
        "RECIPE_LANGUAGE",
        "PROCESSING_CONFIDENCE",
        "PROCESSED_AT",
    ]
    st.dataframe(
        filtered.sort_values("PROCESSED_AT", ascending=False)[latest_columns].head(8),
        use_container_width=True,
        hide_index=True,
    )


def filter_for_search(
    df: pd.DataFrame,
    query: str,
    languages: list[str],
    cuisines: list[str],
    ingredients: list[str],
    dietary_mode: str,
    min_confidence: float,
) -> pd.DataFrame:
    filtered = df.copy()

    if languages:
        filtered = filtered[filtered["RECIPE_LANGUAGE"].astype(str).isin(languages)]
    if cuisines:
        filtered = filtered[filtered["CUISINE_STYLE"].astype(str).isin(cuisines)]
    if ingredients:
        filtered = filtered[filtered["MAIN_INGREDIENT"].astype(str).isin(ingredients)]

    filtered = filtered[filtered["PROCESSING_CONFIDENCE"].fillna(0) >= min_confidence]

    if dietary_mode == "Vegetarian":
        filtered = filtered[filtered["IS_VEGETARIAN"].fillna(False) == True]  # noqa: E712
    elif dietary_mode == "Non-vegetarian":
        filtered = filtered[filtered["IS_VEGETARIAN"].fillna(False) == False]  # noqa: E712

    if query.strip():
        pattern = query.strip().lower()
        searchable = (
            filtered["DISPLAY_TITLE"].fillna("").astype(str)
            + " "
            + filtered["CUISINE_STYLE"].fillna("").astype(str)
            + " "
            + filtered["MAIN_INGREDIENT"].fillna("").astype(str)
            + " "
            + filtered["RECIPE_LANGUAGE"].fillna("").astype(str)
            + " "
            + filtered.get("FINAL_RECIPE_TEXT", pd.Series([""] * len(filtered), index=filtered.index)).fillna("").astype(str)
            + " "
            + filtered.get("ORIGINAL_DESCRIPTION", pd.Series([""] * len(filtered), index=filtered.index)).fillna("").astype(str)
        ).str.lower()
        filtered = filtered[searchable.str.contains(pattern, regex=False)]

    return filtered


def render_result_card(row: pd.Series) -> None:
    cuisine = safe_scalar(row.get("CUISINE_STYLE"))
    ingredient = safe_scalar(row.get("MAIN_INGREDIENT"))
    language = safe_scalar(row.get("RECIPE_LANGUAGE"))
    title = safe_scalar(row.get("DISPLAY_TITLE"), "Untitled recipe")
    url = safe_scalar(row.get("URL_TIKTOK"), "#")
    confidence = row.get("PROCESSING_CONFIDENCE", 0)
    quality = row.get("RECIPE_QUALITY_SCORE", confidence)
    grade = safe_scalar(row.get("RECIPE_QUALITY_GRADE"), "D")
    model = safe_scalar(row.get("MODEL_NAME"))
    dietary = "Vegetarian" if is_true(row.get("IS_VEGETARIAN")) else "Non-vegetarian"
    recipe_status = safe_scalar(row.get("RECIPE_STATUS"), "unknown")
    completeness = row.get("CAPTION_COMPLETENESS_SCORE", 0)
    final_confidence = row.get("FINAL_RECIPE_CONFIDENCE", 0)
    final_recipe_text = safe_scalar(row.get("FINAL_RECIPE_TEXT"), "")
    original_description = safe_scalar(row.get("ORIGINAL_DESCRIPTION"), "")
    recovered_text = safe_scalar(row.get("RECOVERED_TEXT"), "")

    with st.container(border=True):
        body_col, action_col = st.columns([5.6, 1.1])
        with body_col:
            st.markdown(f"<div class='result-title'>{title}</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='result-url'>{url}</div>", unsafe_allow_html=True)
            st.markdown(
                "".join(
                    [
                        f"<span class='pill'>{cuisine}</span>",
                        f"<span class='pill'>{ingredient}</span>",
                        f"<span class='pill'>{language}</span>",
                        f"<span class='pill'>{dietary}</span>",
                        f"<span class='pill'>{recipe_status}</span>",
                        f"<span class='pill'>quality {grade}</span>",
                    ]
                ),
                unsafe_allow_html=True,
            )
            st.markdown(
                "<div class='result-meta'>"
                f"Quality {quality:.2f} | Confidence {confidence:.2f} | Completeness {completeness:.2f} | "
                f"Recipe confidence {final_confidence:.2f} | "
                f"Model {model} | Processed {safe_scalar(row.get('PROCESSED_AT'))}"
                "</div>",
                unsafe_allow_html=True,
            )
            if final_recipe_text:
                with st.expander("Recipe card"):
                    st.markdown(final_recipe_text)
            with st.expander("Source evidence"):
                st.markdown("**Original caption**")
                st.write(original_description)
                if recovered_text:
                    st.markdown("**Recovered text**")
                    st.write(recovered_text)
                best_evidence_text = safe_scalar(row.get("BEST_EVIDENCE_TEXT"), "")
                if best_evidence_text:
                    st.markdown("**Best scored evidence**")
                    st.write(best_evidence_text)
        with action_col:
            if url.startswith("http"):
                st.link_button("Open", url, use_container_width=True)
            else:
                st.button("Open", disabled=True, use_container_width=True)


def render_search_browser(df: pd.DataFrame) -> None:
    st.markdown(
        """
        <div class="search-shell">
            <div class="search-title">Find enriched recipe videos</div>
            <div class="search-subtitle">
                Search the Gold recipe catalog by title, cuisine, ingredient, language, dietary signal,
                and enrichment confidence. Each result links back to the original TikTok source.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    search_query = st.text_input(
        "Search recipes",
        placeholder="Try pasta, chicken, mexican, vegetarian, french...",
        label_visibility="collapsed",
    )

    filter_col_1, filter_col_2, filter_col_3, filter_col_4 = st.columns([1.2, 1.2, 1.4, 1.0])
    language_options = sorted(df["RECIPE_LANGUAGE"].dropna().astype(str).unique().tolist())
    cuisine_options = sorted(df["CUISINE_STYLE"].dropna().astype(str).unique().tolist())
    ingredient_options = sorted(df["MAIN_INGREDIENT"].dropna().astype(str).unique().tolist())

    with filter_col_1:
        selected_languages = st.multiselect("Language", language_options, default=language_options)
    with filter_col_2:
        selected_cuisines = st.multiselect("Cuisine", cuisine_options, default=[])
    with filter_col_3:
        selected_ingredients = st.multiselect("Ingredient", ingredient_options, default=[])
    with filter_col_4:
        dietary_mode = st.selectbox("Dietary", ["All", "Vegetarian", "Non-vegetarian"])

    control_col_1, control_col_2, control_col_3 = st.columns([1.1, 1.2, 1.0])
    with control_col_1:
        min_confidence = st.slider("Minimum confidence", 0.0, 1.0, 0.0, 0.05)
    with control_col_2:
        sort_mode = st.selectbox("Sort by", ["Newest", "Highest quality", "Highest confidence", "Lowest confidence", "Title A-Z"])
    with control_col_3:
        result_limit = st.selectbox("Results", [12, 24, 48, 96], index=1)

    results = filter_for_search(
        df,
        query=search_query,
        languages=selected_languages,
        cuisines=selected_cuisines,
        ingredients=selected_ingredients,
        dietary_mode=dietary_mode,
        min_confidence=min_confidence,
    )

    if sort_mode == "Highest quality" and "RECIPE_QUALITY_SCORE" in results:
        results = results.sort_values("RECIPE_QUALITY_SCORE", ascending=False)
    elif sort_mode == "Highest confidence":
        results = results.sort_values("PROCESSING_CONFIDENCE", ascending=False)
    elif sort_mode == "Lowest confidence":
        results = results.sort_values("PROCESSING_CONFIDENCE", ascending=True)
    elif sort_mode == "Title A-Z":
        results = results.sort_values("DISPLAY_TITLE", ascending=True)
    else:
        results = results.sort_values("PROCESSED_AT", ascending=False)

    metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)
    metric_col_1.metric("Matching recipes", f"{len(results):,}")
    metric_col_2.metric("Cuisines", f"{results['CUISINE_STYLE'].nunique() if not results.empty else 0}")
    metric_col_3.metric("Ingredients", f"{results['MAIN_INGREDIENT'].nunique() if not results.empty else 0}")
    metric_col_4.metric(
        "Avg quality",
        f"{results['RECIPE_QUALITY_SCORE'].mean():.2f}" if not results.empty and "RECIPE_QUALITY_SCORE" in results else "0.00",
    )

    st.markdown(
        f"<div class='result-count'>Showing {min(len(results), result_limit)} of {len(results)} matching recipes</div>",
        unsafe_allow_html=True,
    )

    if results.empty:
        st.info("No recipes match the current search and filters.")
        return

    for _, row in results.head(result_limit).iterrows():
        render_result_card(row)


def render_analytics(filtered: pd.DataFrame) -> None:
    st.title("Analytics")
    st.caption("Explore the enriched recipe catalog and DuckDB aggregate tables.")

    chart_col_1, chart_col_2 = st.columns(2)
    with chart_col_1:
        st.subheader("Top cuisines")
        cuisine_counts = value_counts(filtered, "CUISINE_STYLE", top_n=10)
        st.bar_chart(cuisine_counts, x="CUISINE_STYLE", y="RECIPE_COUNT", use_container_width=True)

    with chart_col_2:
        st.subheader("Languages")
        language_counts = value_counts(filtered, "RECIPE_LANGUAGE", top_n=10)
        st.bar_chart(language_counts, x="RECIPE_LANGUAGE", y="RECIPE_COUNT", use_container_width=True)

    chart_col_3, chart_col_4 = st.columns(2)
    with chart_col_3:
        st.subheader("Top ingredients")
        ingredient_counts = value_counts(filtered, "MAIN_INGREDIENT", top_n=12)
        st.bar_chart(ingredient_counts, x="MAIN_INGREDIENT", y="RECIPE_COUNT", use_container_width=True)

    with chart_col_4:
        st.subheader("LLM model distribution")
        model_counts = value_counts(filtered, "MODEL_NAME", top_n=8)
        st.bar_chart(model_counts, x="MODEL_NAME", y="RECIPE_COUNT", use_container_width=True)

    confidence_col, vegetarian_col = st.columns(2)
    with confidence_col:
        st.subheader("Confidence distribution")
        confidence_buckets = filtered.copy()
        confidence_buckets["CONFIDENCE_BUCKET"] = pd.cut(
            confidence_buckets["PROCESSING_CONFIDENCE"].fillna(0),
            bins=[0, 0.5, 0.75, 0.9, 1.0],
            labels=["0-0.50", "0.50-0.75", "0.75-0.90", "0.90-1.00"],
            include_lowest=True,
        )
        confidence_counts = value_counts(confidence_buckets, "CONFIDENCE_BUCKET", top_n=10)
        st.bar_chart(confidence_counts, x="CONFIDENCE_BUCKET", y="RECIPE_COUNT", use_container_width=True)

    with vegetarian_col:
        st.subheader("Dietary split")
        dietary_df = filtered.copy()
        dietary_df["DIETARY_CLASS"] = dietary_df["IS_VEGETARIAN"].fillna(False).map(
            {True: "Vegetarian", False: "Non-vegetarian"}
        )
        dietary_counts = value_counts(dietary_df, "DIETARY_CLASS", top_n=5)
        st.bar_chart(dietary_counts, x="DIETARY_CLASS", y="RECIPE_COUNT", use_container_width=True)

    st.divider()
    st.title("DuckDB analytics")
    render_duckdb_analytics()


def render_catalog(filtered: pd.DataFrame) -> None:
    st.title("Recipe catalog")
    st.caption("Browse the curated Gold catalog generated from enriched Silver recipe records.")

    sort_mode = st.radio(
        "Sort",
        options=["Newest first", "Highest confidence", "Lowest confidence"],
        horizontal=True,
        label_visibility="collapsed",
    )
    if sort_mode == "Highest confidence":
        display_df = filtered.sort_values("PROCESSING_CONFIDENCE", ascending=False)
    elif sort_mode == "Lowest confidence":
        display_df = filtered.sort_values("PROCESSING_CONFIDENCE", ascending=True)
    else:
        display_df = filtered.sort_values("PROCESSED_AT", ascending=False)

    for _, row in display_df.head(60).iterrows():
        with st.container(border=True):
            title_col, action_col = st.columns([5, 1])
            title_col.markdown(
                f"<div class='recipe-title'>{safe_scalar(row.get('DISPLAY_TITLE'), 'Untitled recipe')}</div>",
                unsafe_allow_html=True,
            )
            action_col.link_button("Open TikTok", safe_scalar(row.get("URL_TIKTOK"), "#"))

            st.markdown(
                "".join(
                    [
                        f"<span class='pill'>{safe_scalar(row.get('CUISINE_STYLE'))}</span>",
                        f"<span class='pill'>{safe_scalar(row.get('MAIN_INGREDIENT'))}</span>",
                        f"<span class='pill'>{safe_scalar(row.get('RECIPE_LANGUAGE'))}</span>",
                        f"<span class='pill'>{'vegetarian' if is_true(row.get('IS_VEGETARIAN')) else 'non-vegetarian'}</span>",
                    ]
                ),
                unsafe_allow_html=True,
            )
            st.markdown(
                "<div class='recipe-meta'>"
                f"Confidence: {row.get('PROCESSING_CONFIDENCE', 0):.2f} | "
                f"Recipe confidence: {row.get('FINAL_RECIPE_CONFIDENCE', 0):.2f} | "
                f"Model: {safe_scalar(row.get('MODEL_NAME'))} | "
                f"Processed: {safe_scalar(row.get('PROCESSED_AT'))}"
                "</div>",
                unsafe_allow_html=True,
            )
            final_recipe_text = safe_scalar(row.get("FINAL_RECIPE_TEXT"), "")
            if final_recipe_text:
                with st.expander("Final recipe"):
                    st.markdown(final_recipe_text)
            with st.expander("Original evidence"):
                st.markdown("**Original caption**")
                st.write(safe_scalar(row.get("ORIGINAL_DESCRIPTION"), ""))
                recovered_text = safe_scalar(row.get("RECOVERED_TEXT"), "")
                if recovered_text:
                    st.markdown("**Recovered text**")
                    st.write(recovered_text)

    if len(display_df) > 60:
        st.caption(f"Showing 60 of {len(display_df)} matching recipes.")

    st.subheader("Table")
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_duckdb_analytics() -> None:
    summary = load_optional_table("RECIPE_ANALYTICS_SUMMARY")
    by_cuisine = load_optional_table("RECIPE_ANALYTICS_BY_CUISINE")
    by_ingredient = load_optional_table("RECIPE_ANALYTICS_BY_INGREDIENT")
    by_language = load_optional_table("RECIPE_ANALYTICS_BY_LANGUAGE")
    by_model = load_optional_table("RECIPE_ANALYTICS_BY_MODEL")

    if summary.empty and by_cuisine.empty and by_ingredient.empty and by_language.empty and by_model.empty:
        st.info("DuckDB analytics tables are not available yet.")
        return

    if not summary.empty:
        st.subheader("DuckDB summary")
        st.dataframe(summary, use_container_width=True, hide_index=True)

    spark_col_1, spark_col_2 = st.columns(2)
    with spark_col_1:
        if not by_cuisine.empty and "CUISINE_STYLE" in by_cuisine and "RECIPE_COUNT" in by_cuisine:
            st.subheader("DuckDB by cuisine")
            st.bar_chart(by_cuisine.head(15), x="CUISINE_STYLE", y="RECIPE_COUNT", use_container_width=True)
        if not by_language.empty and "RECIPE_LANGUAGE" in by_language and "RECIPE_COUNT" in by_language:
            st.subheader("DuckDB by language")
            st.bar_chart(by_language.head(15), x="RECIPE_LANGUAGE", y="RECIPE_COUNT", use_container_width=True)

    with spark_col_2:
        if not by_ingredient.empty and "MAIN_INGREDIENT" in by_ingredient and "RECIPE_COUNT" in by_ingredient:
            st.subheader("DuckDB by ingredient")
            st.bar_chart(by_ingredient.head(15), x="MAIN_INGREDIENT", y="RECIPE_COUNT", use_container_width=True)
        if not by_model.empty and "MODEL_NAME" in by_model and "RECIPE_COUNT" in by_model:
            st.subheader("DuckDB by model")
            st.bar_chart(by_model.head(15), x="MODEL_NAME", y="RECIPE_COUNT", use_container_width=True)


def render_quality(df: pd.DataFrame, filtered: pd.DataFrame) -> None:
    st.title("Data quality")
    st.caption("Monitor enrichment completeness, confidence, and warehouse layer health.")

    layer_counts = load_layer_counts()
    if not layer_counts.empty:
        st.subheader("Warehouse layer counts")
        st.dataframe(layer_counts, use_container_width=True, hide_index=True)

    daily_quality = load_data_quality_daily()
    if not daily_quality.empty:
        st.subheader("Gold data quality daily")
        st.dataframe(daily_quality, use_container_width=True, hide_index=True)

    recovery_counts = load_recovery_counts()
    if not recovery_counts.empty:
        st.subheader("Recipe evidence sources")
        st.dataframe(recovery_counts, use_container_width=True, hide_index=True)

    st.subheader("Enrichment quality")
    text_columns = ["RECIPE_LANGUAGE", "CUISINE_STYLE", "MAIN_INGREDIENT"]
    quality_rows = []
    for column in text_columns:
        unknown_count = df[column].fillna("unknown").astype(str).str.lower().eq("unknown").sum()
        missing_count = df[column].isna().sum()
        quality_rows.append(
            {
                "FIELD": column,
                "UNKNOWN_COUNT": int(unknown_count),
                "MISSING_COUNT": int(missing_count),
                "UNKNOWN_RATE": round(unknown_count / len(df), 4) if len(df) else 0,
            }
        )
    st.dataframe(pd.DataFrame(quality_rows), use_container_width=True, hide_index=True)

    low_confidence = filtered[filtered["PROCESSING_CONFIDENCE"].fillna(0) < 0.75]
    if "RECIPE_QUALITY_SCORE" in filtered:
        low_quality = filtered[filtered["RECIPE_QUALITY_SCORE"].fillna(0) < 0.60]
        st.metric("Low quality records", f"{len(low_quality):,}")
    if "FINAL_RECIPE_CONFIDENCE" in filtered:
        weak_final_recipes = filtered[filtered["FINAL_RECIPE_CONFIDENCE"].fillna(0) < 0.65]
        st.metric("Weak final recipe cards", f"{len(weak_final_recipes):,}")
    st.subheader("Records to review")
    st.dataframe(
        low_confidence[
            [
                "RAW_ID",
                "DISPLAY_TITLE",
                "RECIPE_STATUS",
                "CAPTION_COMPLETENESS_SCORE",
                "FINAL_RECIPE_CONFIDENCE",
                "RECIPE_LANGUAGE",
                "CUISINE_STYLE",
                "MAIN_INGREDIENT",
                "PROCESSING_CONFIDENCE",
                "MODEL_NAME",
                "PROCESSED_AT",
            ]
        ].head(100),
        use_container_width=True,
        hide_index=True,
    )


def render_platform() -> None:
    st.title("Platform")
    st.caption("Operational map of the services behind this portfolio data product.")

    service_rows = [
        {"LAYER": "Demo database", "TECH": "DuckDB", "ROLE": "Curated recipe catalog and aggregate tables"},
        {"LAYER": "Transformation", "TECH": "Python + dbt-duckdb", "ROLE": "Local Gold serving tables for API and dashboard consumers"},
        {"LAYER": "Batch analytics", "TECH": "DuckDB SQL", "ROLE": "Aggregate analytics tables by cuisine, ingredient, language, and model"},
        {"LAYER": "Enrichment", "TECH": "OpenRouter / LLM", "ROLE": "Semantic extraction from recipe descriptions"},
        {"LAYER": "Serving", "TECH": "FastAPI", "ROLE": "HTTP API over curated recipe data"},
        {"LAYER": "Dashboard", "TECH": "Streamlit", "ROLE": "Interactive analytics and quality review app"},
        {"LAYER": "Deployment", "TECH": "Docker + Nginx", "ROLE": "Local service composition and reverse proxy deployment"},
    ]
    st.dataframe(pd.DataFrame(service_rows), use_container_width=True, hide_index=True)

    st.subheader("Useful local endpoints")
    endpoints = pd.DataFrame(
        [
            {"SERVICE": "Streamlit", "LOCAL_URL": "http://127.0.0.1:18501"},
            {"SERVICE": "FastAPI docs", "LOCAL_URL": "http://127.0.0.1:18000/docs"},
        ]
    )
    st.dataframe(endpoints, use_container_width=True, hide_index=True)


def main() -> None:
    inject_styles()
    page = render_sidebar_navigation()

    try:
        df = load_catalog()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Unable to load data from DuckDB: {exc}")
        st.info("Run dbt from the `dbt_project/` directory before opening the app.")
        st.stop()

    if df.empty:
        st.info("No recipes available yet.")
        st.stop()

    df["PROCESSED_AT"] = pd.to_datetime(df["PROCESSED_AT"], errors="coerce")
    df["PROCESSING_CONFIDENCE"] = pd.to_numeric(df["PROCESSING_CONFIDENCE"], errors="coerce")
    if "RECIPE_QUALITY_SCORE" in df:
        df["RECIPE_QUALITY_SCORE"] = pd.to_numeric(df["RECIPE_QUALITY_SCORE"], errors="coerce").fillna(0)
    if "BEST_EVIDENCE_QUALITY_SCORE" in df:
        df["BEST_EVIDENCE_QUALITY_SCORE"] = pd.to_numeric(df["BEST_EVIDENCE_QUALITY_SCORE"], errors="coerce").fillna(0)
    if "CAPTION_COMPLETENESS_SCORE" in df:
        df["CAPTION_COMPLETENESS_SCORE"] = pd.to_numeric(df["CAPTION_COMPLETENESS_SCORE"], errors="coerce").fillna(0)
    if "RECIPE_STATUS" in df:
        df["RECIPE_STATUS"] = df["RECIPE_STATUS"].fillna("unknown")
    if "FINAL_RECIPE_CONFIDENCE" in df:
        df["FINAL_RECIPE_CONFIDENCE"] = pd.to_numeric(df["FINAL_RECIPE_CONFIDENCE"], errors="coerce").fillna(0)
    for text_column in ["FINAL_RECIPE_TEXT", "ORIGINAL_DESCRIPTION", "RECOVERED_TEXT", "BEST_EVIDENCE_TEXT"]:
        if text_column in df:
            df[text_column] = df[text_column].fillna("")
    df["MODEL_NAME"] = df.get("MODEL_NAME", pd.Series(["unknown"] * len(df))).fillna("unknown")

    if page == "Search":
        render_search_browser(df)
        return

    filtered = apply_filters(df)

    if page == "Analytics":
        render_analytics(filtered)
    elif page == "Catalog":
        render_catalog(filtered)
    elif page == "Data quality":
        render_quality(df, filtered)
    else:
        render_platform()


if __name__ == "__main__":
    main()
