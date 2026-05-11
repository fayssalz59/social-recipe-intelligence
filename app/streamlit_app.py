"""Streamlit application for the TikTok recipe catalog."""
from __future__ import annotations

import os
from typing import Any

import pandas as pd
import snowflake.connector
import streamlit as st

st.set_page_config(page_title="TikTok Recipe Intelligence", layout="wide")


def gold_table_name(table: str) -> str:
    schema = os.getenv("SNOWFLAKE_SCHEMA_GOLD", "GOLD")
    if not schema.replace("_", "").isalnum():
        raise ValueError(f"Invalid Snowflake schema name: {schema}")
    return f"{schema}.{table}"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [column.upper() for column in df.columns]
    return df


def query_snowflake(query: str) -> pd.DataFrame:
    required = {
        "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "SNOWFLAKE_DB": os.getenv("SNOWFLAKE_DB"),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(f"Missing Snowflake environment variables: {', '.join(missing)}")

    with snowflake.connector.connect(
        user=required["SNOWFLAKE_USER"],
        password=required["SNOWFLAKE_PASSWORD"],
        account=required["SNOWFLAKE_ACCOUNT"],
        warehouse=required["SNOWFLAKE_WAREHOUSE"],
        database=required["SNOWFLAKE_DB"],
        schema=os.getenv("SNOWFLAKE_SCHEMA_GOLD", "GOLD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    ) as conn:
        return normalize_columns(pd.read_sql(query, conn))


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
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(ttl=300)
def load_catalog() -> pd.DataFrame:
    query = f"""
    SELECT
        RAW_ID,
        DISPLAY_TITLE,
        URL_TIKTOK,
        RECIPE_LANGUAGE,
        IS_VEGETARIAN,
        CUISINE_STYLE,
        MAIN_INGREDIENT,
        PROCESSING_CONFIDENCE,
        MODEL_NAME,
        PROCESSED_AT
    FROM {gold_table_name("GOLD_STREAMLIT_RECIPE_CATALOG")}
    ORDER BY PROCESSED_AT DESC
    """
    return query_snowflake(query)


@st.cache_data(ttl=300)
def load_optional_table(table: str) -> pd.DataFrame:
    try:
        return query_snowflake(f"SELECT * FROM {gold_table_name(table)}")
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
        return query_snowflake(query)
    except Exception:
        return pd.DataFrame()


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filters")
    search = st.sidebar.text_input("Search title, cuisine, ingredient", value="")

    language_options = sorted(df["RECIPE_LANGUAGE"].dropna().astype(str).unique().tolist())
    cuisine_options = sorted(df["CUISINE_STYLE"].dropna().astype(str).unique().tolist())
    ingredient_options = sorted(df["MAIN_INGREDIENT"].dropna().astype(str).unique().tolist())
    model_options = sorted(df["MODEL_NAME"].dropna().astype(str).unique().tolist())

    selected_languages = st.sidebar.multiselect("Language", language_options, default=language_options)
    selected_cuisines = st.sidebar.multiselect("Cuisine", cuisine_options, default=cuisine_options)
    selected_ingredients = st.sidebar.multiselect("Main ingredient", ingredient_options, default=[])
    selected_models = st.sidebar.multiselect("LLM model", model_options, default=model_options)

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
        ).str.lower()
        filtered = filtered[searchable.str.contains(pattern, regex=False)]

    st.sidebar.divider()
    st.sidebar.metric("Visible recipes", len(filtered))
    st.sidebar.caption(f"Confidence range in catalog: {confidence_min:.2f} - {confidence_max:.2f}")
    return filtered


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


def render_overview(filtered: pd.DataFrame) -> None:
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


def render_catalog(filtered: pd.DataFrame) -> None:
    st.subheader("Recipe catalog")

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
                f"Model: {safe_scalar(row.get('MODEL_NAME'))} | "
                f"Processed: {safe_scalar(row.get('PROCESSED_AT'))}"
                "</div>",
                unsafe_allow_html=True,
            )

    if len(display_df) > 60:
        st.caption(f"Showing 60 of {len(display_df)} matching recipes.")

    st.subheader("Table")
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_spark_analytics() -> None:
    summary = load_optional_table("RECIPE_ANALYTICS_SUMMARY")
    by_cuisine = load_optional_table("RECIPE_ANALYTICS_BY_CUISINE")
    by_ingredient = load_optional_table("RECIPE_ANALYTICS_BY_INGREDIENT")
    by_language = load_optional_table("RECIPE_ANALYTICS_BY_LANGUAGE")
    by_model = load_optional_table("RECIPE_ANALYTICS_BY_MODEL")

    if summary.empty and by_cuisine.empty and by_ingredient.empty and by_language.empty and by_model.empty:
        st.info("Spark analytics tables are not available yet.")
        return

    if not summary.empty:
        st.subheader("Spark summary")
        st.dataframe(summary, use_container_width=True, hide_index=True)

    spark_col_1, spark_col_2 = st.columns(2)
    with spark_col_1:
        if not by_cuisine.empty and "CUISINE_STYLE" in by_cuisine and "RECIPE_COUNT" in by_cuisine:
            st.subheader("Spark by cuisine")
            st.bar_chart(by_cuisine.head(15), x="CUISINE_STYLE", y="RECIPE_COUNT", use_container_width=True)
        if not by_language.empty and "RECIPE_LANGUAGE" in by_language and "RECIPE_COUNT" in by_language:
            st.subheader("Spark by language")
            st.bar_chart(by_language.head(15), x="RECIPE_LANGUAGE", y="RECIPE_COUNT", use_container_width=True)

    with spark_col_2:
        if not by_ingredient.empty and "MAIN_INGREDIENT" in by_ingredient and "RECIPE_COUNT" in by_ingredient:
            st.subheader("Spark by ingredient")
            st.bar_chart(by_ingredient.head(15), x="MAIN_INGREDIENT", y="RECIPE_COUNT", use_container_width=True)
        if not by_model.empty and "MODEL_NAME" in by_model and "RECIPE_COUNT" in by_model:
            st.subheader("Spark by model")
            st.bar_chart(by_model.head(15), x="MODEL_NAME", y="RECIPE_COUNT", use_container_width=True)


def render_quality(df: pd.DataFrame, filtered: pd.DataFrame) -> None:
    layer_counts = load_layer_counts()
    if not layer_counts.empty:
        st.subheader("Warehouse layer counts")
        st.dataframe(layer_counts, use_container_width=True, hide_index=True)

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
    st.subheader("Records to review")
    st.dataframe(
        low_confidence[
            [
                "RAW_ID",
                "DISPLAY_TITLE",
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


def main() -> None:
    inject_styles()

    st.title("TikTok Recipe Intelligence")
    st.caption("Recipe content analytics powered by Snowflake, OpenRouter, dbt, Spark, Airflow, FastAPI, and Streamlit.")

    try:
        df = load_catalog()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Unable to load data from Snowflake: {exc}")
        st.info("Run dbt from the `dbt_project/` directory before opening the app.")
        st.stop()

    if df.empty:
        st.info("No recipes available yet.")
        st.stop()

    df["PROCESSED_AT"] = pd.to_datetime(df["PROCESSED_AT"], errors="coerce")
    df["PROCESSING_CONFIDENCE"] = pd.to_numeric(df["PROCESSING_CONFIDENCE"], errors="coerce")
    df["MODEL_NAME"] = df.get("MODEL_NAME", pd.Series(["unknown"] * len(df))).fillna("unknown")

    filtered = apply_filters(df)
    render_metrics(df, filtered)

    overview_tab, catalog_tab, spark_tab, quality_tab = st.tabs(
        ["Overview", "Catalog", "Spark analytics", "Data quality"]
    )
    with overview_tab:
        render_overview(filtered)
    with catalog_tab:
        render_catalog(filtered)
    with spark_tab:
        render_spark_analytics()
    with quality_tab:
        render_quality(df, filtered)


if __name__ == "__main__":
    main()
