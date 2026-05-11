"""Streamlit application for the TikTok recipe catalog."""
from __future__ import annotations

import pandas as pd
import streamlit as st

st.set_page_config(page_title="TikTok Recipe Intelligence", layout="wide")
st.title("🍜 TikTok Recipe Intelligence")
st.caption("Explore enriched TikTok recipe videos stored in Snowflake")

@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    conn = st.connection("snowflake")
    query = """
    SELECT
        DISPLAY_TITLE,
        URL_TIKTOK,
        RECIPE_LANGUAGE,
        IS_VEGETARIAN,
        CUISINE_STYLE,
        MAIN_INGREDIENT,
        PROCESSING_CONFIDENCE,
        PROCESSED_AT
    FROM GOLD.GOLD_STREAMLIT_RECIPE_CATALOG
    ORDER BY PROCESSED_AT DESC
    """
    return conn.query(query, ttl=300)

try:
    df = load_data()
except Exception as exc:  # noqa: BLE001
    st.error(f"Unable to load data from Snowflake: {exc}")
    st.info("Run `dbt run --profiles-dir .` inside `dbt_project/` before opening the app.")
    st.stop()

if df.empty:
    st.info("No recipes available yet. Load Bronze data and run the enrichment script first.")
    st.stop()

cuisine_options = sorted(df["CUISINE_STYLE"].dropna().unique().tolist())
selected_cuisines = st.sidebar.multiselect("Cuisine style", options=cuisine_options, default=cuisine_options)
vegetarian_only = st.sidebar.toggle("Vegetarian only", value=False)

filtered_df = df[df["CUISINE_STYLE"].isin(selected_cuisines)]
if vegetarian_only:
    filtered_df = filtered_df[filtered_df["IS_VEGETARIAN"] == True]  # noqa: E712

total_recipes = len(df)
veg_pct = round((df["IS_VEGETARIAN"].fillna(False).mean() * 100), 1)

metric_col1, metric_col2 = st.columns(2)
metric_col1.metric("Total recipes indexed", f"{total_recipes}")
metric_col2.metric("Vegetarian recipes", f"{veg_pct}%")

st.subheader("Recipe catalog")
for _, row in filtered_df.iterrows():
    with st.container(border=True):
        title_col, action_col = st.columns([6, 1])
        title_col.markdown(f"### {row['DISPLAY_TITLE']}")
        action_col.link_button("Play on TikTok", row["URL_TIKTOK"])
        st.write(
            f"**Cuisine:** {row['CUISINE_STYLE']}  \n**Ingredient:** {row['MAIN_INGREDIENT']}  \n**Language:** {row['RECIPE_LANGUAGE']}  \n**Vegetarian:** {row['IS_VEGETARIAN']}  \n**Confidence:** {row['PROCESSING_CONFIDENCE']}"
        )

st.subheader("Tabular view")
st.dataframe(filtered_df, use_container_width=True, hide_index=True)