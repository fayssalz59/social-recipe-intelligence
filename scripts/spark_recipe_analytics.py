from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env", override=True)


def get_snowflake_options() -> dict[str, str]:
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "PORTFOLIO_WH")
    database = os.getenv("SNOWFLAKE_DB", "TIKTOK_PORTFOLIO_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA_SILVER", "SILVER")
    role = os.getenv("SNOWFLAKE_ROLE", "agent_role")

    missing = [
        key
        for key, value in {
            "SNOWFLAKE_ACCOUNT": account,
            "SNOWFLAKE_USER": user,
            "SNOWFLAKE_PASSWORD": password,
        }.items()
        if not value
    ]
    if missing:
        raise ValueError(f"Missing Snowflake env vars: {missing}")

    return {
        "sfURL": f"{account}.snowflakecomputing.com",
        "sfUser": user,
        "sfPassword": password,
        "sfWarehouse": warehouse,
        "sfDatabase": database,
        "sfSchema": schema,
        "sfRole": role,
    }


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("spark-recipe-analytics").getOrCreate()


def read_silver_table(spark: SparkSession, sf_options: dict[str, str]) -> DataFrame:
    return (
        spark.read
        .format("snowflake")
        .options(**sf_options)
        .option("dbtable", "SILVER_TIKTOK_RECIPES")
        .load()
    )


def write_to_snowflake(
    df: DataFrame,
    sf_options: dict[str, str],
    target_schema: str,
    target_table: str,
    mode: str = "overwrite",
) -> None:
    writer_options = dict(sf_options)
    writer_options["sfSchema"] = target_schema

    (
        df.write
        .format("snowflake")
        .options(**writer_options)
        .option("dbtable", target_table)
        .mode(mode)
        .save()
    )


def build_summary(df: DataFrame) -> DataFrame:
    return df.agg(
        F.count("*").alias("TOTAL_RECIPES"),
        F.sum(F.when(F.col("IS_VEGETARIAN") == True, 1).otherwise(0)).alias("VEGETARIAN_RECIPES"),
        F.avg(F.col("PROCESSING_CONFIDENCE")).alias("AVG_PROCESSING_CONFIDENCE"),
        F.countDistinct("RECIPE_LANGUAGE").alias("DISTINCT_LANGUAGES"),
        F.countDistinct("CUISINE_STYLE").alias("DISTINCT_CUISINES"),
        F.countDistinct("MAIN_INGREDIENT").alias("DISTINCT_MAIN_INGREDIENTS"),
        F.countDistinct("MODEL_NAME").alias("DISTINCT_MODELS"),
    ).withColumn("GENERATED_AT", F.current_timestamp())


def build_by_cuisine(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("CUISINE_STYLE")
        .agg(
            F.count("*").alias("RECIPE_COUNT"),
            F.sum(F.when(F.col("IS_VEGETARIAN") == True, 1).otherwise(0)).alias("VEGETARIAN_COUNT"),
            F.avg(F.col("PROCESSING_CONFIDENCE")).alias("AVG_PROCESSING_CONFIDENCE"),
            F.countDistinct("RECIPE_LANGUAGE").alias("DISTINCT_LANGUAGES"),
        )
        .withColumn("GENERATED_AT", F.current_timestamp())
        .orderBy(F.desc("RECIPE_COUNT"))
    )


def build_by_ingredient(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("MAIN_INGREDIENT")
        .agg(
            F.count("*").alias("RECIPE_COUNT"),
            F.sum(F.when(F.col("IS_VEGETARIAN") == True, 1).otherwise(0)).alias("VEGETARIAN_COUNT"),
            F.avg(F.col("PROCESSING_CONFIDENCE")).alias("AVG_PROCESSING_CONFIDENCE"),
            F.countDistinct("RECIPE_LANGUAGE").alias("DISTINCT_LANGUAGES"),
        )
        .withColumn("GENERATED_AT", F.current_timestamp())
        .orderBy(F.desc("RECIPE_COUNT"))
    )


def build_by_language(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("RECIPE_LANGUAGE")
        .agg(
            F.count("*").alias("RECIPE_COUNT"),
            F.sum(F.when(F.col("IS_VEGETARIAN") == True, 1).otherwise(0)).alias("VEGETARIAN_COUNT"),
            F.avg(F.col("PROCESSING_CONFIDENCE")).alias("AVG_PROCESSING_CONFIDENCE"),
            F.countDistinct("CUISINE_STYLE").alias("DISTINCT_CUISINES"),
            F.countDistinct("MAIN_INGREDIENT").alias("DISTINCT_MAIN_INGREDIENTS"),
        )
        .withColumn("GENERATED_AT", F.current_timestamp())
        .orderBy(F.desc("RECIPE_COUNT"))
    )


def build_by_model(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("MODEL_NAME")
        .agg(
            F.count("*").alias("RECIPE_COUNT"),
            F.avg(F.col("PROCESSING_CONFIDENCE")).alias("AVG_PROCESSING_CONFIDENCE"),
            F.countDistinct("RECIPE_LANGUAGE").alias("DISTINCT_LANGUAGES"),
            F.countDistinct("CUISINE_STYLE").alias("DISTINCT_CUISINES"),
        )
        .withColumn("GENERATED_AT", F.current_timestamp())
        .orderBy(F.desc("RECIPE_COUNT"))
    )


def main() -> None:
    spark = build_spark_session()
    sf_options = get_snowflake_options()

    df = read_silver_table(spark, sf_options).cache()

    summary_df = build_summary(df)
    by_cuisine_df = build_by_cuisine(df)
    by_ingredient_df = build_by_ingredient(df)
    by_language_df = build_by_language(df)
    by_model_df = build_by_model(df)

    target_schema = os.getenv("SNOWFLAKE_SCHEMA_GOLD", "GOLD")

    write_to_snowflake(summary_df, sf_options, target_schema, "RECIPE_ANALYTICS_SUMMARY")
    write_to_snowflake(by_cuisine_df, sf_options, target_schema, "RECIPE_ANALYTICS_BY_CUISINE")
    write_to_snowflake(by_ingredient_df, sf_options, target_schema, "RECIPE_ANALYTICS_BY_INGREDIENT")
    write_to_snowflake(by_language_df, sf_options, target_schema, "RECIPE_ANALYTICS_BY_LANGUAGE")
    write_to_snowflake(by_model_df, sf_options, target_schema, "RECIPE_ANALYTICS_BY_MODEL")

    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()