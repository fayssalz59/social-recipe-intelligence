from __future__ import annotations

import argparse
import hashlib
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from scripts.common import get_snowflake_connection


LOGGER = logging.getLogger("load_bronze")


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload CSV files to Snowflake stage and merge into Bronze table."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing CSV files to ingest.",
    )
    parser.add_argument(
        "--pattern",
        default="*.csv",
        help="Glob pattern for files inside input-dir. Default: *.csv",
    )
    parser.add_argument(
        "--keep-stage-files",
        action="store_true",
        help="Do not remove files from Snowflake stage after load.",
    )
    return parser.parse_args()


def list_csv_files(input_dir: Path, pattern: str) -> list[Path]:
    files = sorted([p for p in input_dir.glob(pattern) if p.is_file()])
    if not files:
        raise FileNotFoundError(
            f"No files matching pattern '{pattern}' found in {input_dir}"
        )
    return files


def escape_path_for_put(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/")


def upload_files_to_stage(cursor, files: list[Path], stage_name: str) -> None:
    for file_path in files:
        abs_path = escape_path_for_put(file_path)
        put_sql = (
            f"PUT 'file://{abs_path}' {stage_name} "
            "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )
        LOGGER.info("Uploading file to stage: %s", file_path.name)
        cursor.execute(put_sql)


def clear_stage(cursor, stage_name: str) -> None:
    LOGGER.info("Removing existing files from stage %s", stage_name)
    cursor.execute(f"REMOVE {stage_name}")


def truncate_load_table(cursor, load_table: str) -> None:
    LOGGER.info("Truncating load table %s", load_table)
    cursor.execute(f"TRUNCATE TABLE {load_table}")


def ensure_load_objects(
    cursor,
    database: str,
    bronze_schema: str,
    stage_object: str,
    load_table: str,
) -> None:
    """Create the transient CSV landing objects used by the Bronze loader."""
    cursor.execute(
        f"""
        CREATE OR REPLACE FILE FORMAT {database}.{bronze_schema}.CSV_TIKTOK_FORMAT
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            TRIM_SPACE = TRUE
            NULL_IF = ('', 'NULL', 'null')
            EMPTY_FIELD_AS_NULL = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
        """
    )
    cursor.execute(
        f"""
        CREATE STAGE IF NOT EXISTS {stage_object}
            FILE_FORMAT = {database}.{bronze_schema}.CSV_TIKTOK_FORMAT;
        """
    )
    cursor.execute(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE {load_table} (
            TITLE STRING,
            DESCRIPTION STRING,
            URL_TIKTOK STRING,
            PLATFORM STRING,
            CONTENT_ID STRING,
            CREATOR_USERNAME STRING,
            SOURCE_PLATFORM_URL STRING,
            RECIPE_LANGUAGE_HINT STRING,
            CUISINE_HINT STRING,
            MAIN_INGREDIENT_HINT STRING,
            DESCRIPTION_IS_PARTIAL BOOLEAN,
            DATA_ORIGIN STRING,
            VERIFICATION_SOURCE_URL STRING,
            DESCRIPTION_SOURCE STRING,
            DESCRIPTION_LENGTH STRING,
            DESCRIPTION_ENRICHED STRING,
            ORIGINAL_DESCRIPTION STRING,
            RECOVERED_TEXT STRING,
            EVIDENCE_TEXT STRING,
            SOURCE_FILE STRING
        );
        """
    )


def copy_stage_to_load_table(
    cursor,
    stage_name: str,
    load_table: str,
    file_format_name: str,
) -> None:
    LOGGER.info("Copying staged files into load table %s", load_table)
    copy_sql = f"""
    COPY INTO {load_table} (
        TITLE,
        DESCRIPTION,
        URL_TIKTOK,
        PLATFORM,
        CONTENT_ID,
        CREATOR_USERNAME,
        SOURCE_PLATFORM_URL,
        RECIPE_LANGUAGE_HINT,
        CUISINE_HINT,
        MAIN_INGREDIENT_HINT,
        DESCRIPTION_IS_PARTIAL,
        DATA_ORIGIN,
        VERIFICATION_SOURCE_URL,
        DESCRIPTION_SOURCE,
        DESCRIPTION_LENGTH,
        DESCRIPTION_ENRICHED,
        ORIGINAL_DESCRIPTION,
        RECOVERED_TEXT,
        EVIDENCE_TEXT,
        SOURCE_FILE
    )
    FROM (
        SELECT
            $1::STRING AS TITLE,
            $2::STRING AS DESCRIPTION,
            $3::STRING AS URL_TIKTOK,
            $4::STRING AS PLATFORM,
            $5::STRING AS CONTENT_ID,
            $6::STRING AS CREATOR_USERNAME,
            $7::STRING AS SOURCE_PLATFORM_URL,
            $8::STRING AS RECIPE_LANGUAGE_HINT,
            $9::STRING AS CUISINE_HINT,
            $10::STRING AS MAIN_INGREDIENT_HINT,
            TRY_TO_BOOLEAN($11::STRING) AS DESCRIPTION_IS_PARTIAL,
            $12::STRING AS DATA_ORIGIN,
            $13::STRING AS VERIFICATION_SOURCE_URL,
            $14::STRING AS DESCRIPTION_SOURCE,
            $15::STRING AS DESCRIPTION_LENGTH,
            $16::STRING AS DESCRIPTION_ENRICHED,
            $17::STRING AS ORIGINAL_DESCRIPTION,
            $18::STRING AS RECOVERED_TEXT,
            $19::STRING AS EVIDENCE_TEXT,
            METADATA$FILENAME::STRING AS SOURCE_FILE
        FROM {stage_name}
    )
    FILE_FORMAT = (FORMAT_NAME = '{file_format_name}')
    ON_ERROR = 'CONTINUE';
    """
    cursor.execute(copy_sql)


def merge_load_into_bronze(cursor, load_table: str, bronze_table: str) -> None:
    LOGGER.info("Merging load table into bronze target %s", bronze_table)

    # Important:
    # Snowflake MERGE fails if multiple source rows match the same target row.
    # This can happen even after basic CONTENT_ID deduplication when one row has
    # CONTENT_ID filled and another row has the same TikTok video only in URL_TIKTOK.
    # We therefore normalize both source and target around a single CONTENT_KEY:
    # 1. CONTENT_ID when present
    # 2. video id extracted from URL_TIKTOK
    # 3. URL_TIKTOK as final fallback
    duplicate_check_sql = f"""
    WITH normalized AS (
        SELECT
            COALESCE(NULLIF(TRIM(PLATFORM), ''), 'tiktok') AS PLATFORM,
            COALESCE(
                NULLIF(TRIM(CONTENT_ID), ''),
                NULLIF(REGEXP_SUBSTR(URL_TIKTOK, '/video/([0-9]+)', 1, 1, 'e', 1), ''),
                NULLIF(TRIM(URL_TIKTOK), '')
            ) AS CONTENT_KEY
        FROM {load_table}
        WHERE URL_TIKTOK IS NOT NULL
    )
    SELECT COUNT(*) AS DUPLICATE_BUSINESS_KEYS
    FROM (
        SELECT PLATFORM, CONTENT_KEY
        FROM normalized
        WHERE CONTENT_KEY IS NOT NULL
        GROUP BY PLATFORM, CONTENT_KEY
        HAVING COUNT(*) > 1
    );
    """
    cursor.execute(duplicate_check_sql)
    duplicate_count = cursor.fetchone()[0]
    if duplicate_count:
        LOGGER.warning(
            "Detected %s duplicate TikTok business key(s) in the load table. "
            "The loader will keep the best row per key before MERGE.",
            duplicate_count,
        )

    merge_sql = f"""
    MERGE INTO {bronze_table} AS tgt
    USING (
        WITH normalized_source AS (
            SELECT
                COALESCE(NULLIF(TRIM(PLATFORM), ''), 'tiktok') AS PLATFORM,

                COALESCE(
                    NULLIF(TRIM(CONTENT_ID), ''),
                    NULLIF(REGEXP_SUBSTR(URL_TIKTOK, '/video/([0-9]+)', 1, 1, 'e', 1), '')
                ) AS CONTENT_ID,

                NULLIF(TRIM(CREATOR_USERNAME), '') AS CREATOR_USERNAME,
                NULLIF(TRIM(TITLE), '') AS TITLE,
                NULLIF(TRIM(DESCRIPTION), '') AS DESCRIPTION,
                NULLIF(TRIM(URL_TIKTOK), '') AS URL_TIKTOK,
                COALESCE(DESCRIPTION_IS_PARTIAL, FALSE) AS DESCRIPTION_IS_PARTIAL,
                SOURCE_FILE,
                ORIGINAL_DESCRIPTION,
                RECOVERED_TEXT,
                EVIDENCE_TEXT,
                SOURCE_PLATFORM_URL,
                RECIPE_LANGUAGE_HINT,
                CUISINE_HINT,
                MAIN_INGREDIENT_HINT,
                DATA_ORIGIN,
                VERIFICATION_SOURCE_URL,
                DESCRIPTION_SOURCE,
                DESCRIPTION_LENGTH,
                DESCRIPTION_ENRICHED,

                COALESCE(
                    NULLIF(TRIM(CONTENT_ID), ''),
                    NULLIF(REGEXP_SUBSTR(URL_TIKTOK, '/video/([0-9]+)', 1, 1, 'e', 1), ''),
                    NULLIF(TRIM(URL_TIKTOK), '')
                ) AS CONTENT_KEY

            FROM {load_table}
            WHERE URL_TIKTOK IS NOT NULL
        ),

        ranked_source AS (
            SELECT
                PLATFORM,
                CONTENT_ID,
                CREATOR_USERNAME,
                TITLE,
                DESCRIPTION,
                URL_TIKTOK,
                DESCRIPTION_IS_PARTIAL,
                SOURCE_FILE,
                OBJECT_CONSTRUCT_KEEP_NULL(
                    'platform', PLATFORM,
                    'content_id', CONTENT_ID,
                    'creator_username', CREATOR_USERNAME,
                    'original_description', ORIGINAL_DESCRIPTION,
                    'recovered_text', RECOVERED_TEXT,
                    'evidence_text', EVIDENCE_TEXT,
                    'source_platform_url', SOURCE_PLATFORM_URL,
                    'recipe_language_hint', RECIPE_LANGUAGE_HINT,
                    'cuisine_hint', CUISINE_HINT,
                    'main_ingredient_hint', MAIN_INGREDIENT_HINT,
                    'description_is_partial', DESCRIPTION_IS_PARTIAL,
                    'data_origin', DATA_ORIGIN,
                    'verification_source_url', VERIFICATION_SOURCE_URL,
                    'description_source', DESCRIPTION_SOURCE,
                    'description_length', DESCRIPTION_LENGTH,
                    'description_enriched', DESCRIPTION_ENRICHED
                ) AS RAW_PAYLOAD,
                SHA2(
                    COALESCE(PLATFORM, 'tiktok') || '|' ||
                    COALESCE(CONTENT_KEY, '') || '|' ||
                    COALESCE(DESCRIPTION, '') || '|' ||
                    COALESCE(TITLE, '') || '|' ||
                    COALESCE(URL_TIKTOK, ''),
                    256
                ) AS RECORD_HASH,
                ROW_NUMBER() OVER (
                    PARTITION BY PLATFORM, CONTENT_KEY
                    ORDER BY
                        LENGTH(COALESCE(EVIDENCE_TEXT, '')) DESC,
                        LENGTH(COALESCE(DESCRIPTION, '')) DESC,
                        IFF(TRY_TO_BOOLEAN(DESCRIPTION_ENRICHED), 1, 0) DESC,
                        SOURCE_FILE DESC
                ) AS ROW_NUM
            FROM normalized_source
            WHERE CONTENT_KEY IS NOT NULL
        )

        SELECT
            PLATFORM,
            CONTENT_ID,
            CREATOR_USERNAME,
            TITLE,
            DESCRIPTION,
            URL_TIKTOK,
            DESCRIPTION_IS_PARTIAL,
            SOURCE_FILE,
            RAW_PAYLOAD,
            RECORD_HASH
        FROM ranked_source
        WHERE ROW_NUM = 1
    ) AS src
    ON (
        COALESCE(tgt.PLATFORM, 'tiktok') = src.PLATFORM
        AND COALESCE(
            NULLIF(TRIM(tgt.CONTENT_ID), ''),
            NULLIF(REGEXP_SUBSTR(tgt.URL_TIKTOK, '/video/([0-9]+)', 1, 1, 'e', 1), ''),
            NULLIF(TRIM(tgt.URL_TIKTOK), '')
        ) = COALESCE(
            NULLIF(TRIM(src.CONTENT_ID), ''),
            NULLIF(REGEXP_SUBSTR(src.URL_TIKTOK, '/video/([0-9]+)', 1, 1, 'e', 1), ''),
            NULLIF(TRIM(src.URL_TIKTOK), '')
        )
    )
    WHEN MATCHED THEN UPDATE SET
        PLATFORM = src.PLATFORM,
        CONTENT_ID = COALESCE(NULLIF(tgt.CONTENT_ID, ''), src.CONTENT_ID),
        CREATOR_USERNAME = COALESCE(NULLIF(tgt.CREATOR_USERNAME, ''), src.CREATOR_USERNAME),
        TITLE = CASE
            WHEN LENGTH(COALESCE(src.TITLE, '')) > LENGTH(COALESCE(tgt.TITLE, '')) THEN src.TITLE
            ELSE tgt.TITLE
        END,
        DESCRIPTION = CASE
            WHEN LENGTH(COALESCE(src.DESCRIPTION, '')) > LENGTH(COALESCE(tgt.DESCRIPTION, '')) THEN src.DESCRIPTION
            ELSE tgt.DESCRIPTION
        END,
        DESCRIPTION_IS_PARTIAL = src.DESCRIPTION_IS_PARTIAL,
        URL_TIKTOK = COALESCE(tgt.URL_TIKTOK, src.URL_TIKTOK),
        SOURCE_FILE = src.SOURCE_FILE,
        RAW_PAYLOAD = src.RAW_PAYLOAD,
        RECORD_HASH = src.RECORD_HASH
    WHEN NOT MATCHED THEN INSERT (
        PLATFORM,
        CONTENT_ID,
        CREATOR_USERNAME,
        TITLE,
        DESCRIPTION,
        DESCRIPTION_IS_PARTIAL,
        URL_TIKTOK,
        SOURCE_FILE,
        RAW_PAYLOAD,
        RECORD_HASH
    ) VALUES (
        src.PLATFORM,
        src.CONTENT_ID,
        src.CREATOR_USERNAME,
        src.TITLE,
        src.DESCRIPTION,
        src.DESCRIPTION_IS_PARTIAL,
        src.URL_TIKTOK,
        src.SOURCE_FILE,
        src.RAW_PAYLOAD,
        src.RECORD_HASH
    );
    """
    cursor.execute(merge_sql)

def main() -> None:
    load_dotenv()
    configure_logging()

    args = parse_args()
    input_dir = Path(args.input_dir)

    files = list_csv_files(input_dir, args.pattern)

    database = os.getenv("SNOWFLAKE_DB", "TIKTOK_PORTFOLIO_DB")
    bronze_schema = os.getenv("SNOWFLAKE_SCHEMA_BRONZE", "BRONZE")

    stage_object = f"{database}.{bronze_schema}.TIKTOK_CSV_STAGE"
    stage_name = f"@{stage_object}"
    file_format_name = f"{database}.{bronze_schema}.CSV_TIKTOK_FORMAT"
    load_table = f"{database}.{bronze_schema}.BRONZE_TIKTOK_RECIPES_LOAD"
    bronze_table = f"{database}.{bronze_schema}.BRONZE_TIKTOK_RECIPES"

    LOGGER.info("Found %s file(s) to ingest", len(files))

    with get_snowflake_connection(schema=bronze_schema) as conn:
        with conn.cursor() as cursor:
            ensure_load_objects(cursor, database, bronze_schema, stage_object, load_table)
            clear_stage(cursor, stage_name)
            truncate_load_table(cursor, load_table)
            upload_files_to_stage(cursor, files, stage_name)
            copy_stage_to_load_table(cursor, stage_name, load_table, file_format_name)
            merge_load_into_bronze(cursor, load_table, bronze_table)

            if not args.keep_stage_files:
                clear_stage(cursor, stage_name)

        conn.commit()

    LOGGER.info("Bronze ingestion completed successfully.")


if __name__ == "__main__":
    main()
