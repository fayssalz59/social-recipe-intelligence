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


def copy_stage_to_load_table(cursor, stage_name: str, load_table: str) -> None:
    LOGGER.info("Copying staged files into load table %s", load_table)
    copy_sql = f"""
    COPY INTO {load_table} (TITLE, DESCRIPTION, URL_TIKTOK, SOURCE_FILE)
    FROM (
        SELECT
            $1::STRING AS TITLE,
            $2::STRING AS DESCRIPTION,
            $3::STRING AS URL_TIKTOK,
            METADATA$FILENAME::STRING AS SOURCE_FILE
        FROM {stage_name}
    )
    FILE_FORMAT = (FORMAT_NAME = 'BRONZE.CSV_TIKTOK_FORMAT')
    ON_ERROR = 'CONTINUE';
    """
    cursor.execute(copy_sql)


def merge_load_into_bronze(cursor, load_table: str, bronze_table: str) -> None:
    LOGGER.info("Merging load table into bronze target %s", bronze_table)

    merge_sql = f"""
    MERGE INTO {bronze_table} AS tgt
    USING (
        SELECT
            TITLE,
            DESCRIPTION,
            URL_TIKTOK,
            SOURCE_FILE,
            SHA2(
                COALESCE(TRIM(TITLE), '') || '|' ||
                COALESCE(TRIM(DESCRIPTION), '') || '|' ||
                COALESCE(TRIM(URL_TIKTOK), ''),
                256
            ) AS RECORD_HASH
        FROM {load_table}
        WHERE URL_TIKTOK IS NOT NULL
    ) AS src
    ON tgt.RECORD_HASH = src.RECORD_HASH
    WHEN NOT MATCHED THEN INSERT (
        TITLE,
        DESCRIPTION,
        URL_TIKTOK,
        SOURCE_FILE,
        RECORD_HASH
    ) VALUES (
        src.TITLE,
        src.DESCRIPTION,
        src.URL_TIKTOK,
        src.SOURCE_FILE,
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

    stage_name = f"@{database}.{bronze_schema}.TIKTOK_CSV_STAGE"
    load_table = f"{database}.{bronze_schema}.BRONZE_TIKTOK_RECIPES_LOAD"
    bronze_table = f"{database}.{bronze_schema}.BRONZE_TIKTOK_RECIPES"

    LOGGER.info("Found %s file(s) to ingest", len(files))

    with get_snowflake_connection(schema=bronze_schema) as conn:
        with conn.cursor() as cursor:
            clear_stage(cursor, stage_name)
            truncate_load_table(cursor, load_table)
            upload_files_to_stage(cursor, files, stage_name)
            copy_stage_to_load_table(cursor, stage_name, load_table)
            merge_load_into_bronze(cursor, load_table, bronze_table)

            if not args.keep_stage_files:
                clear_stage(cursor, stage_name)

        conn.commit()

    LOGGER.info("Bronze ingestion completed successfully.")


if __name__ == "__main__":
    main()