"""Shared utilities for the TikTok recipe intelligence pipeline."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import snowflake.connector
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env", override=True)


def configure_logging(name: str) -> logging.Logger:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    snowflake_log_level = os.getenv("SNOWFLAKE_LOG_LEVEL", "WARNING").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    for logger_name in [
        "snowflake.connector",
        "snowflake.connector.connection",
        "snowflake.connector.cursor",
        "snowflake.connector.network",
    ]:
        logging.getLogger(logger_name).setLevel(snowflake_log_level)
    for noisy_logger_name in [
        "httpx",
        "httpcore",
        "faster_whisper",
        "easyocr",
        "easyocr.easyocr",
    ]:
        logging.getLogger(noisy_logger_name).setLevel(os.getenv("DEPENDENCY_LOG_LEVEL", "WARNING").upper())
    return logging.getLogger(name)


@dataclass(frozen=True)
class SnowflakeSettings:
    user: str
    password: str
    account: str
    warehouse: str
    database: str
    role: Optional[str]

    @classmethod
    def from_env(cls) -> "SnowflakeSettings":
        required = {
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DB"),
        }
        missing = [key for key, value in required.items() if not value]
        if missing:
            raise ValueError(f"Missing Snowflake environment variables: {', '.join(missing)}")
        return cls(
            user=required["user"],
            password=required["password"],
            account=required["account"],
            warehouse=required["warehouse"],
            database=required["database"],
            role=os.getenv("SNOWFLAKE_ROLE"),
        )


def get_snowflake_connection(schema: Optional[str] = None):
    settings = SnowflakeSettings.from_env()
    return snowflake.connector.connect(
        user=settings.user,
        password=settings.password,
        account=settings.account,
        warehouse=settings.warehouse,
        database=settings.database,
        schema=schema,
        role=settings.role,
        autocommit=False,
    )


def parse_json_strict(text: str) -> Dict[str, Any]:
    """Parse the first JSON object found in a model response."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        return json.loads(text[start : end + 1])
