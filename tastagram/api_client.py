from __future__ import annotations

import os
from typing import Any

import requests

API_BASE_URL = os.getenv("TASTAGRAM_API_BASE_URL", "http://recipe-api:8000")


def _get_json(path: str, params: dict[str, Any] | None = None) -> Any:
    response = requests.get(
        f"{API_BASE_URL}{path}",
        params=params or {},
        timeout=25,
    )
    response.raise_for_status()
    return response.json()


def get_recipes(params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    data = _get_json("/recipes", params=params)
    if isinstance(data, list):
        return data
    return data.get("items", data.get("recipes", []))


def get_recipe(recipe_id: int) -> dict[str, Any]:
    return _get_json(f"/recipes/{recipe_id}")


def get_filters() -> dict[str, list[str]]:
    data = _get_json("/recipes/filters")
    return {
        "languages": data.get("languages", []),
        "cuisines": data.get("cuisines", []),
        "ingredients": data.get("ingredients", []),
        "qualities": ["A", "B", "C"],
    }
