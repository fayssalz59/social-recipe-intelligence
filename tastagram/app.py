from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional, Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from tastagram.api_client import get_filters, get_recipe, get_recipes

app = FastAPI(
    title="Tastagram",
    description="A recipe website powered by social video data and AI extraction.",
)

LANGUAGE_LABELS = {
    "en": "English",
    "fr": "French",
    "it": "Italian",
    "es": "Spanish",
    "pt": "Portuguese",
    "ar": "Arabic",
}


def _parse_json_field(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _coerce_list_field(value: Any) -> list[Any]:
    value = _parse_json_field(value)
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, dict):
        for key in ("ingredients", "items", "values"):
            nested = value.get(key)
            if nested:
                return _coerce_list_field(nested)
        return [value]
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() in {"null", "none", "unknown", "[]"}:
            return []
        parsed = _parse_json_field(text)
        if parsed is not value:
            return _coerce_list_field(parsed)
        if "\n" in text:
            return [part.strip(" -\t") for part in text.splitlines() if part.strip(" -\t")]
        return [part.strip() for part in text.split(",") if part.strip()]
    return [value]


def _normalize_ingredients(final_json: Any, recipe: dict[str, Any]) -> list[str]:
    final_ingredients = []
    if isinstance(final_json, dict):
        final_ingredients = _coerce_list_field(final_json.get("ingredients"))
    fallback_ingredients = _coerce_list_field(recipe.get("INGREDIENTS"))
    candidates = final_ingredients or fallback_ingredients

    output: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        formatted = _format_ingredient(item)
        normalized = formatted.strip().strip(",")
        if not normalized or normalized in {"[", "]", "{", "}"}:
            continue
        if normalized.lower() in {"unknown", "none", "null", "ingredient"}:
            continue
        key = _ingredient_key(normalized)
        if _is_weaker_duplicate(normalized, key, output):
            continue
        output = [
            existing
            for existing in output
            if not _is_weaker_duplicate(existing, _ingredient_key(existing), [normalized])
        ]
        seen = {_ingredient_key(existing) for existing in output}
        if key not in seen:
            output.append(normalized)
            seen.add(key)
    return output


def _ingredient_key(value: str) -> str:
    text = re.sub(r"\([^)]*\)", "", value.lower())
    text = re.sub(r"\b\d+([.,]\d+)?\b", "", text)
    text = re.sub(
        r"\b(g|kg|ml|cl|l|tbsp|tsp|cup|cups|cuil|cuillere|cuillère|soupe|sachet|pincée|pincee|de|d'|du|des|fresh|frais|fondu|fondue|tiède|tiede)\b",
        " ",
        text,
    )
    text = re.sub(r"[^a-zà-ÿ0-9]+", " ", text).strip()
    return text or value.lower().strip()


def _has_quantity(value: str) -> bool:
    return bool(re.search(r"\d", value))


def _is_weaker_duplicate(value: str, key: str, existing_values: list[str]) -> bool:
    if not key:
        return False
    for existing in existing_values:
        existing_key = _ingredient_key(existing)
        same_meaning = key == existing_key or key in existing_key or existing_key in key
        if same_meaning and _has_quantity(existing) and not _has_quantity(value):
            return True
    return False


def _format_ingredient(ingredient: Any) -> str:
    ingredient = _parse_json_field(ingredient)
    if isinstance(ingredient, dict):
        name = ingredient.get("name") or ingredient.get("ingredient") or ingredient.get("text") or "ingredient"
        quantity = ingredient.get("quantity")
        unit = ingredient.get("unit")
        notes = ingredient.get("notes") or ingredient.get("preparation") or ingredient.get("comment")
        primary = ""
        if quantity and unit:
            primary = f"{quantity} {unit} {name}"
        elif quantity:
            primary = f"{quantity} {name}"
        else:
            primary = name
        if notes:
            return f"{primary} ({notes})"
        return primary
    text = str(ingredient).strip()
    return text.strip("[]{}'\" ")


def _format_step(step: Any) -> str:
    if isinstance(step, dict):
        return str(step.get("instruction") or step.get("text") or step.get("step") or next(iter(step.values()), ""))
    return str(step)


def _clean_recipe_text(value: Any) -> str:
    if not value:
        return ""
    lines = []
    for raw_line in str(value).splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = re.sub(r"^#{1,6}\s*", "", line).strip()
        line = re.sub(r"^\*\*(.*?)\*\*:?\s*$", r"\1", line).strip()
        line = re.sub(r"^\*\*(.*?)\*\*", r"\1", line).strip()
        line = re.sub(r"^[-*]\s+", "", line).strip()
        label = line.lower().strip(":").strip()
        if label in {"ingredients", "ingredient", "steps", "instructions", "preparation", "method", "directions"}:
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def _recipe_intro(recipe: dict[str, Any]) -> str:
    text = _clean_recipe_text(recipe.get("FINAL_RECIPE_TEXT"))
    if not text:
        return ""
    title = str(recipe.get("FINAL_RECIPE_TITLE") or recipe.get("TITLE") or "").strip().lower()
    for line in text.splitlines():
        clean_line = line.strip()
        if title and clean_line.lower() == title:
            continue
        label = clean_line.lower().strip(":").strip()
        if label in {"ingredients", "ingredient", "steps", "instructions", "preparation", "method", "directions"}:
            continue
        return clean_line
    return ""


def _language_label(value: Any) -> str:
    code = str(value or "").strip()
    return LANGUAGE_LABELS.get(code.lower(), code.upper() if len(code) <= 3 else code)


def _tiktok_video_id(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"/video/(\d+)", url)
    if match:
        return match.group(1)
    match = re.search(r"/(\d+)(?:\?|$)", url)
    if match:
        return match.group(1)
    return None


def _thumbnail_url(url: str | None) -> str | None:
    video_id = _tiktok_video_id(url)
    if not video_id:
        return None
    thumbnail_path = Path("tastagram/static/thumbnails") / f"{video_id}.jpg"
    if thumbnail_path.exists():
        return f"/static/thumbnails/{video_id}.jpg"
    return None


templates = Jinja2Templates(directory="tastagram/templates")
app.mount("/static", StaticFiles(directory="tastagram/static"), name="static")


@app.get("/health")
def health():
    return {"status": "ok", "app": "tastagram"}


@app.get("/", response_class=HTMLResponse)
def home(
    request: Request,
    q: Optional[str] = Query(default=None),
    language: Optional[str] = Query(default=None),
    cuisine_style: Optional[str] = Query(default=None),
    ingredient: Optional[str] = Query(default=None),
    is_vegetarian: Optional[bool] = Query(default=None),
):
    params = {
        "language": language,
        "cuisine_style": cuisine_style,
        "ingredient": ingredient,
        "is_vegetarian": is_vegetarian,
        "limit": 120,
    }
    params = {k: v for k, v in params.items() if v not in (None, "")}

    recipes = get_recipes(params)
    filters = get_filters()

    for recipe in recipes:
        final_json = _parse_json_field(recipe.get("FINAL_RECIPE_JSON") or {})
        recipe["preview_ingredients"] = _normalize_ingredients(final_json, recipe)[:4]
        recipe["thumbnail_url"] = _thumbnail_url(recipe.get("URL_TIKTOK"))
        recipe["preview_text"] = _recipe_intro(recipe)
        recipe["language_label"] = _language_label(recipe.get("LANGUAGE"))

    if q:
        q_lower = q.lower()
        recipes = [
            recipe
            for recipe in recipes
            if q_lower in str(recipe.get("TITLE", "")).lower()
            or q_lower in str(recipe.get("FINAL_RECIPE_TEXT", "")).lower()
            or q_lower in str(recipe.get("MAIN_INGREDIENT", "")).lower()
        ]

    filters["language_options"] = [
        {"value": item, "label": _language_label(item)}
        for item in filters.get("languages", [])
    ]

    stats = {
        "recipes": len(recipes),
        "languages": len({recipe.get("LANGUAGE") for recipe in recipes if recipe.get("LANGUAGE")}),
        "cuisines": len({recipe.get("CUISINE_STYLE") for recipe in recipes if recipe.get("CUISINE_STYLE")}),
        "with_thumbnails": sum(1 for recipe in recipes if recipe.get("thumbnail_url")),
    }

    selected = {
        "q": q or "",
        "language": language or "",
        "cuisine_style": cuisine_style or "",
        "ingredient": ingredient or "",
        "is_vegetarian": is_vegetarian,
    }

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request": request,
            "recipes": recipes,
            "filters": filters,
            "selected": selected,
            "stats": stats,
        },
    )


@app.get("/recipes/{recipe_id}", response_class=HTMLResponse)
def recipe_detail(request: Request, recipe_id: int):
    try:
        recipe = get_recipe(recipe_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Recipe not found") from exc

    final_json = _parse_json_field(recipe.get("FINAL_RECIPE_JSON") or {})
    if not isinstance(final_json, dict):
        final_json = {}

    ingredients = _normalize_ingredients(final_json, recipe)
    steps = final_json.get("steps") or []
    missing_info = final_json.get("missing_info") or []
    video_id = _tiktok_video_id(recipe.get("URL_TIKTOK"))

    steps = [_format_step(item) for item in steps]
    thumbnail_url = _thumbnail_url(recipe.get("URL_TIKTOK"))
    recipe_intro = _recipe_intro(recipe)
    recipe["language_label"] = _language_label(recipe.get("LANGUAGE"))

    return templates.TemplateResponse(
        request,
        "recipe_detail.html",
        {
            "request": request,
            "recipe": recipe,
            "ingredients": ingredients,
            "steps": steps,
            "missing_info": missing_info,
            "tiktok_video_id": video_id,
            "thumbnail_url": thumbnail_url,
            "recipe_intro": recipe_intro,
        },
    )
