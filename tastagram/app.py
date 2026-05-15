from __future__ import annotations

from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from tastagram.api_client import get_filters, get_recipe, get_recipes

app = FastAPI(
    title="Tastagram",
    description="A recipe website powered by social video data and AI extraction.",
)

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
    quality: Optional[str] = Query(default=None),
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

    if q:
        q_lower = q.lower()
        recipes = [
            recipe
            for recipe in recipes
            if q_lower in str(recipe.get("TITLE", "")).lower()
            or q_lower in str(recipe.get("FINAL_RECIPE_TEXT", "")).lower()
            or q_lower in str(recipe.get("MAIN_INGREDIENT", "")).lower()
        ]

    if quality:
        recipes = [
            recipe
            for recipe in recipes
            if str(recipe.get("RECIPE_QUALITY_GRADE", "")).upper() == quality.upper()
        ]

    selected = {
        "q": q or "",
        "language": language or "",
        "cuisine_style": cuisine_style or "",
        "ingredient": ingredient or "",
        "quality": quality or "",
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
        },
    )


@app.get("/recipes/{recipe_id}", response_class=HTMLResponse)
def recipe_detail(request: Request, recipe_id: int):
    try:
        recipe = get_recipe(recipe_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Recipe not found") from exc

    final_json = recipe.get("FINAL_RECIPE_JSON") or {}
    ingredients = final_json.get("ingredients") or recipe.get("INGREDIENTS") or []
    steps = final_json.get("steps") or []
    missing_info = final_json.get("missing_info") or []

    return templates.TemplateResponse(
        request,
        "recipe_detail.html",
        {
            "request": request,
            "recipe": recipe,
            "ingredients": ingredients,
            "steps": steps,
            "missing_info": missing_info,
        },
    )
