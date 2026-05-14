"""Shared scoring helpers for social recipe evidence.

These functions are intentionally lightweight and deterministic. They are used
before LLM enrichment to decide whether a caption/OCR/transcript/comment is good
enough to become recipe evidence.
"""
from __future__ import annotations

import re
import unicodedata

KNOWN_HIGH_YIELD_CREATORS = {
    "thegoldenbalance",
    "cjeatsrecipes",
    "fitwaffle",
    "wishbonekitchen",
    "tinekeyounger",
    "the_pastaqueen",
    "louloukitchen",
    "hervecuisine",
    "marmiton_org",
    "kiwilimon",
    "chefjosera",
    "giallozafferano",
    "fattoincasadabenedetta",
    "cucinabotanica",
    "tastemadebr",
    "panelinha",
    "receiteria",
}

RECIPE_HASHTAGS = {
    "recipe",
    "recipes",
    "recipesoftiktok",
    "easyrecipe",
    "foodtok",
    "mealprep",
    "healthyrecipes",
    "dinnerideas",
    "recette",
    "recettesfaciles",
    "receta",
    "recetasfaciles",
    "ricetta",
    "ricettefacili",
    "receita",
    "receitasfaceis",
}

RECIPE_KEYWORDS = [
    "recipe",
    "how to make",
    "ingredients",
    "full recipe",
    "recipe below",
    "recette",
    "ingredients en description",
    "receta",
    "ingredientes",
    "ricetta",
    "ingredienti",
    "receita",
    "ingredientes",
    "\u0648\u0635\u0641\u0629",
    "\u0645\u0643\u0648\u0646\u0627\u062a",
]

INGREDIENT_KEYWORDS = [
    "flour",
    "sugar",
    "butter",
    "egg",
    "chicken",
    "garlic",
    "onion",
    "cheese",
    "pasta",
    "rice",
    "tomato",
    "huile",
    "beurre",
    "oeuf",
    "poulet",
    "farine",
    "azucar",
    "mantequilla",
    "pollo",
    "farina",
    "burro",
    "uovo",
    "frango",
    "farinha",
]

COOKING_VERBS = [
    "mix",
    "stir",
    "bake",
    "cook",
    "fry",
    "boil",
    "chop",
    "blend",
    "roast",
    "season",
    "add",
    "melt",
    "mijoter",
    "melanger",
    "ajouter",
    "cuire",
    "couper",
    "mezclar",
    "cocinar",
    "hornear",
    "freir",
    "aggiungere",
    "cuocere",
    "mescolare",
    "assar",
    "cozinhar",
    "misturar",
]

QUANTITY_PATTERNS = [
    re.compile(r"\b\d+(?:[.,]\d+)?\s?(g|kg|ml|l|tbsp|tsp|cup|cups|oz|lb|lbs|min|minutes|h|hour|hours)\b", re.I),
    re.compile(r"\b\d+\s?/\s?\d+\b"),
    re.compile(r"\b\d{2,3}\s?(c|f|°c|°f)\b", re.I),
]


def normalize_evidence_text(text: str | None) -> str:
    """Normalize whitespace and unicode without changing the language."""
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKC", str(text))
    normalized = normalized.replace("\r", "\n")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def _clean_hashtag(value: str) -> str:
    return value.strip().lower().lstrip("#")


def _contains_any(text: str, values: list[str]) -> bool:
    return any(value in text for value in values)


def _alpha_ratio(text: str) -> float:
    if not text:
        return 0.0
    return sum(char.isalpha() for char in text) / max(len(text), 1)


def compute_recipe_evidence_score(
    text: str,
    hashtags: list[str] | None = None,
    creator: str = "",
) -> float:
    """Score how useful a text fragment is for recipe extraction."""
    normalized = normalize_evidence_text(text)
    lower_text = normalized.lower()
    normalized_hashtags = {_clean_hashtag(hashtag) for hashtag in hashtags or [] if hashtag}

    score = 0.0
    if len(normalized) >= 120:
        score += 0.15
    elif len(normalized) >= 60:
        score += 0.08

    if _contains_any(lower_text, RECIPE_KEYWORDS):
        score += 0.20

    if _contains_any(lower_text, INGREDIENT_KEYWORDS):
        score += 0.15

    if any(pattern.search(lower_text) for pattern in QUANTITY_PATTERNS):
        score += 0.20

    if _contains_any(lower_text, COOKING_VERBS):
        score += 0.15

    if normalized_hashtags & RECIPE_HASHTAGS:
        score += 0.10

    if creator.strip().lower().lstrip("@") in KNOWN_HIGH_YIELD_CREATORS:
        score += 0.05

    if _alpha_ratio(normalized) < 0.35:
        score *= 0.45

    return round(max(0.0, min(score, 1.0)), 4)


def classify_evidence_quality(score: float) -> str:
    if score >= 0.70:
        return "high"
    if score >= 0.45:
        return "medium"
    if score >= 0.20:
        return "low"
    return "rejected"


def is_usable_ocr(text: str) -> bool:
    """Return True only when OCR text is likely to be useful recipe evidence."""
    normalized = normalize_evidence_text(text)
    if len(normalized) < 40:
        return False
    if _alpha_ratio(normalized) < 0.45:
        return False
    return compute_recipe_evidence_score(normalized) >= 0.30
