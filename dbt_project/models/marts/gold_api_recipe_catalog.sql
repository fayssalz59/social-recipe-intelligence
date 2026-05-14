select
    raw_id as id,
    display_title as title,
    url_tiktok,
    final_recipe_language as language,
    is_vegetarian,
    cuisine_style,
    main_ingredient,
    ingredients,
    final_recipe_title,
    final_recipe_text,
    final_recipe_json,
    recipe_status,
    recipe_quality_score,
    recipe_quality_grade,
    recipe_quality_score as confidence,
    processed_at
from {{ ref('gold_tiktok_recipe_catalog') }}
where is_recipe = true
  and recipe_status in ('full_recipe', 'partial_recipe')
  and recipe_quality_score >= 0.60
  and coalesce(ingredient_count, 0) >= 2
  and coalesce(step_count, 0) >= 1
  and final_recipe_text is not null
  and trim(final_recipe_text) <> ''
