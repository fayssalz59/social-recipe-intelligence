select
    raw_id as id,
    display_title as title,
    url_tiktok,
    recipe_language as language,
    is_vegetarian,
    cuisine_style,
    main_ingredient,
    ingredients,
    is_recipe,
    recipe_status,
    has_ingredient_list,
    has_instructions,
    caption_completeness_score,
    rejection_reason,
    processing_confidence as confidence,
    model_name,
    processed_at
from {{ ref('gold_tiktok_recipe_catalog') }}
