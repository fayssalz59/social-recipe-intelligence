select
    raw_id as id,
    display_title as title,
    url_tiktok,
    recipe_language as language,
    is_vegetarian,
    cuisine_style,
    main_ingredient,
    processing_confidence as confidence,
    model_name,
    processed_at
from {{ ref('gold_tiktok_recipe_catalog') }}
