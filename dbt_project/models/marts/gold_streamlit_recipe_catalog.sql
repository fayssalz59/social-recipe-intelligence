select
    raw_id,
    display_title,
    url_tiktok,
    recipe_language,
    is_vegetarian,
    cuisine_style,
    main_ingredient,
    processing_confidence,
    model_name,
    processed_at
from {{ ref('gold_tiktok_recipe_catalog') }}
