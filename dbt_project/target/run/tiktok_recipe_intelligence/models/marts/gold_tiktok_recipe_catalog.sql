
  create or replace   view TIKTOK_PORTFOLIO_DB.GOLD.gold_tiktok_recipe_catalog
  
  
  
  
  as (
    select
    raw_id,
    trim(regexp_replace(original_title, '\\s+', ' ')) as display_title,
    url_tiktok,
    recipe_language,
    is_vegetarian,
    cuisine_style,
    main_ingredient,
    processing_confidence,
    model_name,
    processed_at
from TIKTOK_PORTFOLIO_DB.GOLD.stg_silver_tiktok_recipes
  );

