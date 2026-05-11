
  create or replace   view TIKTOK_PORTFOLIO_DB.GOLD.gold_api_recipe_catalog
  
  
  
  
  as (
    select
    raw_id as id,
    display_title as title,
    url_tiktok,
    recipe_language as language,
    is_vegetarian,
    cuisine_style,
    main_ingredient,
    processing_confidence as confidence,
    processed_at
from TIKTOK_PORTFOLIO_DB.GOLD.gold_tiktok_recipe_catalog
  );

