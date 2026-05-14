select *
from {{ ref('gold_tiktok_recipe_catalog') }}
where recipe_status = 'full_recipe'
  and coalesce(ingredient_count, 0) < 2
