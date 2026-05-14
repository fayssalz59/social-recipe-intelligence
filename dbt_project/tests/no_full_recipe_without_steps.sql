select *
from {{ ref('gold_tiktok_recipe_catalog') }}
where recipe_status = 'full_recipe'
  and coalesce(step_count, 0) = 0
