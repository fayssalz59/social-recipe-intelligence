select *
from {{ ref('gold_tiktok_recipe_catalog') }}
where final_recipe_confidence < 0
   or final_recipe_confidence > 1
