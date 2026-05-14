select *
from {{ ref('gold_tiktok_recipe_catalog') }}
where recipe_quality_score < 0
   or recipe_quality_score > 1
