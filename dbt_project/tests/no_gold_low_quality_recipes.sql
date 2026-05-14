select *
from {{ ref('gold_api_recipe_catalog') }}
where coalesce(recipe_quality_score, 0) < 0.60
