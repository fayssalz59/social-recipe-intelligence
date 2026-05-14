with bronze as (
    select count(*) as bronze_rows
    from {{ env_var('SNOWFLAKE_DB') }}.{{ env_var('SNOWFLAKE_SCHEMA_BRONZE', 'BRONZE') }}.BRONZE_TIKTOK_RECIPES
),

silver as (
    select
        count(*) as silver_rows,
        count_if(is_recipe) as recipe_rows,
        count_if(recipe_status = 'full_recipe') as full_recipe_rows,
        count_if(recipe_status = 'partial_recipe') as partial_recipe_rows,
        count_if(recipe_status in ('food_content', 'non_recipe') or not is_recipe) as rejected_rows,
        avg(recipe_quality_score) as avg_recipe_quality_score,
        count_if(final_recipe_json is null) / nullif(count(*), 0) as missing_final_json_rate,
        count(*) - count(distinct lower(url_tiktok)) as duplicate_url_count
    from {{ ref('gold_tiktok_recipe_catalog') }}
),

gold as (
    select count(*) as gold_rows
    from {{ ref('gold_api_recipe_catalog') }}
),

evidence as (
    select
        count_if(source_type = 'video_ocr') as ocr_attempt_rows,
        count_if(source_type = 'video_ocr' and is_recipe_signal) as usable_ocr_rows
    from {{ ref('stg_silver_recipe_evidence') }}
)

select
    current_date() as run_date,
    bronze.bronze_rows,
    silver.silver_rows,
    gold.gold_rows,
    silver.recipe_rows / nullif(silver.silver_rows, 0) as recipe_rate,
    silver.full_recipe_rows / nullif(silver.silver_rows, 0) as full_recipe_rate,
    silver.partial_recipe_rows / nullif(silver.silver_rows, 0) as partial_recipe_rate,
    silver.rejected_rows / nullif(silver.silver_rows, 0) as rejected_rate,
    silver.avg_recipe_quality_score,
    silver.missing_final_json_rate,
    silver.duplicate_url_count,
    evidence.usable_ocr_rows / nullif(evidence.ocr_attempt_rows, 0) as usable_ocr_rate
from bronze
cross join silver
cross join gold
cross join evidence
