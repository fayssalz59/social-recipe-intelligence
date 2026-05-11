with source as (
    select *
    from {{ source('silver', 'SILVER_TIKTOK_RECIPES') }}
),

deduplicated as (
    select
        raw_id,
        original_title,
        original_description,
        url_tiktok,
        lower(recipe_language) as recipe_language,
        is_vegetarian,
        initcap(cuisine_style) as cuisine_style,
        initcap(main_ingredient) as main_ingredient,
        processing_confidence,
        model_name,
        processed_at,
        record_hash,
        row_number() over (
            partition by raw_id
            order by processed_at desc
        ) as row_num
    from source
)

select *
from deduplicated
where row_num = 1