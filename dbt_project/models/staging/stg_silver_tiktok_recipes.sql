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
        case
            when lower(recipe_language) in ('english', 'eng') then 'en'
            when lower(recipe_language) in ('french', 'français', 'francais') then 'fr'
            when lower(recipe_language) in ('spanish', 'español') then 'es'
            when lower(recipe_language) in ('italian', 'italiano') then 'it'
            when lower(recipe_language) in ('portuguese', 'portugues', 'português') then 'pt'
            when lower(recipe_language) in ('arabic', 'العربية') then 'ar'
            when lower(recipe_language) in ('en', 'fr', 'es', 'it', 'pt', 'ar') then lower(recipe_language)
            else 'unknown'
        end as recipe_language,
        is_vegetarian,
        initcap(cuisine_style) as cuisine_style,
        initcap(main_ingredient) as main_ingredient,
        ingredients,
        coalesce(is_recipe, true) as is_recipe,
        coalesce(recipe_status, 'partial_recipe') as recipe_status,
        coalesce(has_ingredient_list, false) as has_ingredient_list,
        coalesce(has_instructions, false) as has_instructions,
        coalesce(caption_completeness_score, 0) as caption_completeness_score,
        rejection_reason,
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

select
    raw_id,
    original_title,
    original_description,
    url_tiktok,
    recipe_language,
    is_vegetarian,
    cuisine_style,
    main_ingredient,
    ingredients,
    is_recipe,
    recipe_status,
    has_ingredient_list,
    has_instructions,
    caption_completeness_score,
    rejection_reason,
    processing_confidence,
    model_name,
    processed_at,
    record_hash
from deduplicated
where row_num = 1
