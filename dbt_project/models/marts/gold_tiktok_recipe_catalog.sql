with source as (
    select *
    from {{ ref('stg_silver_tiktok_recipes') }}
),

ranked as (
    select
        raw_id,
        trim(regexp_replace(original_title, '\\s+', ' ')) as display_title,
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
        row_number() over (
            partition by lower(url_tiktok)
            order by
                case recipe_status
                    when 'full_recipe' then 1
                    when 'partial_recipe' then 2
                    when 'food_content' then 3
                    else 4
                end,
                caption_completeness_score desc,
                processing_confidence desc,
                processed_at desc
        ) as row_num
    from source
)

select
    raw_id,
    display_title,
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
    processed_at
from ranked
where row_num = 1
