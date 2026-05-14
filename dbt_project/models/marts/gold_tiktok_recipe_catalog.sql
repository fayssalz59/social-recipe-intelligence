with source as (
    select *
    from {{ ref('int_recipe_quality_scoring') }}
),

ranked as (
    select
        raw_id,
        content_key,
        trim(regexp_replace(coalesce(nullif(final_recipe_title, ''), original_title), '\\s+', ' ')) as display_title,
        original_description,
        recovered_text,
        evidence_text,
        best_evidence_text,
        url_tiktok,
        recipe_language,
        is_vegetarian,
        cuisine_style,
        main_ingredient,
        ingredients,
        ingredient_count,
        is_recipe,
        recipe_status,
        has_ingredient_list,
        has_instructions,
        caption_completeness_score,
        evidence_quality_score,
        best_evidence_quality_score,
        avg_evidence_quality_score,
        evidence_source_count,
        ocr_source_count,
        audio_source_count,
        comment_source_count,
        recipe_signal_count,
        rejection_reason,
        final_recipe_title,
        final_recipe_text,
        final_recipe_json,
        step_count,
        missing_recipe_info,
        missing_info_count,
        final_recipe_confidence,
        final_recipe_language,
        recipe_quality_score,
        recipe_quality_grade,
        processing_confidence,
        model_name,
        processed_at,
        row_number() over (
            partition by content_key
            order by
                recipe_quality_score desc,
                case recipe_status
                    when 'full_recipe' then 1
                    when 'partial_recipe' then 2
                    when 'food_content' then 3
                    else 4
                end,
                final_recipe_confidence desc,
                caption_completeness_score desc,
                processing_confidence desc,
                processed_at desc
        ) as row_num
    from source
)

select
    raw_id,
    content_key,
    display_title,
    original_description,
    recovered_text,
    evidence_text,
    best_evidence_text,
    url_tiktok,
    recipe_language,
    is_vegetarian,
    cuisine_style,
    main_ingredient,
    ingredients,
    ingredient_count,
    is_recipe,
    recipe_status,
    has_ingredient_list,
    has_instructions,
    caption_completeness_score,
    evidence_quality_score,
    best_evidence_quality_score,
    avg_evidence_quality_score,
    evidence_source_count,
    ocr_source_count,
    audio_source_count,
    comment_source_count,
    recipe_signal_count,
    rejection_reason,
    final_recipe_title,
    final_recipe_text,
    final_recipe_json,
    step_count,
    missing_recipe_info,
    missing_info_count,
    final_recipe_confidence,
    final_recipe_language,
    recipe_quality_score,
    recipe_quality_grade,
    processing_confidence,
    model_name,
    processed_at
from ranked
where row_num = 1
