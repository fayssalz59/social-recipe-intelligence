with recipes as (
    select *
    from {{ ref('stg_silver_tiktok_recipes') }}
),

evidence as (
    select *
    from {{ ref('int_recipe_evidence_by_raw') }}
),

scored as (
    select
        recipes.*,
        coalesce(
            nullif(regexp_substr(recipes.url_tiktok, 'video/([0-9]+)', 1, 1, 'e', 1), ''),
            nullif(recipes.url_tiktok, ''),
            to_varchar(recipes.raw_id)
        ) as content_key,
        coalesce(evidence.best_evidence_quality_score, recipes.evidence_quality_score, 0) as best_evidence_quality_score,
        coalesce(evidence.avg_evidence_quality_score, recipes.evidence_quality_score, 0) as avg_evidence_quality_score,
        coalesce(evidence.evidence_source_count, 0) as evidence_source_count,
        coalesce(evidence.ocr_source_count, 0) as ocr_source_count,
        coalesce(evidence.audio_source_count, 0) as audio_source_count,
        coalesce(evidence.comment_source_count, 0) as comment_source_count,
        coalesce(evidence.recipe_signal_count, 0) as recipe_signal_count,
        evidence.best_evidence_text,
        coalesce(array_size(ingredients), 0) as ingredient_count,
        coalesce(array_size(final_recipe_json:steps), 0) as step_count,
        coalesce(array_size(missing_recipe_info), 0) as missing_info_count
    from recipes
    left join evidence
        on recipes.raw_id = evidence.raw_id
),

quality as (
    select
        *,
        case
            when not coalesce(is_recipe, false) then 0
            else greatest(
                0,
                least(
                    1,
                    0.22 * iff(has_ingredient_list, 1, 0)
                    + 0.22 * iff(has_instructions, 1, 0)
                    + 0.16 * coalesce(caption_completeness_score, 0)
                    + 0.16 * coalesce(final_recipe_confidence, 0)
                    + 0.14 * coalesce(best_evidence_quality_score, 0)
                    + 0.06 * iff(ingredient_count >= 3, 1, 0)
                    + 0.04 * iff(step_count >= 2, 1, 0)
                    - 0.03 * least(missing_info_count, 5)
                )
            )
        end as recipe_quality_score
    from scored
)

select
    *,
    case
        when recipe_quality_score >= 0.80 then 'A'
        when recipe_quality_score >= 0.60 then 'B'
        when recipe_quality_score >= 0.40 then 'C'
        else 'D'
    end as recipe_quality_grade
from quality
