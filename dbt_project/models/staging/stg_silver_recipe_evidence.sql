with source as (
    select *
    from {{ source('silver', 'SILVER_RECIPE_EVIDENCE') }}
),

deduplicated as (
    select
        evidence_id,
        raw_id,
        content_id,
        url_tiktok,
        lower(source_type) as source_type,
        source_name,
        evidence_text,
        coalesce(evidence_length, length(evidence_text)) as evidence_length,
        greatest(0, least(1, coalesce(evidence_quality_score, 0))) as evidence_quality_score,
        coalesce(evidence_quality_class, 'unknown') as evidence_quality_class,
        coalesce(is_recipe_signal, false) as is_recipe_signal,
        source_details,
        record_hash,
        created_at,
        row_number() over (
            partition by raw_id, lower(source_type), record_hash
            order by created_at desc
        ) as row_num
    from source
)

select
    evidence_id,
    raw_id,
    content_id,
    url_tiktok,
    source_type,
    source_name,
    evidence_text,
    evidence_length,
    evidence_quality_score,
    evidence_quality_class,
    is_recipe_signal,
    source_details,
    record_hash,
    created_at
from deduplicated
where row_num = 1
