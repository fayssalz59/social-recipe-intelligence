with evidence as (
    select *
    from {{ ref('stg_silver_recipe_evidence') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by raw_id
            order by evidence_quality_score desc, evidence_length desc, created_at desc
        ) as evidence_rank
    from evidence
    where evidence_length > 0
),

aggregated as (
    select
        raw_id,
        max(evidence_quality_score) as best_evidence_quality_score,
        avg(evidence_quality_score) as avg_evidence_quality_score,
        count(*) as evidence_source_count,
        count_if(source_type = 'video_ocr') as ocr_source_count,
        count_if(source_type = 'audio_transcript') as audio_source_count,
        count_if(source_type = 'comments') as comment_source_count,
        count_if(is_recipe_signal) as recipe_signal_count,
        listagg(
            iff(evidence_rank <= 5, evidence_text, null),
            '\n\n---\n\n'
        ) within group (order by evidence_quality_score desc, evidence_length desc) as best_evidence_text
    from ranked
    group by raw_id
)

select *
from aggregated
