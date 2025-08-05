{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (publication_id)"
) }}

with ranked as (
    select
        *,
        row_number() over (partition by doi order by doi) as rn
    from {{ ref('final_cur_seltox') }}
)
select
    row_number() over (order by doi) as publication_id,
    doi,
    article_list,
    journal_name,
    publisher,
    year,
    title,
    journal_is_oa,
    is_oa,
    oa_status,
    pdf,
    access,
    reference
from ranked
where rn = 1