{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (publication_id)"
) }}

with ranked as (
    select
        *,
        row_number() over (partition by doi order by doi) as rn
    from {{ ref('final_cur_nanozymes') }}
)
select
    row_number() over (order by doi) as publication_id,
    doi,
    journal,
    year,
    title,
    pdf,
    access,
    access_bool
from ranked
where rn = 1