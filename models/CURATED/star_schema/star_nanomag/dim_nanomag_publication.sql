{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (publication_id)"
) }}

with ranked as (
    select
        *,
        row_number() over (partition by doi order by doi) as rn
    from {{ ref('final_cur_nanomag') }}
)
select
    row_number() over (order by doi) as publication_id,
    doi,
    journal,
    publisher,
    year,
    title,
    pdf,
    access,
    access_bool,
    supp,
    article_name_folder,
    supp_info_name_folder
from ranked
where rn = 1