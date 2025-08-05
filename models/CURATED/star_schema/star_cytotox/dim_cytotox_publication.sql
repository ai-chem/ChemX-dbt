with ranked as (
    select
        *,
        row_number() over (partition by doi order by doi) as rn
    from {{ ref('final_cur_cytotox') }}
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
    access_bool
from ranked
where rn = 1