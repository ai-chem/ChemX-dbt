{{ config(materialized='table', schema='serving') }}

select
    nanoparticle,
    count(*) as experiments_count,
    count(distinct publication_id) as unique_publications,
    min(year) as first_year,
    max(year) as last_year
from {{ ref('serving_all_data_nanomag') }}
group by nanoparticle
order by experiments_count desc
limit 10