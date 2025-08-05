{{ config(materialized='table', schema='serving') }}

select
    nanoparticle,
    count(*) as experiments_count,
    avg(vmax_value) as avg_vmax,
    min(vmax_value) as min_vmax,
    max(vmax_value) as max_vmax,
    count(distinct publication_id) as unique_publications,
    min(year) as first_year,
    max(year) as last_year
from {{ ref('serving_all_data_nanozymes') }}
group by nanoparticle
order by experiments_count desc
limit 10