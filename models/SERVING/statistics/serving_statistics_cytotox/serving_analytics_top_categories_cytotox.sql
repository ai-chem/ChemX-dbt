{{ config(materialized='table', schema='serving') }}

select
    nanoparticle,
    count(*) as experiments_count,
    avg(viability_percent) as avg_viability,
    min(viability_percent) as min_viability,
    max(viability_percent) as max_viability,
    count(distinct cell_line_id) as unique_cell_lines,
    count(distinct publication_id) as unique_publications,
    min(year) as first_year,
    max(year) as last_year
from {{ ref('serving_all_data_cytotox') }}
group by nanoparticle
order by experiments_count desc
limit 10
