{{ config(materialized='table', schema='serving') }}

select
    nanoparticle,
    count(*) as experiments_count,
    avg(mic_np_µg_ml_parsed) as avg_mic_np_µg_ml,
    min(mic_np_µg_ml_parsed) as min_mic_np_µg_ml,
    max(mic_np_µg_ml_parsed) as max_mic_np_µg_ml,
    count(distinct publication_id) as unique_publications,
    count(distinct bacteria_id) as unique_bacteria,
    min(year) as first_year,
    max(year) as last_year
from {{ ref('serving_all_data_seltox') }}
group by nanoparticle
order by experiments_count desc
limit 10