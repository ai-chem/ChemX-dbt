
-- Аналитика:
-- поиск nanoparticle_id, встречающихся более чем в одной финальной таблице.
-- Этот запрос позволяет выявить наночастицы,
-- которые используются сразу в нескольких типах экспериментов (cytotox, seltox, synergy).
-- Это полезно для анализа пересечений и сопоставления данных между разными датасетами.

with all_ids as (
    select nanoparticle_id, 'cytotox' as source from dbt_curated.final_cur_cytotox
    union all
    select nanoparticle_id, 'seltox' as source from dbt_curated.final_cur_seltox
    union all
    select nanoparticle_id, 'synergy' as source from dbt_curated.final_cur_synergy
)
select
    nanoparticle_id,
    count(distinct source) as sources_count
from all_ids
group by nanoparticle_id
having count(distinct source) > 1
order by nanoparticle_id;
