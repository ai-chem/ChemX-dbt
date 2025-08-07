-- Аналитика:
-- поиск дублирующихся наночастиц по составному ключу.
-- Запрос определяет, встречается ли одна и та же комбинация
-- (nanoparticle, normalized_shape, has_coating, np_size_avg_nm)
-- более одного раза в объединённых данных из всех финальных таблиц (cytotox, seltox, synergy).
-- Это полезно для выявления повторов,
-- массового использования одних и тех же наночастиц и проверки корректности справочника.

with all_nanoparticles as (
    select
        nanoparticle,
        normalized_shape,
        has_coating,
        np_size_avg_nm
    from dbt_curated.cur_cytotox
    union all
    select
        nanoparticle,
        normalized_shape,
        has_coating,
        np_size_avg_nm
    from dbt_curated.cur_seltox
    union all
    select
        nanoparticle,
        normalized_shape,
        has_coating,
        np_size_avg_nm
    from dbt_curated.cur_synergy
)
select
    nanoparticle,
    normalized_shape,
    has_coating,
    np_size_avg_nm,
    count(*) as total_count
from all_nanoparticles
group by
    nanoparticle,
    normalized_shape,
    has_coating,
    np_size_avg_nm
having count(*) > 1
