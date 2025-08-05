-- Справочник источников данных (dim_nanozymes_source).
-- Каждая строка — уникальный источник данных по имени таблицы и времени загрузки/обработки.
-- Используется для связи с факт-таблицей через source_id.

{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (source_id)"
) }}

select
    row_number() over (
        order by source_table, dbt_loaded_at, dbt_curated_at
    ) as source_id,
    source_table,
    dbt_loaded_at,
    dbt_curated_at
from (
    select distinct
        source_table,
        dbt_loaded_at,
        dbt_curated_at
    from {{ ref('final_cur_nanozymes') }}
) as unique_sources