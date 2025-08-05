-- Справочник бактерий (dim_synergy_bacteria).
-- Каждая строка — уникальная бактерия (штамм), определяемая по названию, штамму и MDR-статусу.
-- Используется для связи с факт-таблицей через bacteria_id.

{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (bacteria_id)"
) }}

with unique_bacteria as (
    select distinct
        bacteria,
        strain,
        mdr,
        is_mdr
    from {{ ref('final_cur_synergy') }}
)

select
    row_number() over (
        order by bacteria, strain, mdr, is_mdr
    ) as bacteria_id,
    bacteria,
    strain,
    mdr,
    is_mdr
from unique_bacteria