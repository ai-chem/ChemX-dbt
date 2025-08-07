-- Финальная модель cur_nanomag.
-- Просто копирует все данные из обработанной модели cur_nanomag без изменений.
-- Используется для унификации структуры и дальнейших join'ов или аналитики на уровне curated-слоя.

{{ config(
    materialized='table',
    schema='curated',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (id)"
) }}

select *
from {{ ref('cur_nanomag') }}
