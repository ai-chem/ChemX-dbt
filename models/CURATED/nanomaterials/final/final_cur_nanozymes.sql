-- Финальная модель cur_nanozymes.
-- Копирует все данные из обработанной модели cur_nanozymes без изменений.
-- Используется для унификации структуры и дальнейшей аналитики на уровне curated-слоя.

{{ config(
    materialized='table',
    schema='curated',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (id)"
) }}

select *
from {{ ref('cur_nanozymes') }}
