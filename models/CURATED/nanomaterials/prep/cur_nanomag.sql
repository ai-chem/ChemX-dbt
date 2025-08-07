-- Модель: cur_nanomag
-- Нормализация и подготовка данных.
-- Здесь выполняется удаление дублей, булева интерпретация признака доступа и нормализация названия наночастицы.
-- Модель используется как промежуточный слой для дальнейшей аналитики и построения справочников.

{{ config(
    materialized='table',
    schema='curated',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (id)"
) }}

with dedup_nanomag as (
    -- Удаляем дублирующиеся строки из исходной таблицы uni_nanomag
    {{ deduplicate_model('uni_nanomag') }}
)

select
    dedup_nanomag.*,

    -- Булево: признак открытого доступа (access = 1)
    {{ bool_from_int('access') }} as access_bool

from dedup_nanomag
