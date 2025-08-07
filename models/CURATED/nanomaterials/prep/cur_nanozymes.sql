-- Модель: cur_nanozymes
-- Нормализация и подготовка данных по наноэнзимам (nanozymes).
-- Здесь выполняется удаление дублей, преобразование булевых признаков и парсинг размерных характеристик (length, width, depth).
-- Модель служит промежуточным слоем для дальнейшей аналитики и построения справочников.

{{ config(
    materialized='table',
    schema='curated',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (id)"
) }}

with dedup_nanozymes as (
    -- Удаляем дублирующиеся строки из исходной таблицы uni_nanozymes
    {{ deduplicate_model('uni_nanozymes') }}
)

select
    dedup_nanozymes.*,

    -- Булево: признак открытого доступа (access = 1)
    {{ bool_from_int('access') }} as access_bool,

    -- Парсим диапазон длины (length): извлекаем нижнюю, верхнюю и среднюю границы
    {{ parse_range_component('length', 'lower') }} as length_lower,
    {{ parse_range_component('length', 'upper') }} as length_upper,
    {{ parse_range_component('length', 'mean') }}  as length_mean,

    -- Парсим диапазон ширины (width): извлекаем нижнюю, верхнюю и среднюю границы
    {{ parse_range_component('width', 'lower') }} as width_lower,
    {{ parse_range_component('width', 'upper') }} as width_upper,
    {{ parse_range_component('width', 'mean') }}  as width_mean,

    -- Парсим диапазон глубины (depth): извлекаем нижнюю, верхнюю и среднюю границы
    {{ parse_range_component('depth', 'lower') }} as depth_lower,
    {{ parse_range_component('depth', 'upper') }} as depth_upper,
    {{ parse_range_component('depth', 'mean') }}  as depth_mean

from dedup_nanozymes
