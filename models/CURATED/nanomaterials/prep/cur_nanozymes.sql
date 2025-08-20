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
    {{ deduplicate_model('uni_nanozymes') }}
),

standardized_values as (

    select
        *,

        {{ bool_from_int('access') }} as access_bool,

        {{ parse_range_component('length', 'lower') }} as length_lower,
        {{ parse_range_component('length', 'upper') }} as length_upper,
        {{ parse_range_component('length', 'mean') }}  as length_mean,

        {{ parse_range_component('width', 'lower') }} as width_lower,
        {{ parse_range_component('width', 'upper') }} as width_upper,
        {{ parse_range_component('width', 'mean') }}  as width_mean,

        {{ parse_range_component('depth', 'lower') }} as depth_lower,
        {{ parse_range_component('depth', 'upper') }} as depth_upper,
        {{ parse_range_component('depth', 'mean') }}  as depth_mean,

        {{ standardize_vmax('vmax_value', 'vmax_unit') }} as vmax_standardized_m_per_s

    from dedup_nanozymes
)

select * from standardized_values
