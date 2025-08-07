-- Модель: cur_cytotox
-- Нормализация и подготовка данных по цитотоксичности наночастиц.
-- Здесь формируются булевые признаки, нормализуются ключевые поля и создаются упрощённые категории для дальнейшей аналитики.

{{ config(
    materialized='table',
    schema='curated',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (serial_number)"
) }}

with dedup_cytotox as (
    {{ deduplicate_model('uni_cytotox') }}
)

select
    dedup_cytotox.*,

    -- Булево: признак положительного заряда поверхности
    {{ bool_from_text_equals('surface_charge', 'Positive') }} as is_surface_positive,

    -- Булево: признак, что источник клеток — человек (H)
    {{ bool_from_text_equals('human_animal', 'H') }} as is_human,

    -- Булево: признак открытого доступа (access = 1)
    {{ bool_from_int('access') }} as access_bool,

    -- Нормализуем форму наночастицы (например, "Sphere" → "spherical")
    {{ normalize_shape("shape") }} as normalized_shape,

    -- Стандартизируем метод синтеза по укрупнённым группам (например, "Precipitation" → "Chemical Synthesis")
    {{ standardize_synthesis_method('synthesis_method') }} as standardized_synthesis_method,

    -- Средний размер наночастицы (используется для идентификации)
    size_in_medium_nm as np_size_avg_nm,

    -- Признак наличия покрытия (создаётся на основе coat_functional_group)
    {{ create_has_coating_column('coat_functional_group') }}

from dedup_cytotox
