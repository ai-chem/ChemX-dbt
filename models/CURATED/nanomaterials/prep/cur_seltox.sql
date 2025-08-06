-- Модель: cur_seltox
-- Нормализация и подготовка данных по селективной токсичности наночастиц (seltox).
-- Здесь фильтруются некорректные значения концентрации, парсятся числовые значения, нормализуются форма и метод синтеза, а также создаётся признак покрытия.
-- Модель служит промежуточным слоем для дальнейшей аналитики и построения справочников.

{{ config(
    materialized='table',
    schema='curated',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (serial_number)"
) }}

with dedup_seltox as (
    -- Оставляем только строки с корректным значением mic_np_µg_ml (NULL или число с запятой)
    select *
    from {{ ref('uni_seltox') }}
    where mic_np_µg_ml IS NULL
       OR mic_np_µg_ml ~ '^\d+(,\d+)?$'  -- допустимое число с запятой
)

select
    dedup_seltox.*,

    -- Преобразуем mic_np_µg_ml из строки с запятой в число с плавающей точкой
    {{ parse_decimal_comma_to_float('mic_np_µg_ml') }} as mic_np_µg_ml_parsed,

    -- Нормализуем форму наночастицы (например, "Sphere" в "spherical"), стандартизируем по укрупнённым группам
    {{ normalize_shape("shape") }} as normalized_shape,

    -- Стандартизируем метод синтеза по укрупнённым группам
    {{ standardize_synthesis_method('synthesis_method') }} as standardized_synthesis_method,

    -- Признак наличия покрытия (создаётся на основе поля coating)
    {{ create_has_coating_column('coating', 'bigint') }}

from dedup_seltox
