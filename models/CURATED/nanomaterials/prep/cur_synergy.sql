-- Модель: cur_synergy
-- Нормализация и подготовка данных по синергетическим эффектам наночастиц (synergy).
-- Здесь фильтруются строки с некорректными числовыми значениями, парсятся числовые поля, нормализуются форма и метод синтеза, а также создаётся признак покрытия.
-- Модель служит промежуточным слоем для дальнейшей аналитики и построения справочников.

{{ config(
    materialized='table',
    schema='curated',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (serial_number)"
) }}

with dedup_synergy as (
    -- Оставляем только строки с корректными значениями размеров и потенциала (или NULL)
    select *
    from {{ ref('uni_synergy') }}
    where
        -- np_size_min_nm: допускаем только числа с точкой/запятой или NULL
        np_size_min_nm IS NULL
        OR regexp_replace(np_size_min_nm, ',', '.', 'g') ~ '^[-+]?\d+(\.\d+)?$'

        -- zeta_potential_mv: допускаем только числа с точкой/запятой или NULL
        AND (
            zeta_potential_mv IS NULL
            OR regexp_replace(zeta_potential_mv, ',', '.', 'g') ~ '^[-+]?\d+(\.\d+)?$'
        )
)

select
    dedup_synergy.*,

    -- Булево: признак открытого доступа (access = 1)
    {{ bool_from_int('access') }} as access_bool,

    -- Преобразуем np_size_min_nm из строки с запятой в число с плавающей точкой
    {{ parse_decimal_comma_to_float('np_size_min_nm') }} as np_size_min_nm_parsed,

    -- Преобразуем zeta_potential_mv из строки с запятой в число с плавающей точкой
    {{ parse_decimal_comma_to_float('zeta_potential_mv') }} as zeta_potential_mv_parsed,

    -- Нормализуем форму наночастицы
    {{ normalize_shape("shape") }} as normalized_shape,

    -- Стандартизируем метод синтеза по укрупнённым группам
    {{ standardize_synthesis_method('synthesis_method') }} as standardized_synthesis_method,

    -- Признак наличия покрытия (создаётся на основе поля coating_with_antimicrobial_peptide_polymers)
    {{ create_has_coating_column('coating_with_antimicrobial_peptide_polymers') }},


    CASE
        -- Приводим все к нижнему регистру и проверяем на маркеры устойчивости
        WHEN lower(mdr) IN ('r', 'methicilin resistant') THEN true
        -- Если mdr не NULL, но не является маркером устойчивости, считаем, что устойчивости нет
        WHEN mdr IS NOT NULL THEN false
        -- Если mdr был NULL, оставляем NULL, чтобы потом его обработать
        ELSE NULL
    END as is_mdr

from dedup_synergy
