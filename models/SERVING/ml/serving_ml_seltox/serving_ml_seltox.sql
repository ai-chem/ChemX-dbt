{{ config(materialized='table', schema='serving') }}

-- Шаг 1: Объединяем данные
with joined_data as (
    select
        fact.serial_number,
        np.nanoparticle,
        np.shape,
        np.coating,
        np.has_coating,
        np.np_size_min_nm,
        np.np_size_max_nm,
        np.np_size_avg_nm,
        np.zeta_potential_mv,
        np.hydrodynamic_diameter_nm,
        np.normalized_shape,
        np.standardized_synthesis_method,
        np.synthesis_method,
        np.solvent_for_extract,
        np.temperature_for_extract_c,
        np.duration_preparing_extract_min,
        np.precursor_of_np,
        np.concentration_of_precursor_mm,

        fact.method,
        fact.mic_np_µg_ml,
        fact.mic_np_µg_ml_parsed,
        fact.concentration,
        fact.zoi_np_mm,
        fact.time_set_hours,

        bact.bacteria,
        bact.mdr,
        bact.strain

    from {{ ref('fact_seltox_experiments') }} as fact
    left join {{ ref('dim_seltox_nanoparticle') }} as np on fact.nanoparticle_id = np.nanoparticle_id
    left join {{ ref('dim_seltox_bacteria') }} as bact on fact.bacteria_id = bact.bacteria_id
)

-- Шаг 2: Выбираем поля и обрабатываем пропуски
select
    -- Столбцы без пропусков или с которыми работаем иначе
    serial_number,
    nanoparticle,
    has_coating,
    normalized_shape,
    method,
    bacteria,
    mdr,

    -- ====================================================================
    -- Обработка ЧИСЛОВЫХ колонок
    -- ====================================================================
    {{ impute_numeric('coating') }}, -- bigint, но обрабатываем как число
    {{ impute_numeric('np_size_min_nm') }},
    {{ impute_numeric('np_size_max_nm') }},
    {{ impute_numeric('np_size_avg_nm') }},
    {{ impute_numeric('zeta_potential_mv') }},
    {{ impute_numeric('hydrodynamic_diameter_nm') }},
    {{ impute_numeric('temperature_for_extract_c') }},
    {{ impute_numeric('duration_preparing_extract_min') }},
    {{ impute_numeric('concentration_of_precursor_mm') }},
    {{ impute_numeric('mic_np_µg_ml_parsed') }},
    {{ impute_numeric('concentration') }},
    {{ impute_numeric('zoi_np_mm') }},
    {{ impute_numeric('time_set_hours') }},

    -- ====================================================================
    -- Обработка ТЕКСТОВЫХ колонок
    -- ====================================================================
    {{ impute_categorical('shape') }},
    {{ impute_categorical('standardized_synthesis_method') }},
    {{ impute_categorical('synthesis_method') }},
    {{ impute_categorical('solvent_for_extract') }},
    {{ impute_categorical('precursor_of_np') }},
    {{ impute_categorical('mic_np_µg_ml') }},
    {{ impute_categorical('strain') }}

from joined_data