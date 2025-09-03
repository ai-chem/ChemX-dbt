{{ config(materialized='table', schema='serving') }}

-- Шаг 1: Объединяем данные
with joined_data as (
    select
        fact.id,
        np.nanoparticle,
        np.syngony,
        np.length,
        np.width,
        np.depth,
        np.surface,
        np.length_mean,
        np.width_mean,
        np.depth_mean,

        fact.activity,
        fact.reaction_type,
        fact.target_source,
        fact.c_min,
        fact.c_max,
        fact.c_const,
        fact.c_const_unit,
        fact.ccat_value,
        fact.ccat_unit,
        fact.km_value,
        fact.vmax_standardized_m_per_s,
        fact.km_unit,
        fact.ph,
        fact.temperature

    from {{ ref('fact_nanozymes_experiments') }} as fact
    left join {{ ref('dim_nanozymes_nanoparticle') }} as np on fact.nanoparticle_id = np.nanoparticle_id
)

-- Шаг 2: Выбираем поля и обрабатываем пропуски
select
    -- Столбцы без пропусков
    nanoparticle,
    activity,
    reaction_type,

    -- ====================================================================
    -- Обработка ЧИСЛОВЫХ колонок
    -- ====================================================================
    {{ impute_numeric('length_mean') }},
    {{ impute_numeric('width_mean') }},
    {{ impute_numeric('depth_mean') }},
    {{ impute_numeric('c_min') }},
    {{ impute_numeric('c_max') }},
    {{ impute_numeric('c_const') }},
    {{ impute_numeric('ccat_value') }},
    {{ impute_numeric('km_value') }},
    {{ impute_numeric('ph') }},
    {{ impute_numeric('temperature') }},
    {{ impute_numeric('vmax_standardized_m_per_s') }},

    -- ====================================================================
    -- Обработка ТЕКСТОВЫХ колонок
    -- ====================================================================
    {{ impute_categorical('syngony') }},
    {{ impute_categorical('length') }},
    {{ impute_categorical('width') }},
    {{ impute_categorical('depth') }},
    {{ impute_categorical('surface') }},
    {{ impute_categorical('c_const_unit') }},
    {{ impute_categorical('ccat_unit') }},
    {{ impute_categorical('km_unit') }}

from joined_data
