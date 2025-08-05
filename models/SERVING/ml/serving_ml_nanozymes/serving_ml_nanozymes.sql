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
        np.length_lower,
        np.length_upper,
        np.length_mean,
        np.width_lower,
        np.width_upper,
        np.width_mean,
        np.depth_lower,
        np.depth_upper,
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
        fact.km_unit,
        fact.vmax_value,
        fact.vmax_unit,
        fact.ph,
        fact.temperature

    from {{ ref('fact_nanozymes_experiments') }} as fact
    left join {{ ref('dim_nanozymes_nanoparticle') }} as np on fact.nanoparticle_id = np.nanoparticle_id
)

-- Шаг 2: Выбираем поля и обрабатываем пропуски
select
    -- Столбцы без пропусков
    id,
    nanoparticle,
    activity,
    reaction_type,
    target_source,

    -- ====================================================================
    -- Обработка ЧИСЛОВЫХ колонок
    -- ====================================================================
    {{ impute_numeric('length_lower') }},
    {{ impute_numeric('length_upper') }},
    {{ impute_numeric('length_mean') }},
    {{ impute_numeric('width_lower') }},
    {{ impute_numeric('width_upper') }},
    {{ impute_numeric('width_mean') }},
    {{ impute_numeric('depth_lower') }},
    {{ impute_numeric('depth_upper') }},
    {{ impute_numeric('depth_mean') }},
    {{ impute_numeric('c_min') }},
    {{ impute_numeric('c_max') }},
    {{ impute_numeric('c_const') }},
    {{ impute_numeric('ccat_value') }},
    {{ impute_numeric('km_value') }},
    {{ impute_numeric('vmax_value') }},
    {{ impute_numeric('ph') }},
    {{ impute_numeric('temperature') }},

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
    {{ impute_categorical('km_unit') }},
    {{ impute_categorical('vmax_unit') }}

from joined_data