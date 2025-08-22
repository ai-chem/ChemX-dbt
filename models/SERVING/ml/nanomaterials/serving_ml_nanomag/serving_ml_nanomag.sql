{{ config(materialized='table', schema='serving') }}

-- Шаг 1: Объединяем данные
with joined_data as (
    select
        fact.id,
        np.np_shell_2,
        np.xrd_scherrer_size,
        fact.zfc_h_meas,
        fact.htherm_sar,
        fact.mri_r1,
        fact.mri_r2,
        np.nanoparticle,
        np.np_shell,
        np.space_group_core,
        np.space_group_shell,
        fact.squid_h_max,
        fact.hc_koe,
        fact.fc_field_t_numeric,
        fact.squid_temperature_numeric,
        fact.squid_sat_mag_numeric,
        fact.coercivity_numeric,
        fact.squid_rem_mag_numeric,
        fact.exchange_bias_shift_oe_numeric,
        fact.vertical_loop_shift_m_vsl_emu_g_numeric,

        np.core_shell_formula,
        np.emic_size,
        np.np_hydro_size,
        np.instrument,
        np.name
    from {{ ref('fact_nanomag_experiments') }} as fact
    left join {{ ref('dim_nanomag_nanoparticle') }} as np on fact.nanoparticle_id = np.nanoparticle_id
)

-- Шаг 2: Выбираем поля и обрабатываем пропуски
select
    -- Столбцы без пропусков
    id,
    nanoparticle,

    -- ====================================================================
    -- Обработка ЧИСЛОВЫХ колонок
    -- ====================================================================
    {{ impute_numeric('xrd_scherrer_size') }},
    {{ impute_numeric('zfc_h_meas') }},
    {{ impute_numeric('htherm_sar') }},
    {{ impute_numeric('mri_r1') }},
    {{ impute_numeric('mri_r2') }},
    {{ impute_numeric('squid_h_max') }},
    {{ impute_numeric('hc_koe') }},
    {{ impute_numeric('fc_field_t_numeric') }},
    {{ impute_numeric('squid_temperature_numeric') }},
    {{ impute_numeric('squid_sat_mag_numeric') }},
    {{ impute_numeric('coercivity_numeric') }},
    {{ impute_numeric('squid_rem_mag_numeric') }},
    {{ impute_numeric('exchange_bias_shift_oe_numeric') }},
    {{ impute_numeric('vertical_loop_shift_m_vsl_emu_g_numeric') }},

    {{ impute_numeric('emic_size') }},
    {{ impute_numeric('np_hydro_size') }},

    -- ====================================================================
    -- Обработка ТЕКСТОВЫХ колонок
    -- ====================================================================
    {{ impute_categorical('np_shell_2') }},
    {{ impute_categorical('np_shell') }},
    {{ impute_categorical('space_group_core') }},
    {{ impute_categorical('space_group_shell') }},

    {{ impute_categorical('core_shell_formula') }},
    {{ impute_categorical('instrument') }},
    {{ impute_categorical('name') }}

from joined_data
