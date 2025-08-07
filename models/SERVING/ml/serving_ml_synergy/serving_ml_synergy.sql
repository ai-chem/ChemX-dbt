{{ config(materialized='table', schema='serving') }}

-- Шаг 1: Объединяем данные
with joined_data as (
    select
        fact.serial_number,
        np.nanoparticle,
        np.has_coating,
        np.normalized_shape,
        np.shape,
        np.standardized_synthesis_method,
        np.np_size_min_nm_parsed,
        np.np_size_max_nm,
        np.np_size_avg_nm,
        np.zeta_potential_mv_parsed,
        np.coating_with_antimicrobial_peptide_polymers,
        fact.method,
        fact.time_hr,
        drg.drug,
        fact.drug_dose_µg_disk,
        fact.np_concentration_µg_ml,
        fact.zoi_drug_mm_or_mic__µg_ml,
        fact.error_zoi_drug_mm_or_mic_µg_ml,
        fact.zoi_np_mm_or_mic_np_µg_ml,
        fact.error_zoi_np_mm_or_mic_np_µg_ml,
        fact.zoi_drug_np_mm_or_mic_drug_np_µg_ml,
        fact.error_zoi_drug_np_mm_or_mic_drug_np_µg_ml,
        fact.fold_increase_in_antibacterial_activity,
        fact.fic,
        fact.effect,
        fact.combined_mic,
        fact.peptide_mic,
        fact.viability_percent,
        fact.viability_error,
        bact.bacteria,
        bact.is_mdr,
        bact.strain

    from {{ ref('fact_synergy_experiments') }} as fact
    left join {{ ref('dim_synergy_nanoparticle') }} as np on fact.nanoparticle_id = np.nanoparticle_id
    left join {{ ref('dim_synergy_bacteria') }} as bact on fact.bacteria_id = bact.bacteria_id
    left join {{ ref('dim_synergy_drug') }} as drg on fact.drug_id = drg.drug_id
)

-- Шаг 2: Выбираем поля и обрабатываем пропуски
select
    -- Столбцы без пропусков
    serial_number,
    nanoparticle,
    has_coating,
    normalized_shape,
    bacteria,

    -- ====================================================================
    -- Обработка ЧИСЛОВЫХ колонок
    -- ====================================================================
    {{ impute_numeric('np_size_min_nm_parsed') }},
    {{ impute_numeric('np_size_max_nm') }},
    {{ impute_numeric('np_size_avg_nm') }},
    {{ impute_numeric('zeta_potential_mv_parsed') }},
    {{ impute_numeric('time_hr') }},
    {{ impute_numeric('drug_dose_µg_disk') }},
    {{ impute_numeric('np_concentration_µg_ml') }},
    {{ impute_numeric('zoi_drug_mm_or_mic__µg_ml') }},
    {{ impute_numeric('error_zoi_drug_mm_or_mic_µg_ml') }},
    {{ impute_numeric('zoi_np_mm_or_mic_np_µg_ml') }},
    {{ impute_numeric('error_zoi_np_mm_or_mic_np_µg_ml') }},
    {{ impute_numeric('zoi_drug_np_mm_or_mic_drug_np_µg_ml') }},
    {{ impute_numeric('error_zoi_drug_np_mm_or_mic_drug_np_µg_ml') }},
    {{ impute_numeric('fold_increase_in_antibacterial_activity') }},
    {{ impute_numeric('fic') }},
    {{ impute_numeric('combined_mic') }},
    {{ impute_numeric('peptide_mic') }},
    {{ impute_numeric('viability_percent') }},
    {{ impute_numeric('viability_error') }},

    -- ====================================================================
    -- Обработка ТЕКСТОВЫХ колонок
    -- ====================================================================
    {{ impute_categorical('shape') }},
    {{ impute_categorical('standardized_synthesis_method') }},
    {{ impute_categorical('coating_with_antimicrobial_peptide_polymers') }},
    {{ impute_categorical('drug') }},
    {{ impute_categorical('effect') }},
    {{ impute_categorical('strain') }},
    {{ impute_boolean('is_mdr') }},
    {{ impute_categorical('method') }}

from joined_data
