{{ config(
    materialized='view',
    schema='unified',
    unique_key='serial_number'
) }}

SELECT
    sn AS serial_number,
    np AS nanoparticle,  -- 🔁 Переименование поля
    bacteria,
    strain,
    np_synthesis as synthesis_method,
    drug,
    drug_dose_µg_disk,
    np_concentration_µg_ml,
    np_size_min_nm,
    np_size_max_nm,
    np_size_avg_nm,
    shape,
    method,
    zoi_drug_mm_or_mic__µg_ml,
    error_zoi_drug_mm_or_mic_µg_ml,
    zoi_np_mm_or_mic_np_µg_ml,
    error_zoi_np_mm_or_mic_np_µg_ml,
    zoi_drug_np_mm_or_mic_drug_np_µg_ml,
    error_zoi_drug_np_mm_or_mic_drug_np_µg_ml,
    fold_increase_in_antibacterial_activity,
    zeta_potential_mv,
    mdr,
    fic,
    effect,
    reference,
    doi,
    article_list,
    time_hr,
    coating_with_antimicrobial_peptide_polymers,
    combined_mic,
    peptide_mic,
    "viability_%" AS viability_percent,
    viability_error,
    journal_name,
    publisher,
    year,
    title,
    journal_is_oa,
    is_oa,
    oa_status,
    pdf,
    access,

    -- Метаданные DBT
    current_timestamp AS dbt_loaded_at,
    'synergy' AS source_table

FROM {{ source('raw', 'synergy') }}
