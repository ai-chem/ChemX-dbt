{{ config(materialized='table', schema='serving') }}

with fact as (
    select *
    from {{ ref('fact_synergy_experiments') }}
),

np as (
    select
        nanoparticle_id,
        nanoparticle,
        shape,
        has_coating,
        normalized_shape,
        standardized_synthesis_method,
        np_size_min_nm,
        np_size_max_nm,
        np_size_avg_nm,
        zeta_potential_mv,
        zeta_potential_mv_parsed,
        coating_with_antimicrobial_peptide_polymers
    from {{ ref('dim_synergy_nanoparticle') }}
),

bact as (
    select
        bacteria_id,
        bacteria,
        mdr,
        strain
    from {{ ref('dim_synergy_bacteria') }}
),

drg as (
    select
        drug_id,
        drug
    from {{ ref('dim_synergy_drug') }}
)

select
    fact.serial_number,
    np.nanoparticle,
    np.shape,
    np.has_coating,
    np.normalized_shape,
    np.standardized_synthesis_method,
    np.np_size_min_nm,
    np.np_size_max_nm,
    np.np_size_avg_nm,
    np.zeta_potential_mv,
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
    bact.mdr,
    bact.strain

from fact
left join np on fact.nanoparticle_id = np.nanoparticle_id
left join bact on fact.bacteria_id = bact.bacteria_id
left join drg on fact.drug_id = drg.drug_id