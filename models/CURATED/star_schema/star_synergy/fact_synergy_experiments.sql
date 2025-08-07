{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (serial_number)"
) }}

with publication as (
    select
        doi,
        publication_id
    from {{ ref('dim_synergy_publication') }}
),

bacteria as (
    select
        bacteria,
        strain,
        mdr,
        bacteria_id
    from {{ ref('dim_synergy_bacteria') }}
),

drug as (
    select
        drug,
        drug_id
    from {{ ref('dim_synergy_drug') }}
),

source as (
    select
        source_table,
        dbt_loaded_at,
        source_id
    from {{ ref('dim_synergy_source') }}
)

select
    final_cur_synergy.serial_number,
    final_cur_synergy.nanoparticle_id,  -- если nanoparticle_id уже присвоен на этапе подготовки данных
    bacteria.bacteria_id,
    drug.drug_id,
    publication.publication_id,
    source.source_id,

    final_cur_synergy.method,
    final_cur_synergy.zoi_drug_mm_or_mic__µg_ml,
    final_cur_synergy.error_zoi_drug_mm_or_mic_µg_ml,
    final_cur_synergy.zoi_np_mm_or_mic_np_µg_ml,
    final_cur_synergy.error_zoi_np_mm_or_mic_np_µg_ml,
    final_cur_synergy.zoi_drug_np_mm_or_mic_drug_np_µg_ml,
    final_cur_synergy.error_zoi_drug_np_mm_or_mic_drug_np_µg_ml,
    final_cur_synergy.fold_increase_in_antibacterial_activity,
    final_cur_synergy.fic,
    final_cur_synergy.effect,
    final_cur_synergy.drug_dose_µg_disk,
    final_cur_synergy.np_concentration_µg_ml,
    final_cur_synergy.combined_mic,
    final_cur_synergy.peptide_mic,
    final_cur_synergy.viability_percent,
    final_cur_synergy.viability_error,
    final_cur_synergy.time_hr

from {{ ref('final_cur_synergy') }} as final_cur_synergy

left join publication
    on final_cur_synergy.doi = publication.doi

left join bacteria
    on final_cur_synergy.bacteria IS NOT DISTINCT FROM bacteria.bacteria
    and final_cur_synergy.strain IS NOT DISTINCT FROM bacteria.strain
    and final_cur_synergy.mdr IS NOT DISTINCT FROM bacteria.mdr

left join drug
    on final_cur_synergy.drug IS NOT DISTINCT FROM drug.drug

left join source
    on final_cur_synergy.source_table = source.source_table
    and final_cur_synergy.dbt_loaded_at = source.dbt_loaded_at
