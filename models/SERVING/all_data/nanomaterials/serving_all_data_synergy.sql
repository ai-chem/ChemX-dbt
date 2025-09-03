{{ config(
    materialized='table',
    schema='serving',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (serial_number)"
) }}

select
    fact.serial_number,

    -- nanoparticle
    np.nanoparticle_id,
    np.nanoparticle,
    np.normalized_shape,
    np.has_coating,
    np.np_size_avg_nm,
    np.canonical_name,
    np.shape,
    np.np_size_min_nm,
    np.np_size_max_nm,
    np.np_size_min_nm_parsed,
    np.zeta_potential_mv,
    np.zeta_potential_mv_parsed,
    np.synthesis_method,
    np.standardized_synthesis_method,
    np.coating_with_antimicrobial_peptide_polymers,

    -- bacteria
    bact.bacteria_id,
    bact.bacteria,
    bact.strain,
    bact.mdr,

    -- drug
    drg.drug_id,
    drg.drug,

    -- publication
    pub.publication_id,
    pub.doi,
    pub.article_list,
    pub.journal_name,
    pub.publisher,
    pub.year,
    pub.title,
    pub.journal_is_oa,
    pub.is_oa,
    pub.oa_status,
    pub.pdf,
    pub.access,
    pub.access_bool,
    pub.reference,

    -- source
    src.source_id,
    src.source_table,
    src.dbt_loaded_at,

    -- fact fields
    fact.method,
    fact.zoi_drug_mm_or_mic__µg_ml,
    fact.error_zoi_drug_mm_or_mic_µg_ml,
    fact.zoi_np_mm_or_mic_np_µg_ml,
    fact.error_zoi_np_mm_or_mic_np_µg_ml,
    fact.zoi_drug_np_mm_or_mic_drug_np_µg_ml,
    fact.error_zoi_drug_np_mm_or_mic_drug_np_µg_ml,
    fact.fold_increase_in_antibacterial_activity,
    fact.fic,
    fact.effect,
    fact.drug_dose_µg_disk,
    fact.np_concentration_µg_ml,
    fact.combined_mic,
    fact.peptide_mic,
    fact.viability_percent,
    fact.viability_error,
    fact.time_hr

from {{ ref('fact_synergy_experiments') }} as fact

left join {{ ref('dim_synergy_nanoparticle') }} as np
    on fact.nanoparticle_id = np.nanoparticle_id

left join {{ ref('dim_synergy_bacteria') }} as bact
    on fact.bacteria_id = bact.bacteria_id

left join {{ ref('dim_synergy_drug') }} as drg
    on fact.drug_id = drg.drug_id

left join {{ ref('dim_synergy_publication') }} as pub
    on fact.publication_id = pub.publication_id

left join {{ ref('dim_synergy_source') }} as src
    on fact.source_id = src.source_id
