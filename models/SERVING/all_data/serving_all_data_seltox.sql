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
    np.coating,
    np.np_size_min_nm,
    np.np_size_max_nm,
    np.hydrodynamic_diameter_nm,
    np.zeta_potential_mv,
    np.synthesis_method,
    np.standardized_synthesis_method,
    np.precursor_of_np,
    np.ph_during_synthesis,
    np.temperature_for_extract_c,
    np.duration_preparing_extract_min,
    np.concentration_of_precursor_mm,
    np.solvent_for_extract,

    -- bacteria
    bact.bacteria_id,
    bact.bacteria,
    bact.strain,
    bact.mdr,

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
    pub.reference,

    -- source
    src.source_id,
    src.source_table,
    src.dbt_loaded_at,

    -- fact fields
    fact.method,
    fact.mic_np_µg_ml,
    fact.mic_np_µg_ml_parsed,
    fact.concentration,
    fact.zoi_np_mm,
    fact.time_set_hours

from {{ ref('fact_seltox_experiments') }} as fact

left join {{ ref('dim_seltox_nanoparticle') }} as np
    on fact.nanoparticle_id = np.nanoparticle_id

left join {{ ref('dim_seltox_bacteria') }} as bact
    on fact.bacteria_id = bact.bacteria_id

left join {{ ref('dim_seltox_publication') }} as pub
    on fact.publication_id = pub.publication_id

left join {{ ref('dim_seltox_source') }} as src
    on fact.source_id = src.source_id