{{ config(
    materialized='table',
    schema='serving',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (id)"
) }}


select
    -- fact
    fact.id,
    fact.zfc_h_meas,
    fact.htherm_sar,
    fact.mri_r1,
    fact.mri_r2,
    fact.squid_h_max,
    fact.hc_koe,
    fact.fc_field_t_numeric,
    fact.squid_temperature_numeric,
    fact.squid_sat_mag_numeric,
    fact.coercivity_numeric,
    fact.squid_rem_mag_numeric,
    fact.exchange_bias_shift_oe_numeric,
    fact.vertical_loop_shift_m_vsl_emu_g_numeric,

    -- nanoparticle
    np.nanoparticle_id,
    np.name,
    np.np_shell_2,
    np.np_hydro_size,
    np.xrd_scherrer_size,
    np.emic_size,
    np.instrument,
    np.core_shell_formula,
    np.nanoparticle,
    np.np_shell,
    np.space_group_core,
    np.space_group_shell,
    np.fc_field_t_original,
    np.squid_temperature_original,
    np.squid_sat_mag_original,
    np.coercivity_original,
    np.squid_rem_mag_original,
    np.exchange_bias_shift_oe_original,
    np.vertical_loop_shift_m_vsl_emu_g_original,

    -- publication
    pub.publication_id,
    pub.doi,
    pub.journal,
    pub.publisher,
    pub.year,
    pub.title,
    pub.pdf,
    pub.access,
    pub.access_bool,
    pub.supp,
    pub.article_name_folder,
    pub.supp_info_name_folder,

    -- validation
    val.validation_id,
    val.comment,
    val.has_mistake_in_matadata,
    val.verification_required,
    val.verified_by,
    val.verification_date,

    -- source
    src.source_id,
    src.source_table,
    src.dbt_loaded_at,
    src.dbt_curated_at

from {{ ref('fact_nanomag_experiments') }} as fact

left join {{ ref('dim_nanomag_nanoparticle') }} as np
    on fact.nanoparticle_id = np.nanoparticle_id

left join {{ ref('dim_nanomag_publication') }} as pub
    on fact.publication_id = pub.publication_id

left join {{ ref('dim_nanomag_validation') }} as val
    on fact.validation_id = val.validation_id

left join {{ ref('dim_nanomag_source') }} as src
    on fact.source_id = src.source_id