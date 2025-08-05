{{ config(
    materialized='table',
    schema='serving',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (id)"
) }}

select
    -- fact_nanozymes_experiments
    fact.id,
    fact.nanoparticle_id,
    fact.publication_id,
    fact.source_id,
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
    fact.temperature,

    -- dim_nanozymes_nanoparticle
    np.nanoparticle,
    np.syngony,
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
    np.length,
    np.width,
    np.depth,

    -- dim_nanozymes_publication
    pub.doi,
    pub.journal,
    pub.year,
    pub.title,
    pub.pdf,
    pub.access,
    pub.access_bool,

    -- dim_nanozymes_source
    src.source_table,
    src.dbt_loaded_at,
    src.dbt_curated_at

from {{ ref('fact_nanozymes_experiments') }} as fact

left join {{ ref('dim_nanozymes_nanoparticle') }} as np
    on fact.nanoparticle_id = np.nanoparticle_id

left join {{ ref('dim_nanozymes_publication') }} as pub
    on fact.publication_id = pub.publication_id

left join {{ ref('dim_nanozymes_source') }} as src
    on fact.source_id = src.source_id