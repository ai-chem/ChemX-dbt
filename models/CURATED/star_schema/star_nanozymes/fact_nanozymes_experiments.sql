{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (id)"
) }}

with publication as (
    select
        doi,
        publication_id
    from {{ ref('dim_nanozymes_publication') }}
),

nanoparticle as (
    select
        nanoparticle,
        syngony,
        surface,
        length_lower,
        length_upper,
        length_mean,
        width_lower,
        width_upper,
        width_mean,
        depth_lower,
        depth_upper,
        depth_mean,
        nanoparticle_id
    from {{ ref('dim_nanozymes_nanoparticle') }}
),

source as (
    select
        source_table,
        dbt_loaded_at,
        dbt_curated_at,
        source_id
    from {{ ref('dim_nanozymes_source') }}
)

select
    final_cur_nanozymes.id,

    nanoparticle.nanoparticle_id,
    publication.publication_id,
    source.source_id,

    final_cur_nanozymes.activity,
    final_cur_nanozymes.reaction_type,
    final_cur_nanozymes.target_source,
    final_cur_nanozymes.c_min,
    final_cur_nanozymes.c_max,
    final_cur_nanozymes.c_const,
    final_cur_nanozymes.c_const_unit,
    final_cur_nanozymes.ccat_value,
    final_cur_nanozymes.ccat_unit,
    final_cur_nanozymes.km_value,
    final_cur_nanozymes.km_unit,
    final_cur_nanozymes.vmax_value,
    final_cur_nanozymes.vmax_unit,
    final_cur_nanozymes.ph,
    final_cur_nanozymes.temperature

from {{ ref('final_cur_nanozymes') }} as final_cur_nanozymes

left join nanoparticle
    on final_cur_nanozymes.nanoparticle IS NOT DISTINCT FROM nanoparticle.nanoparticle
    and final_cur_nanozymes.syngony IS NOT DISTINCT FROM nanoparticle.syngony
    and final_cur_nanozymes.surface IS NOT DISTINCT FROM nanoparticle.surface
    and final_cur_nanozymes.length_lower IS NOT DISTINCT FROM nanoparticle.length_lower
    and final_cur_nanozymes.length_upper IS NOT DISTINCT FROM nanoparticle.length_upper
    and final_cur_nanozymes.length_mean IS NOT DISTINCT FROM nanoparticle.length_mean
    and final_cur_nanozymes.width_lower IS NOT DISTINCT FROM nanoparticle.width_lower
    and final_cur_nanozymes.width_upper IS NOT DISTINCT FROM nanoparticle.width_upper
    and final_cur_nanozymes.width_mean IS NOT DISTINCT FROM nanoparticle.width_mean
    and final_cur_nanozymes.depth_lower IS NOT DISTINCT FROM nanoparticle.depth_lower
    and final_cur_nanozymes.depth_upper IS NOT DISTINCT FROM nanoparticle.depth_upper
    and final_cur_nanozymes.depth_mean IS NOT DISTINCT FROM nanoparticle.depth_mean

left join publication
    on final_cur_nanozymes.doi = publication.doi

left join source
    on final_cur_nanozymes.source_table = source.source_table
    and final_cur_nanozymes.dbt_loaded_at = source.dbt_loaded_at
    and final_cur_nanozymes.dbt_curated_at = source.dbt_curated_at