{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (serial_number)"
) }}

with publication as (
    select
        doi,
        publication_id
    from {{ ref('dim_seltox_publication') }}
),

bacteria as (
    select
        bacteria,
        strain,
        mdr,
        bacteria_id
    from {{ ref('dim_seltox_bacteria') }}
),

source as (
    select
        source_table,
        dbt_loaded_at,
        source_id
    from {{ ref('dim_seltox_source') }}
),

nanoparticle as (
    select
        nanoparticle_id
    from {{ ref('dim_seltox_nanoparticle') }}
)

select
    final_cur_seltox.serial_number,
    final_cur_seltox.nanoparticle_id,  -- если nanoparticle_id уже присвоен на этапе подготовки данных
    bacteria.bacteria_id,
    publication.publication_id,
    source.source_id,

    final_cur_seltox.method,
    final_cur_seltox.mic_np_µg_ml,
    final_cur_seltox.mic_np_µg_ml_parsed,
    final_cur_seltox.concentration,
    final_cur_seltox.zoi_np_mm,
    final_cur_seltox.time_set_hours

from {{ ref('final_cur_seltox') }} as final_cur_seltox

left join publication
    on final_cur_seltox.doi = publication.doi

left join bacteria
    on final_cur_seltox.bacteria IS NOT DISTINCT FROM bacteria.bacteria
    and final_cur_seltox.strain IS NOT DISTINCT FROM bacteria.strain
    and final_cur_seltox.mdr IS NOT DISTINCT FROM bacteria.mdr

left join source
    on final_cur_seltox.source_table = source.source_table
    and final_cur_seltox.dbt_loaded_at = source.dbt_loaded_at