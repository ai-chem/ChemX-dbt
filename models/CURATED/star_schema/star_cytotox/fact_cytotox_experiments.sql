{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (serial_number)"
) }}

with pub as (
    select
        doi,
        publication_id
    from {{ ref('dim_cytotox_publication') }}
),

cell_line as (
    select
        canonical_cell_line_name,
        cell_line_id
    from {{ ref('dim_cytotox_cell_line') }}
),

nanoparticle as (
    select
        nanoparticle,
        normalized_shape,
        has_coating,
        np_size_avg_nm,
        nanoparticle_id
    from {{ ref('dim_cytotox_nanoparticle') }}
),

source as (
    select
        source_table,
        dbt_loaded_at,
        dbt_curated_at,
        source_id
    from {{ ref('dim_cytotox_source') }}
)

select
    final_cur_cytotox.serial_number,
    nanoparticle.nanoparticle_id,  -- теперь берём id через join со справочником!
    pub.publication_id,
    cell_line.cell_line_id,
    source.source_id,

    final_cur_cytotox.no_of_cells_cells_well,
    final_cur_cytotox.time_hr,
    final_cur_cytotox.concentration,
    final_cur_cytotox.test,
    final_cur_cytotox.test_indicator,
    final_cur_cytotox.viability_percent

from {{ ref('final_cur_cytotox') }} as final_cur_cytotox

left join nanoparticle
    on final_cur_cytotox.nanoparticle IS NOT DISTINCT FROM nanoparticle.nanoparticle
    and final_cur_cytotox.normalized_shape IS NOT DISTINCT FROM nanoparticle.normalized_shape
    and final_cur_cytotox.has_coating IS NOT DISTINCT FROM nanoparticle.has_coating
    and final_cur_cytotox.np_size_avg_nm IS NOT DISTINCT FROM nanoparticle.np_size_avg_nm

left join pub
    on final_cur_cytotox.doi = pub.doi

left join cell_line
    on {{ generate_cell_line_name('final_cur_cytotox') }} = cell_line.canonical_cell_line_name

left join source
    on final_cur_cytotox.source_table = source.source_table
    and final_cur_cytotox.dbt_loaded_at = source.dbt_loaded_at
    and final_cur_cytotox.dbt_curated_at = source.dbt_curated_at