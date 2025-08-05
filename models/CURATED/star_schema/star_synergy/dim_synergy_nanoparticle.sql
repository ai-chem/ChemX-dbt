-- Справочник наночастиц (dim_synergy_nanoparticle).
-- Каждая строка — уникальная наночастица по nanoparticle_list,
-- с дополнительными характеристиками из synergy.

{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (nanoparticle_id)"
) }}

with first_synergy_row as (
    select
        *,
        row_number() over (
            partition by nanoparticle, normalized_shape, has_coating, np_size_avg_nm
            order by serial_number
        ) as row_num_in_group
    from {{ ref('final_cur_synergy') }}
)

select
    nanoparticle_list.nanoparticle_id,
    nanoparticle_list.nanoparticle,
    nanoparticle_list.normalized_shape,
    nanoparticle_list.has_coating,
    nanoparticle_list.np_size_avg_nm,
    nanoparticle_list.canonical_name,

    -- Дополнительные характеристики
    first_synergy_row.shape,
    first_synergy_row.np_size_min_nm,
    first_synergy_row.np_size_max_nm,
    first_synergy_row.np_size_min_nm_parsed,
    first_synergy_row.zeta_potential_mv,
    first_synergy_row.zeta_potential_mv_parsed,
    first_synergy_row.synthesis_method,
    first_synergy_row.standardized_synthesis_method,
    first_synergy_row.coating_with_antimicrobial_peptide_polymers

from {{ ref('nanoparticle_list') }} as nanoparticle_list
left join first_synergy_row
    on nanoparticle_list.nanoparticle = first_synergy_row.nanoparticle
    and nanoparticle_list.normalized_shape = first_synergy_row.normalized_shape
    and nanoparticle_list.has_coating = first_synergy_row.has_coating
    and nanoparticle_list.np_size_avg_nm = first_synergy_row.np_size_avg_nm
    and first_synergy_row.row_num_in_group = 1