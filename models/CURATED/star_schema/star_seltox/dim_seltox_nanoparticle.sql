-- Справочник наночастиц для seltox.
-- Каждая строка — уникальная наночастица по nanoparticle_list,
-- с дополнительными характеристиками из seltox.

{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (nanoparticle_id)"
) }}

with first_seltox_row as (
    select
        *,
        row_number() over (
            partition by nanoparticle, normalized_shape, has_coating, np_size_avg_nm
            order by serial_number
        ) as row_num_in_group
    from {{ ref('final_cur_seltox') }}
)

select
    nanoparticle_list.nanoparticle_id,
    nanoparticle_list.nanoparticle,
    nanoparticle_list.normalized_shape,
    nanoparticle_list.has_coating,
    nanoparticle_list.np_size_avg_nm,
    nanoparticle_list.canonical_name,

    -- Дополнительные характеристики (берём только одну строку на каждую частицу)
    first_seltox_row.shape,
    first_seltox_row.coating,
    first_seltox_row.np_size_min_nm,
    first_seltox_row.np_size_max_nm,
    first_seltox_row.hydrodynamic_diameter_nm,
    first_seltox_row.zeta_potential_mv,
    first_seltox_row.synthesis_method,
    first_seltox_row.standardized_synthesis_method,
    first_seltox_row.precursor_of_np,
    first_seltox_row.ph_during_synthesis,
    first_seltox_row.temperature_for_extract_c,
    first_seltox_row.duration_preparing_extract_min,
    first_seltox_row.concentration_of_precursor_mm,
    first_seltox_row.solvent_for_extract

from {{ ref('nanoparticle_list') }} as nanoparticle_list
left join first_seltox_row
    on nanoparticle_list.nanoparticle = first_seltox_row.nanoparticle
    and nanoparticle_list.normalized_shape = first_seltox_row.normalized_shape
    and nanoparticle_list.has_coating = first_seltox_row.has_coating
    and nanoparticle_list.np_size_avg_nm = first_seltox_row.np_size_avg_nm
    and first_seltox_row.row_num_in_group = 1