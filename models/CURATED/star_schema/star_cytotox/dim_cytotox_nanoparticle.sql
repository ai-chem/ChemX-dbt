-- Справочник наночастиц
-- Каждая строка уникальная наночастица (по nanoparticle_list)
-- К основным полям добавляются дополнительные характеристики из экспериментов (берём первую попавшуюся строку для каждой частицы потому что они одинаковы, меняются только параметры экспериментов).

{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (nanoparticle_id)"
) }}

with first_cytotox_row as (
    select
        *,
        row_number() over (
            partition by nanoparticle, normalized_shape, has_coating, np_size_avg_nm
            order by serial_number
        ) as row_num_in_group
    from {{ ref('final_cur_cytotox') }}
)

select
    nanoparticle_list.nanoparticle_id,
    nanoparticle_list.nanoparticle,
    nanoparticle_list.normalized_shape,
    nanoparticle_list.has_coating,
    nanoparticle_list.np_size_avg_nm,
    nanoparticle_list.canonical_name,

    -- Дополнительные характеристики (берём только одну строку на каждую частицу)
    first_cytotox_row.coat_functional_group,
    first_cytotox_row.standardized_synthesis_method,
    first_cytotox_row.is_surface_positive,
    first_cytotox_row.potential_mv,
    first_cytotox_row.hydrodynamic_nm,
    first_cytotox_row.core_nm,
    first_cytotox_row.shape,
    first_cytotox_row.zeta_potential_mv,
    first_cytotox_row.size_in_medium_nm,
    first_cytotox_row.surface_charge,
    first_cytotox_row.synthesis_method

from {{ ref('nanoparticle_list') }} as nanoparticle_list
left join first_cytotox_row
    on nanoparticle_list.nanoparticle = first_cytotox_row.nanoparticle
    and nanoparticle_list.normalized_shape = first_cytotox_row.normalized_shape
    and nanoparticle_list.has_coating = first_cytotox_row.has_coating
    and nanoparticle_list.np_size_avg_nm = first_cytotox_row.np_size_avg_nm
    and first_cytotox_row.row_num_in_group = 1