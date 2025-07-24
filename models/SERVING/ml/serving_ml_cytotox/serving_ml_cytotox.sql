{{ config(materialized='table', schema='serving') }}

with fact as (
    select *
    from {{ ref('fact_cytotox_experiments') }}
),

np as (
    select
        nanoparticle_id,
        nanoparticle,
        shape,
        coat_functional_group,
        synthesis_method,
        surface_charge,
        size_in_medium_nm,
        zeta_potential_mv,
        core_nm,
        hydrodynamic_nm,
        potential_mv,
        is_surface_positive,
        normalized_shape,
        standardized_synthesis_method,
        np_size_avg_nm,
        has_coating
    from {{ ref('dim_cytotox_nanoparticle') }}
),

cell as (
    select
        cell_line_id,
        cell_type,
        cell_source,
        cell_tissue,
        cell_morphology,
        cell_age,
        is_human
    from {{ ref('dim_cytotox_cell_line') }}
)

select
    fact.serial_number,
    np.nanoparticle,
    np.shape,
    np.coat_functional_group,
    np.synthesis_method,
    np.surface_charge,
    np.size_in_medium_nm,
    np.zeta_potential_mv,
    np.hydrodynamic_nm,
    np.potential_mv,
    np.is_surface_positive,
    np.normalized_shape,
    np.standardized_synthesis_method,
    np.np_size_avg_nm,
    np.has_coating,

    fact.no_of_cells_cells_well,
    fact.time_hr,
    fact.concentration,
    fact.test,
    fact.test_indicator,
    cell.cell_source,
    cell.cell_tissue,
    cell.cell_morphology,
    cell.cell_age,
    cell.cell_type,
    cell.is_human,
    fact.viability_percent

from fact
left join np on fact.nanoparticle_id = np.nanoparticle_id
left join cell on fact.cell_line_id = cell.cell_line_id