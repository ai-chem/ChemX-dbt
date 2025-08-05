{{ config(materialized='table', schema='serving') }}

with joined_data as (
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
    from {{ ref('fact_cytotox_experiments') }} as fact
    left join {{ ref('dim_cytotox_nanoparticle') }} as np on fact.nanoparticle_id = np.nanoparticle_id
    left join {{ ref('dim_cytotox_cell_line') }} as cell on fact.cell_line_id = cell.cell_line_id
)

select
    -- Столбцы, которые не трогаем
    serial_number,
    nanoparticle,
    normalized_shape,
    has_coating,
    time_hr,
    cell_type,

    -- ====================================================================
    -- Обработка ЧИСЛОВЫХ колонок
    -- ====================================================================
    {{ impute_numeric('potential_mv') }},
    {{ impute_numeric('zeta_potential_mv') }},
    {{ impute_numeric('hydrodynamic_nm') }},
    {{ impute_numeric('size_in_medium_nm') }},
    {{ impute_numeric('np_size_avg_nm') }},
    {{ impute_numeric('no_of_cells_cells_well') }},
    {{ impute_numeric('concentration') }},
    {{ impute_numeric('viability_percent') }},

    -- ====================================================================
    -- Обработка ТЕКСТОВЫХ колонок
    -- ====================================================================
    {{ impute_categorical('shape') }},
    {{ impute_categorical('coat_functional_group') }},
    {{ impute_categorical('synthesis_method') }},
    {{ impute_categorical('surface_charge') }},
    {{ impute_categorical('test') }},
    {{ impute_categorical('test_indicator') }},
    {{ impute_categorical('cell_source') }},
    {{ impute_categorical('cell_tissue') }},
    {{ impute_categorical('cell_morphology') }},
    {{ impute_categorical('cell_age') }},
    {{ impute_categorical('standardized_synthesis_method') }},

    -- ====================================================================
    -- Обработка БУЛЕВЫХ колонок
    -- ====================================================================
    {{ impute_boolean('is_surface_positive') }},
    {{ impute_boolean('is_human') }}

from joined_data