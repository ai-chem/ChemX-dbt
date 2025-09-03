{{ config(
    materialized='table',
    schema='serving',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (serial_number)"
) }}

select
    fact.serial_number,

    -- Все поля из справочника наночастиц
    np.nanoparticle_id,
    np.nanoparticle,
    np.normalized_shape,
    np.shape,
    np.has_coating,
    np.np_size_avg_nm,
    np.canonical_name,
    np.coat_functional_group,
    np.standardized_synthesis_method,
    np.is_surface_positive,
    np.potential_mv,
    np.hydrodynamic_nm,
    np.zeta_potential_mv,
    np.size_in_medium_nm,
    np.surface_charge,
    np.synthesis_method,

    -- Все поля из справочника клеточных линий
    cell.cell_line_id,
    cell.canonical_cell_line_name,
    cell.cell_type,
    cell.human_animal,
    cell.cell_source,
    cell.cell_tissue,
    cell.cell_morphology,
    cell.cell_age,
    cell.is_human,

    -- Все поля из справочника публикаций
    pub.publication_id,
    pub.doi,
    pub.article_list,
    pub.journal_name,
    pub.publisher,
    pub.year,
    pub.title,
    pub.journal_is_oa,
    pub.is_oa,
    pub.oa_status,
    pub.pdf,
    pub.access,
    pub.access_bool,

    -- Все поля из справочника источников
    src.source_id,
    src.source_table,
    src.dbt_loaded_at,
    src.dbt_curated_at,

    -- Все экспериментальные параметры из fact
    fact.no_of_cells_cells_well,
    fact.time_hr,
    fact.concentration,
    fact.test,
    fact.test_indicator,
    fact.viability_percent

from {{ ref('fact_cytotox_experiments') }} as fact

left join {{ ref('dim_cytotox_nanoparticle') }} as np
    on fact.nanoparticle_id = np.nanoparticle_id

left join {{ ref('dim_cytotox_cell_line') }} as cell
    on fact.cell_line_id = cell.cell_line_id

left join {{ ref('dim_cytotox_publication') }} as pub
    on fact.publication_id = pub.publication_id

left join {{ ref('dim_cytotox_source') }} as src
    on fact.source_id = src.source_id
