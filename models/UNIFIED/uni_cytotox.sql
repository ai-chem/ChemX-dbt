-- models/UNIFIED/uni_cytotox.sql
{{ config(
    materialized='view',
    schema='unified',
    unique_key='serial_number'
) }}

SELECT
    -- Все исходные поля без изменений
    sn AS serial_number,
    material AS nanoparticle,
    shape,
    coat_functional_group,
    synthesis_method,
    surface_charge,
    size_in_medium_nm,
    zeta_in_medium_mv AS zeta_potential_mv,
    no_of_cells_cells_well,
    human_animal,
    cell_source,
    cell_tissue,
    cell_morphology,
    cell_age,
    time_hr,
    concentration,
    test,
    test_indicator,
    "viability_%" AS viability_percent,
    doi,
    article_list,
    core_nm,
    hydrodynamic_nm,
    potential_mv,
    cell_type,
    journal_name,
    publisher,
    year,
    title,
    journal_is_oa,

    -- Исправляем опечатку в is_oa и преобразуем в boolean
    CASE
        WHEN UPPER(is_oa) IN ('TRUE', 'T', 'YES', 'Y', '1') THEN TRUE
        WHEN UPPER(is_oa) IN ('FALSE', 'FASLE', 'F', 'NO', 'N', '0') THEN FALSE
        ELSE NULL
    END AS is_oa,

    oa_status,
    pdf,
    access,

    -- Добавляем метаданные
    current_timestamp AS dbt_loaded_at,
    'cytotox' AS source_table
FROM
    {{ source('raw', 'cytotox') }}
