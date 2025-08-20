-- models/UNIFIED/uni_oxazolidinones.sql
{{ config(
    materialized='view',
    schema='unified'
) }}

SELECT
    -- Все исходные поля без изменений
    pdf,
    smiles,
    doi,
    title,
    publisher,
    year,
    access,
    compound_id,
    target_type,
    target_relation,
    target_value,
    target_units,
    bacteria,
    bacteria_name_unified,
    bacteria_info,
    page_bacteria,
    origin_bacteria,
    section_bacteria,
    subsection_bacteria,
    page_target,
    origin_target,
    section_target,
    column_prop,
    line_prop,
    page_scaffold,
    origin_scaffold,
    section_scaffold,
    page_residue,
    origin_residue,
    section_residue,

    -- Добавляем метаданные
    current_timestamp AS dbt_loaded_at,
    'oxazolidinones' AS source_table
FROM
    {{ source('raw', 'oxazolidinones') }}
