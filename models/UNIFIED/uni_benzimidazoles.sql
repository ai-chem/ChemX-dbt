-- models/UNIFIED/uni_benzimidazoles.sql
{{ config(
    materialized='view',
    schema='unified'
) }}

SELECT
    -- Все исходные поля без изменений
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
    bacteria_unified,
    page_bacteria,
    origin_bacteria,
    section_bacteria,
    subsection_bacteria,
    page_target,
    origin_target,
    section_target,
    subsection_target,
    page_scaffold,
    origin_scaffold,
    CAST(page_residue AS INTEGER) AS page_residue, -- Преобразуем в INTEGER
    origin_residue,
    pdf,

    -- Добавляем метаданные
    current_timestamp AS dbt_loaded_at,
    'benzimidazoles' AS source_table
FROM
    {{ source('raw', 'benzimidazoles') }}
