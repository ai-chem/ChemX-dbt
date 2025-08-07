-- models/UNIFIED/uni_complexes.sql
{{ config(
    materialized='view',
    schema='unified'
) }}

SELECT
    -- Все исходные поля без изменений
    pdf,
    doi,
    doi_sourse,
    supplementary,
    title,
    publisher,
    year,
    access,
    compound_id,
    compound_name,
    smiles,
    smiles_type,
    metal,

    -- Преобразуем target в числовой тип, заменяя запятую на точку
    CASE
        WHEN target IS NULL OR TRIM(target) = '' THEN NULL
        WHEN target ~ '^[0-9]+,[0-9]+$' THEN CAST(REPLACE(target, ',', '.') AS FLOAT)
        WHEN target ~ '^[0-9]+\.[0-9]+$' THEN CAST(target AS FLOAT)
        WHEN target ~ '^[0-9]+$' THEN CAST(target AS FLOAT)
        ELSE NULL
    END AS target_numeric,

    -- Сохраняем исходное значение для справки
    target AS target_original,

    page_smiles,
    origin_smiles,
    page_metal,
    origin_metal,
    CAST(page_target_value AS INTEGER) AS page_target_value, -- Преобразуем в INTEGER
    origin_target_value,

    -- Добавляем метаданные
    current_timestamp AS dbt_loaded_at,
    'complexes' AS source_table
FROM
    {{ source('raw', 'complexes') }}
