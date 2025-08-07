-- models/UNIFIED/uni_eyedrops.sql
{{ config(
    materialized='view',
    schema='unified'
) }}

SELECT
    -- Все исходные поля без изменений
    smiles,
    name,
    "perm_(cm/s)" AS perm_original, -- Сохраняем исходное значение

    -- Преобразуем perm_(cm/s) в числовой тип, заменяя запятую на точку
    CASE
        WHEN "perm_(cm/s)" IS NULL OR TRIM("perm_(cm/s)") = '' THEN NULL
        WHEN "perm_(cm/s)" ~ '^-?[0-9]+,[0-9]+$' THEN CAST(REPLACE("perm_(cm/s)", ',', '.') AS FLOAT)
        WHEN "perm_(cm/s)" ~ '^-?[0-9]+\.[0-9]+$' THEN CAST("perm_(cm/s)" AS FLOAT)
        WHEN "perm_(cm/s)" ~ '^-?[0-9]+$' THEN CAST("perm_(cm/s)" AS FLOAT)
        ELSE NULL
    END AS perm_numeric,

    logp AS logp_original, -- Сохраняем исходное значение

    -- Преобразуем logp в числовой тип, заменяя запятую на точку
    CASE
        WHEN logp IS NULL OR TRIM(logp) = '' THEN NULL
        WHEN logp ~ '^-[0-9]+,[0-9]+$' THEN CAST(REPLACE(logp, ',', '.') AS FLOAT)
        WHEN logp ~ '^-[0-9]+\.[0-9]+$' THEN CAST(logp AS FLOAT)
        WHEN logp ~ '^-[0-9]+$' THEN CAST(logp AS FLOAT)
        WHEN logp ~ '^[0-9]+,[0-9]+$' THEN CAST(REPLACE(logp, ',', '.') AS FLOAT)
        WHEN logp ~ '^[0-9]+\.[0-9]+$' THEN CAST(logp AS FLOAT)
        WHEN logp ~ '^[0-9]+$' THEN CAST(logp AS FLOAT)
        ELSE NULL
    END AS logp_numeric,

    doi,
    CAST(pmid AS BIGINT) AS pmid, -- Преобразуем в BIGINT
    title,
    publisher,
    year,
    access,
    page,
    origin,

    -- Добавляем метаданные
    current_timestamp AS dbt_loaded_at,
    'eyedrops' AS source_table
FROM
    {{ source('raw', 'eyedrops') }}
