-- dim_benzimidazoles_bacteria.sql
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (bacteria_id)"
) }}

WITH unique_bacteria_references AS (
    --Находим все уникальные комбинации полей относящихся к бактериям
    SELECT DISTINCT
        bacteria,
        bacteria_unified,
        page_bacteria,
        origin_bacteria,
        section_bacteria,
        subsection_bacteria
    FROM
        {{ ref('final_cur_benzimidazoles') }}
)

-- Генерируем ключ для каждой комбинации
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            bacteria_unified,
            page_bacteria,
            origin_bacteria
    ) AS bacteria_id,

    -- Включаем в справочник ВСЕ поля, относящиеся к бактериям
    bacteria,
    bacteria_unified,
    page_bacteria,
    origin_bacteria,
    section_bacteria,
    subsection_bacteria
FROM
    unique_bacteria_references
