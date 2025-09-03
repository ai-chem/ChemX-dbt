-- dim_cocrystals_publications.sql
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (publication_id)"
) }}

WITH unique_publication_references AS (
    -- Находим все уникальные комбинации полей, относящихся к публикациям
    SELECT DISTINCT
        pdf,
        doi,
        supplementary,
        authors,
        title,
        journal,
        year,
        page,
        access
    FROM
        {{ ref('final_cur_co_crystals') }}
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            doi,
            year
    ) AS publication_id,

    -- Включаем в справочник ВСЕ поля относящиеся к публикациям
    pdf,
    doi,
    supplementary,
    authors,
    title,
    journal,
    year,
    page,
    access
FROM
    unique_publication_references
