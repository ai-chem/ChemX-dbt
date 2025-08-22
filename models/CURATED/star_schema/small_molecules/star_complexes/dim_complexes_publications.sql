
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (publication_id)"
) }}

WITH unique_publication_references AS (
    SELECT DISTINCT
        pdf,
        doi,
        doi_sourse,
        supplementary,
        supplementary_bool,
        title,
        publisher,
        year,
        access
    FROM
        {{ ref('final_cur_complexes') }}
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            doi,
            year
    ) AS publication_id,

    pdf,
    doi,
    doi_sourse,
    supplementary,
    supplementary_bool,
    title,
    publisher,
    year,
    access
FROM
    unique_publication_references
