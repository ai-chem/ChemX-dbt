--dim_benzimidazoles_publications.sql
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (publication_id)"
) }}

WITH unique_publication_references AS (
    SELECT DISTINCT
        doi,
        title,
        publisher,
        year,
        access,
        pdf
    FROM
        {{ ref('final_cur_benzimidazoles') }}
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            doi,
            year
    ) AS publication_id,

    doi,
    title,
    publisher,
    year,
    access,
    pdf
FROM
    unique_publication_references
