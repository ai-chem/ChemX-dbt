{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (publication_id)"
) }}

WITH unique_publication_references AS (
    SELECT DISTINCT
        doi,
        pmid,
        title,
        publisher,
        year,
        access
    FROM
        {{ ref('final_cur_eyedrops') }}
)
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            doi,
            pmid,
            year
    ) AS publication_id,
    doi,
    pmid,
    title,
    publisher,
    year,
    access
FROM
    unique_publication_references
