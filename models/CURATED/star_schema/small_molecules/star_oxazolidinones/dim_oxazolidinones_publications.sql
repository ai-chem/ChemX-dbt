{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (publication_id)"
) }}

WITH unique_publication_references AS (
    SELECT DISTINCT
        pdf,
        doi,
        title,
        publisher,
        year,
        access,
        access_bool
    FROM
        {{ ref('final_cur_oxazolidinones') }}
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            doi,
            year
    ) AS publication_id,

    pdf,
    doi,
    title,
    publisher,
    year,
    access,
    access_bool
FROM
    unique_publication_references
