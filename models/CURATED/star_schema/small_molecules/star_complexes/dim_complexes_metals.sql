-- dim_complexes_metals.sql
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (metal_id)"
) }}

WITH unique_metal_references AS (
    SELECT DISTINCT
        metal,
        page_metal,
        origin_metal
    FROM
        {{ ref('final_cur_complexes') }}
    WHERE
        metal IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            metal,
            page_metal,
            origin_metal
    ) AS metal_id,

    metal,
    page_metal,
    origin_metal
FROM
    unique_metal_references
