{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (source_id)"
) }}

WITH unique_source_references AS (
    SELECT DISTINCT
        source_table,
        dbt_loaded_at,
        dbt_curated_at
    FROM
        {{ ref('final_cur_complexes') }}
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            source_table,
            dbt_loaded_at
    ) AS source_id,

    source_table,
    dbt_loaded_at,
    dbt_curated_at
FROM
    unique_source_references
