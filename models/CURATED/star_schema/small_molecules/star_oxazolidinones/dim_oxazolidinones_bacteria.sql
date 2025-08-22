{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (bacteria_id)"
) }}

WITH unique_bacteria_references AS (
    SELECT DISTINCT
        bacteria,
        bacteria_name_unified,
        bacteria_info,
        page_bacteria,
        origin_bacteria,
        section_bacteria,
        subsection_bacteria
    FROM
        {{ ref('final_cur_oxazolidinones') }}
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            bacteria_name_unified,
            page_bacteria,
            origin_bacteria
    ) AS bacteria_id,

    bacteria,
    bacteria_name_unified,
    bacteria_info,
    page_bacteria,
    origin_bacteria,
    section_bacteria,
    subsection_bacteria
FROM
    unique_bacteria_references
