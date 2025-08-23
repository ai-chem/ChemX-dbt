{{ config(
    materialized='table',
    schema='serving'
) }}

SELECT
    metal,
    COUNT(*) AS experiments_count,
    AVG(target_numeric) AS avg_target_value,
    MIN(target_numeric) AS min_target_value,
    MAX(target_numeric) AS max_target_value,
    COUNT(DISTINCT molecule_id) AS unique_molecules,
    COUNT(DISTINCT publication_id) AS unique_publications,
    MIN(year) AS first_year,
    MAX(year) AS last_year
FROM
    {{ ref('serving_all_data_complexes') }}
WHERE
    metal IS NOT NULL
GROUP BY
    metal
ORDER BY
    experiments_count DESC
LIMIT 10
