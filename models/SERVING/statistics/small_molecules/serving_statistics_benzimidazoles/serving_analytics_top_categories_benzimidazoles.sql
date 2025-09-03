{{ config(
    materialized='table',
    schema='serving'
) }}

SELECT
    smiles,
    compound_id,
    COUNT(*) AS experiments_count,
    AVG(target_value_parsed) AS avg_target_value,
    MIN(target_value_parsed) AS min_target_value,
    MAX(target_value_parsed) AS max_target_value,
    COUNT(DISTINCT bacteria_id) AS unique_bacteria,
    COUNT(DISTINCT publication_id) AS unique_publications,
    MIN(year) AS first_year,
    MAX(year) AS last_year
FROM
    {{ ref('serving_all_data_benzimidazoles') }}
WHERE
    smiles IS NOT NULL
GROUP BY
    smiles,
    compound_id
ORDER BY
    experiments_count DESC
LIMIT 10
