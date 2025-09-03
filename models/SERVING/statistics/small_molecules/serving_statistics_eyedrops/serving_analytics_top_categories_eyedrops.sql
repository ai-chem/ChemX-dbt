{{ config(
    materialized='table',
    schema='serving'
) }}

SELECT
    smiles,
    name,
    COUNT(*) AS measurements_count,
    AVG(perm_numeric) AS avg_permeability,
    AVG(logp_numeric) AS avg_logp,
    COUNT(DISTINCT publication_id) AS unique_publications,
    MIN(year) AS first_year,
    MAX(year) AS last_year
FROM
    {{ ref('serving_all_data_eyedrops') }}
WHERE
    smiles IS NOT NULL
GROUP BY
    smiles,
    name
ORDER BY
    measurements_count DESC
LIMIT 10
