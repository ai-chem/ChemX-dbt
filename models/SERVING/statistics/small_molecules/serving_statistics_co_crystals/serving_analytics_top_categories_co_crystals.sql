{{ config(
    materialized='table',
    schema='serving'
) }}

SELECT
    name_cocrystal,
    COUNT(*) AS experiments_count,
    -- Считаем количество экспериментов для каждого исхода
    SUM(CASE WHEN photostability_change = 'increases' THEN 1 ELSE 0 END) as count_increases,
    SUM(CASE WHEN photostability_change = 'decreases' THEN 1 ELSE 0 END) as count_decreases,
    SUM(CASE WHEN photostability_change = 'no change' THEN 1 ELSE 0 END) as count_no_change,
    -- Добавляем информацию о составе
    COUNT(DISTINCT drug_molecule_id) AS unique_drugs,
    COUNT(DISTINCT coformer_molecule_id) AS unique_coformers,
    COUNT(DISTINCT publication_id) AS unique_publications,
    MIN(year) AS first_year,
    MAX(year) AS last_year
FROM
    {{ ref('serving_all_data_co_crystals') }}
WHERE
    name_cocrystal IS NOT NULL
GROUP BY
    name_cocrystal
ORDER BY
    experiments_count DESC
LIMIT 10
