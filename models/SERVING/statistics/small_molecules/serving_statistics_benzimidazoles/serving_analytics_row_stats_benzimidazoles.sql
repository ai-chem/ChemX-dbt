{{ config(
    materialized='table',
    schema='serving'
) }}

{% set columns = adapter.get_columns_in_relation(ref('serving_all_data_benzimidazoles')) %}
{% set total_columns = columns | length %}

WITH base AS (
    SELECT * FROM {{ ref('serving_all_data_benzimidazoles') }}
),

row_completeness AS (
    SELECT
        *,
        (
            {% for col in columns %}
                CASE WHEN "{{ col.name }}" IS NOT NULL THEN 1 ELSE 0 END
                {% if not loop.last %} + {% endif %}
            {% endfor %}
        ) AS filled_count
    FROM base
)

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT smiles) AS unique_molecules,
    COUNT(DISTINCT publication_id) AS unique_publications,
    COUNT(DISTINCT bacteria_id) AS unique_bacteria,
    COUNT(DISTINCT smiles || '_' || bacteria_unified) AS unique_molecule_bacteria_pairs,
    MIN(year) AS min_year,
    MAX(year) AS max_year,
    SUM(CASE WHEN filled_count >= {{ (total_columns * 0.8) | round(0, 'floor') }} THEN 1 ELSE 0 END) AS rows_with_80_percent_filled
FROM row_completeness
