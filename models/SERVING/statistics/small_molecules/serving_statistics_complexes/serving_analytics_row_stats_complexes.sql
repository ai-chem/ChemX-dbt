
{{ config(
    materialized='table',
    schema='serving'
) }}

{% set columns = adapter.get_columns_in_relation(ref('serving_all_data_complexes')) %}
{% set total_columns = columns | length %}

WITH base AS (
    SELECT * FROM {{ ref('serving_all_data_complexes') }}
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
    COUNT(DISTINCT molecule_id) AS unique_molecules,
    COUNT(DISTINCT metal_id) AS unique_metals,
    COUNT(DISTINCT publication_id) AS unique_publications,
    COUNT(DISTINCT molecule_id || '_' || metal_id) AS unique_molecule_metal_pairs,
    MIN(year) AS min_year,
    MAX(year) AS max_year,
    SUM(CASE WHEN filled_count >= {{ (total_columns * 0.8) | round(0, 'floor') }} THEN 1 ELSE 0 END) AS rows_with_80_percent_filled
FROM row_completeness
