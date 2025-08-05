{{ config(materialized='table', schema='serving') }}

{% set columns = adapter.get_columns_in_relation(ref('serving_all_data_nanozymes')) %}
{% set total_columns = columns | length %}

with base as (
    select * from {{ ref('serving_all_data_nanozymes') }}
),
row_completeness as (
    select
        *,
        (
            {% for col in columns %}
                case when "{{ col.name }}" is not null then 1 else 0 end
                {% if not loop.last %} + {% endif %}
            {% endfor %}
        ) as filled_count
    from base
)
select
    count(*) as total_rows,
    count(distinct nanoparticle) as unique_nanoparticles,
    count(distinct publication_id) as unique_publications,
    count(distinct source_id) as unique_sources,
    min(year) as min_year,
    max(year) as max_year,
    sum(case when filled_count >= {{ (total_columns * 0.8) | round(0, 'floor') }} then 1 else 0 end) as rows_with_80_percent_filled
from row_completeness