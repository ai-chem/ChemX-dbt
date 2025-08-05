
--____________1 get_rows_without_nulls_____________________
{% macro cur_get_rows_without_nulls(model_name) %}
{# Используем ref — dbt подставит правильную схему и имя объекта #}
{% set relation = ref(model_name) %}

WITH total_rows AS (
    SELECT COUNT(*) AS count_all
    FROM {{ relation }}
),
rows_with_nulls AS (
    SELECT COUNT(*) AS count_with_nulls
    FROM {{ relation }}
    WHERE
    {% for column in adapter.get_columns_in_relation(relation) %}
        {{ column.name }} IS NULL
        {% if not loop.last %} OR {% endif %}
    {% endfor %}
)

SELECT
    total_rows.count_all AS total_row_count,
    rows_with_nulls.count_with_nulls AS rows_with_nulls_count,
    (total_rows.count_all - rows_with_nulls.count_with_nulls) AS rows_without_nulls_count,
    ROUND(100.0 * rows_with_nulls.count_with_nulls / total_rows.count_all, 2) AS percentage_rows_with_nulls,
    ROUND(100.0 * (total_rows.count_all - rows_with_nulls.count_with_nulls) / total_rows.count_all, 2) AS percentage_rows_without_nulls
FROM
    total_rows, rows_with_nulls
{% endmacro %}

--____________2  get_first_n_columns_____________________

{% macro cur_get_first_n_columns(model_name, n=5) %}
{% set relation = ref(model_name) %}

SELECT
    {% for column in adapter.get_columns_in_relation(relation) %}
        {% if loop.index <= n %}
            {{ column.name }}{% if not loop.last and loop.index < n %},{% endif %}
        {% endif %}
    {% endfor %}
FROM {{ relation }}
{% endmacro %}

--____________3  cur_column_stats_____________________

{% macro cur_column_stats(model_name) %}
{% set relation = ref(model_name) %}

WITH base_counts AS (
    SELECT
        COUNT(*) AS total_rows
    FROM
        {{ relation }}
)

{% for column in adapter.get_columns_in_relation(relation) %}
SELECT
    '{{ column.name }}' AS column_name,
    '{{ column.data_type }}' AS data_type,
    base_counts.total_rows AS total_rows,
    SUM(CASE WHEN "{{ column.name }}" IS NULL THEN 1 ELSE 0 END) AS null_count,
    ROUND(100.0 * SUM(CASE WHEN "{{ column.name }}" IS NULL THEN 1 ELSE 0 END) / base_counts.total_rows, 2) AS null_percentage
FROM
    {{ relation }},
    base_counts
GROUP BY
    base_counts.total_rows

{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
ORDER BY
    null_percentage DESC,
    column_name
{% endmacro %}
--____________4  count_full_duplicates_____________________

{% macro cur_count_full_duplicates(model_name) %}
{% set relation = ref(model_name) %}

WITH all_rows AS (
    SELECT * FROM {{ relation }}
),

row_counts AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT MD5(CAST((
            {% for column in adapter.get_columns_in_relation(relation) %}
                COALESCE(CAST({{ column.name }} AS VARCHAR), 'NULL')
                {% if not loop.last %} || '|' || {% endif %}
            {% endfor %}
        ) AS VARCHAR))) AS distinct_rows
    FROM
        all_rows
)

SELECT
    '{{ model_name }}' AS table_name,
    total_rows,
    distinct_rows,
    (total_rows - distinct_rows) AS duplicate_rows,
    ROUND(100.0 * (total_rows - distinct_rows) / total_rows, 2) AS duplicate_percentage
FROM
    row_counts
{% endmacro %}