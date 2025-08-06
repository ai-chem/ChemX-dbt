--1 get_rows_without_nulls_________
{% macro raw_get_rows_without_nulls(source_name, table_name) %}

WITH total_rows AS (
    SELECT COUNT(*) AS count_all
    FROM {{ source(source_name, table_name) }}
),

rows_with_nulls AS (
    SELECT COUNT(*) AS count_with_nulls
    FROM {{ source(source_name, table_name) }}
    WHERE
        {% for column in adapter.get_columns_in_relation(source(source_name, table_name)) %}
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

--2 get_first_n_columns_________

{% macro raw_get_first_n_columns(source_name, table_name, n=5) %}

SELECT
    {% for column in adapter.get_columns_in_relation(source(source_name, table_name)) %}
        {% if loop.index <= n %}
            {{ column.name }}{% if not loop.last and loop.index < n %},{% endif %}
        {% endif %}
    {% endfor %}
FROM {{ source(source_name, table_name) }}

{% endmacro %}

--3 column_stats_________

{% macro raw_column_stats(source_name, table_name) %}

WITH base_counts AS (
    SELECT
        COUNT(*) AS total_rows
    FROM
        {{ source(source_name, table_name) }}
)

{% for column in adapter.get_columns_in_relation(source(source_name, table_name)) %}
SELECT
    '{{ column.name }}' AS column_name,
    '{{ column.data_type }}' AS data_type,
    base_counts.total_rows AS total_rows,
    SUM(CASE WHEN {{ column.name }} IS NULL THEN 1 ELSE 0 END) AS null_count,
    ROUND(100.0 * SUM(CASE WHEN {{ column.name }} IS NULL THEN 1 ELSE 0 END) / base_counts.total_rows, 2) AS null_percentage
FROM
    {{ source(source_name, table_name) }},
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

--4 count_full_duplicates_________

{% macro raw_count_full_duplicates(source_name, table_name) %}

/**
 * Макрос для подсчета полных дубликатов строк в таблице.
 *
 * Параметры:
 *   source_name (string): Имя источника
 *   table_name (string): Имя таблицы
 *
 * Возвращает:
 *   SQL-запрос, который выводит количество дубликатов
 */

WITH all_rows AS (
    SELECT * FROM {{ source(source_name, table_name) }}
),

row_counts AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT MD5(CAST((
            {% for column in adapter.get_columns_in_relation(source(source_name, table_name)) %}
                COALESCE(CAST({{ column.name }} AS VARCHAR), 'NULL')
                {% if not loop.last %} || '|' || {% endif %}
            {% endfor %}
        ) AS VARCHAR))) AS distinct_rows
    FROM
        all_rows
)

SELECT
    '{{ source_name }}.{{ table_name }}' AS table_name,
    total_rows,
    distinct_rows,
    (total_rows - distinct_rows) AS duplicate_rows,
    ROUND(100.0 * (total_rows - distinct_rows) / total_rows, 2) AS duplicate_percentage
FROM
    row_counts

{% endmacro %}
