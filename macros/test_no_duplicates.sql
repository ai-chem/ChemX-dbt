-- macros/test_no_duplicates.sql
{% macro test_no_duplicates(model) %}
/**
 * Упрощенный тест для проверки отсутствия полных дубликатов строк в таблице.
 */

WITH all_rows AS (
    SELECT COUNT(*) AS total_rows FROM {{ model }}
),
distinct_rows AS (
    SELECT COUNT(*) AS distinct_count FROM (SELECT DISTINCT * FROM {{ model }}) AS t
)
SELECT
    'Найдены дубликаты' AS issue,
    (total_rows - distinct_count) AS duplicate_count
FROM
    all_rows, distinct_rows
WHERE
    (total_rows - distinct_count) > 0

{% endmacro %}
