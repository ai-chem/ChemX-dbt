{% macro parse_numeric_with_error(column_name) %}
    CAST(
        regexp_replace(
            regexp_substr({{ column_name }}, '[0-9]+,[0-9]+|[0-9]+'),
            ',', '.', 'g'
        ) AS DOUBLE PRECISION
    )
{% endmacro %}
