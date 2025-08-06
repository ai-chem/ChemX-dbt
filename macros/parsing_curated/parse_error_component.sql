{% macro parse_error_component(column_name) %}
    CAST(
        regexp_replace(
            regexp_replace(
                regexp_substr({{ column_name }}, '±\s*[0-9]+,[0-9]+'),
                '[^\d,]', '', 'g'
            ),
            ',', '.', 'g'
        ) AS DOUBLE PRECISION
    )
{% endmacro %}
