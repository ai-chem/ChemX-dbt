{% macro parse_range_component(column_name, part) %}
    -- part: 'lower', 'upper', 'mean'
    {% set col_cleaned = "regexp_replace(" ~ column_name ~ ", '[–—−]', '-', 'g')" %}
    {% if part == 'lower' %}
        CAST(split_part({{ col_cleaned }}, '-', 1) AS DOUBLE PRECISION)
    {% elif part == 'upper' %}
        CASE
            WHEN {{ col_cleaned }} LIKE '%-%'
            THEN CAST(split_part({{ col_cleaned }}, '-', 2) AS DOUBLE PRECISION)
            ELSE CAST(split_part({{ col_cleaned }}, '-', 1) AS DOUBLE PRECISION)
        END
    {% elif part == 'mean' %}
        CASE
            WHEN {{ col_cleaned }} LIKE '%-%'
            THEN (
                CAST(split_part({{ col_cleaned }}, '-', 1) AS DOUBLE PRECISION) +
                CAST(split_part({{ col_cleaned }}, '-', 2) AS DOUBLE PRECISION)
            ) / 2.0
            ELSE CAST(split_part({{ col_cleaned }}, '-', 1) AS DOUBLE PRECISION)
        END
    {% endif %}
{% endmacro %}
