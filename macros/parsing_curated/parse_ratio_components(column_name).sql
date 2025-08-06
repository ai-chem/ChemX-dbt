{% macro parse_ratio_component_1(column_name) %}
  CAST(
    NULLIF(
      regexp_replace(
        split_part(replace({{ column_name }}, ' ', ''), ':', 1),
        ',', '.', 'g'
      ),
      ''
    )
    AS DOUBLE PRECISION
  )
{% endmacro %}

{% macro parse_ratio_component_2(column_name) %}
  CAST(
    NULLIF(
      regexp_replace(
        split_part(replace({{ column_name }}, ' ', ''), ':', 2),
        ',', '.', 'g'
      ),
      ''
    )
    AS DOUBLE PRECISION
  )
{% endmacro %}
