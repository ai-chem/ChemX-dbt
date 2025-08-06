{% macro parse_decimal_comma_to_float(column_name) %}
  CAST(
    NULLIF(
      regexp_replace({{ column_name }}, ',', '.', 'g'),
      ''
    ) AS DOUBLE PRECISION
  )
{% endmacro %}
