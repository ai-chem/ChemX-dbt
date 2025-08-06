{% macro bool_from_text_equals(column_name, target_value) %}
  case
    when {{ column_name }} = '{{ target_value }}' then true
    when {{ column_name }} is null then null
    else false
  end
{% endmacro %}
