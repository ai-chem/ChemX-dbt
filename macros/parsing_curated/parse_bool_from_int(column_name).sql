{% macro bool_from_int(column_name) %}
  case
    when {{ column_name }} = 1 then true
    when {{ column_name }} = 0 then false
    else null
  end
{% endmacro %}
