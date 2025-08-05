-- macros/impute_helpers.sql

-- Макрос для обработки ЧИСЛОВЫХ колонок
{% macro impute_numeric(column_name, default_value=0) %}
    coalesce({{ column_name }}, {{ default_value }}) as {{ column_name }},
    case when {{ column_name }} is null then 1 else 0 end as is_{{ column_name }}_imputed
{% endmacro %}


-- Макрос для обработки ТЕКСТОВЫХ (категориальных) колонок
{% macro impute_categorical(column_name, default_value="'Unknown'") %}
    coalesce({{ column_name }}, {{ default_value }}) as {{ column_name }},
    case when {{ column_name }} is null then 1 else 0 end as is_{{ column_name }}_imputed
{% endmacro %}


-- Макрос для обработки БУЛЕВЫХ колонок
{% macro impute_boolean(column_name, default_value='false') %}
    coalesce({{ column_name }}, {{ default_value }}) as {{ column_name }},
    case when {{ column_name }} is null then 1 else 0 end as is_{{ column_name }}_imputed
{% endmacro %}