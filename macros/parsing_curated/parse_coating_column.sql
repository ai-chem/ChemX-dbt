{#
    Макрос: create_has_coating_column
    Создаёт булевый столбец has_coating на основе значения столбца покрытия.
    Работает с разными типами данных:
      - Если column_type = 'bigint', ожидает числовые значения (0 или 1).
      - В остальных случаях работает со строками (NULL, пустая строка, 'null' → 0; иначе 1).
    Используется для унификации признака покрытия наночастицы во всех моделях.
    Аргументы:
      - coating_column: имя столбца с покрытием
      - column_type: тип столбца ('bigint' или 'text', по умолчанию 'text')
#}

{% macro create_has_coating_column(coating_column, column_type='text') %}

{% if column_type == 'bigint' %}
CASE
    WHEN {{ coating_column }} = 0 THEN 0
    WHEN {{ coating_column }} = 1 THEN 1
    WHEN {{ coating_column }} IS NULL THEN 0
    ELSE 1
END AS has_coating
{% else %}
CASE
    WHEN {{ coating_column }} IS NULL THEN 0
    WHEN TRIM({{ coating_column }}) = '' THEN 0
    WHEN {{ coating_column }}::text = 'null' THEN 0
    ELSE 1
END AS has_coating
{% endif %}

{% endmacro %}
