{#
    Макрос: generate_cell_line_name
    Формирует строковый ключ (canonical name) для клеточной линии на основе всех её описательных полей.
    Используется для уникальной идентификации клеточных линий и join'а с соответствующим справочником.
    Если какое-либо поле отсутствует (NULL), оно заменяется на строку 'NULL'.
    Аргумент table_alias позволяет явно указать алиас таблицы для использования макроса в join'ах.
#}

{% macro generate_cell_line_name(table_alias='') %}
    {% set prefix = table_alias ~ '.' if table_alias else '' %}
    concat(
        coalesce({{ prefix }}cell_type, 'NULL'), '_',
        coalesce({{ prefix }}human_animal, 'NULL'), '_',
        coalesce({{ prefix }}cell_source, 'NULL'), '_',
        coalesce({{ prefix }}cell_tissue, 'NULL'), '_',
        coalesce({{ prefix }}cell_morphology, 'NULL'), '_',
        coalesce(cast({{ prefix }}cell_age as text), 'NULL'), '_',
        coalesce(cast({{ prefix }}is_human as text), 'NULL')
    )
{% endmacro %}
