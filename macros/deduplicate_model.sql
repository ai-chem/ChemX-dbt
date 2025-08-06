-- macros/deduplicate_model.sql
{% macro deduplicate_model(model_name, exclude_columns=['dbt_loaded_at', 'source_table']) %}
/**
 * Макрос для удаления дубликатов из модели с использованием DISTINCT ON.
 *
 * Параметры:
 *   model_name (string): Имя модели для дедупликации
 *   exclude_columns (list, optional): Список колонок, которые нужно исключить из сравнения
 *
 * Возвращает:
 *   SQL-запрос, который выбирает только уникальные записи
 */

{% set relation = ref(model_name) %}
{% set columns = adapter.get_columns_in_relation(relation) %}

-- Создаем список колонок для DISTINCT ON и ORDER BY, исключая указанные
{% set partition_columns = [] %}
{% for column in columns %}
    {% if column.name not in exclude_columns %}
        {% do partition_columns.append(column.name) %}
    {% endif %}
{% endfor %}

SELECT DISTINCT ON (
    {% for column in partition_columns %}
        {% if '%' in column or '.' in column or '(' in column or ')' in column %}
            "{{ column }}"{% if not loop.last %},{% endif %}
        {% else %}
            {{ column }}{% if not loop.last %},{% endif %}
        {% endif %}
    {% endfor %}
)
    {% for column in columns %}
        {% if '%' in column.name or '.' in column.name or '(' in column.name or ')' in column.name %}
            "{{ column.name }}" AS "{{ column.name }}",
        {% else %}
            {{ column.name }},
        {% endif %}
    {% endfor %}
    current_timestamp AS dbt_curated_at
FROM
    {{ relation }}
ORDER BY
    {% for column in partition_columns %}
        {% if '%' in column or '.' in column or '(' in column or ')' in column %}
            "{{ column }}"{% if not loop.last %},{% endif %}
        {% else %}
            {{ column }}{% if not loop.last %},{% endif %}
        {% endif %}
    {% endfor %}
    {% if 'dbt_loaded_at' in columns|map(attribute='name') %}
        , dbt_loaded_at DESC
    {% endif %}

{% endmacro %}
