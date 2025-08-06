{#
    Макрос: generate_canonical_name
    Формирует строковый ключ (canonical name) для наночастицы на основе состава, формы, покрытия и среднего размера.
    Используется для уникальной идентификации наночастиц и join'а с соответствующим справочником.
    Если какое-либо поле отсутствует (NULL), оно заменяется на строку 'NULL'.
#}

{% macro generate_canonical_name() %}
    concat(
        coalesce(nanoparticle, 'NULL'), '_',
        coalesce(normalized_shape, 'NULL'), '_',
        coalesce(cast(has_coating as text), 'NULL'), '_',
        coalesce(cast(np_size_avg_nm as text), 'NULL')
    )
{% endmacro %}
