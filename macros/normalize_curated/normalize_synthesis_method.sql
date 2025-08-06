{#
    Приводит описание метода синтеза наночастиц к одной из стандартных категорий:
    'Biosynthesis', 'Green Synthesis', 'Chemical Synthesis', 'Physical Methods', 'Commercial', 'Other'.

    Как работает:
    Ищет ключевые слова в строке с методом синтеза (без учёта регистра).
    Если найдено слово, относящееся к биосинтезу, возвращает 'Biosynthesis', и т.п.
    Если ни одно условие не выполнено, возвращает 'Other'.

    Пример:
      'green synthesis using plant extract' → 'Green Synthesis'

    Аргумент:
    column_name: имя столбца с исходным описанием метода синтеза
#}


{% macro standardize_synthesis_method(column_name) %}

CASE
    WHEN LOWER({{ column_name }}) LIKE '%biosynthesis%'
        OR LOWER({{ column_name }}) LIKE '%bacterial%'
        OR LOWER({{ column_name }}) LIKE '%fungi%'
        OR LOWER({{ column_name }}) LIKE '%actinobacteria%' THEN 'Biosynthesis'

    WHEN LOWER({{ column_name }}) LIKE '%green%'
        OR LOWER({{ column_name }}) LIKE '%eco%'
        OR LOWER({{ column_name }}) LIKE '%extract%' THEN 'Green Synthesis'

    WHEN LOWER({{ column_name }}) LIKE '%chemical%'
        OR LOWER({{ column_name }}) LIKE '%chem_%'
        OR LOWER({{ column_name }}) LIKE '%reduction%'
        OR LOWER({{ column_name }}) LIKE '%oxidation%'
        OR LOWER({{ column_name }}) LIKE '%precipitation%'
        OR LOWER({{ column_name }}) LIKE '%sol-gel%'
        OR LOWER({{ column_name }}) LIKE '%solvothermal%'
        OR LOWER({{ column_name }}) LIKE '%emulsion%'
        OR LOWER({{ column_name }}) LIKE '%microemulsion%' THEN 'Chemical Synthesis'

    WHEN LOWER({{ column_name }}) LIKE '%ball mill%'
        OR LOWER({{ column_name }}) LIKE '%vapor%'
        OR LOWER({{ column_name }}) LIKE '%thermal%'
        OR LOWER({{ column_name }}) LIKE '%pyrolysis%'
        OR LOWER({{ column_name }}) LIKE '%irradiation%'
        OR LOWER({{ column_name }}) LIKE '%electrochemical%'
        OR LOWER({{ column_name }}) LIKE '%ebpvd%'
        OR LOWER({{ column_name }}) LIKE '%explosion%'
        OR LOWER({{ column_name }}) LIKE '%sonomechanical%'
        OR LOWER({{ column_name }}) LIKE '%templating%'
        OR LOWER({{ column_name }}) LIKE '%seed-mediated%' THEN 'Physical Methods'

    WHEN LOWER({{ column_name }}) LIKE '%purchased%'
        OR LOWER({{ column_name }}) LIKE '%commercial%' THEN 'Commercial'

    ELSE 'Other'
END

{% endmacro %}
