{#
    Приводит значения столбца с названием наночастицы к единому (нормализованному) формату для дальнейшей идентификации и анализа.
    Используется для устранения разночтений, синонимов, опечаток и разных способов записи одного и того же вещества в разных источниках данных.
    Как работает:
    Приводит значение к нижнему регистру и убирает лишние пробелы.
    Сопоставляет значение с заранее заданным списком синонимов и сокращений для наиболее часто встречающихся наночастиц.
    Если значение найдено в списке возвращает стандартное обозначение (например, 'cobalt ferrite' → 'CoFe2O4').
    Если значение не найдено возвращает исходное значение без изменений.

    Почему так сделано:
    В разных статьях и датасетах одни и те же наночастицы могут называться по-разному ( 'Fe3O4', 'ferrous ferric oxide', 'iron oxide' и т.д.).
    Для работы важно привести все варианты к единому виду.
    Список синонимов составлен на основе анализа встречающихся вариантов в исходных данных и может дополняться по мере необходимости.

    Ограничения:
    Если в данных встречается новый вариант написания, которого нет в списке, он не будет нормализован.
    Для сложных случаев (например, составных или смешанных материалов) требуется ручное добавление новых правил.

    Аргументы:
    column: имя столбца с исходным названием наночастицы (строка)

    Пример использования:
      {{ normalize_nanoparticle('nanoparticle') }}

    Пример результата:
      'cobalt ferrite' → 'CoFe2O4'
      'Fe3O4' → 'Fe3O4'
      'unknown' → 'unknown'
#}


{% macro normalize_nanoparticle(column) %}
    case lower(trim({{ column }}))
        -- cobalt ferrite family (CFO, cobalt ferrite, etc.)
        when 'cfo' then 'CoFe2O4'
        when 'cobalt ferrite' then 'CoFe2O4'
        when 'cobalt oxide' then 'CoO'
        when 'cobaltum' then 'Co'
        when 'co' then 'Co'
        when 'co/Co-oxide' then 'Core/Shell Co-CoO'
        when 'co-oxide/co' then 'Core/Shell Co-CoO'
        when 'CoO/Co/polystyrene nanosphere' then 'Core/Shell Co-CoO-polystyrene'
        when 'cobalt irin oxide' then 'Mixed cobalt iron oxides'
        when 'cobalt irin oxide and bismuth ferrite' then 'Mixed cobalt-iron-bismuth oxides'

        -- iron oxide & ferrites
        when 'fe3o4' then 'Fe3O4'
        when 'ferrous ferric oxide' then 'Fe3O4'
        when 'iron oxide' then 'Fe2O3'
        when 'iron ferrite' then 'Fe2O3'
        when 'iron hematite' then 'Fe2O3'
        when 'ferrous ferric oxide' then 'Fe3O4'
        when 'hematite' then 'Fe2O3'
        when 'iron magnesium oxide' then 'FeMgO'
        when 'iron rhodium' then 'FeRh'
        when 'iron carbide' then 'Fe3C'

        -- bismuth family
        when 'bfo' then 'BiFeO3'
        when 'bismuth ferrite' then 'BiFeO3'
        when 'bismuth iron oxide' then 'BiFeO3'
        when 'bismuth complex oxide' then 'Bi-based complex oxide'

        -- lanthanum ferrites
        when 'lanthanum iron oxide' then 'LaFeO3'
        when 'lanthanum iron perovskite' then 'LaFeO3'
        when 'lanthanum iton oxide' then 'LaFeO3'

        -- nickname families
        when 'blfo' then 'BiLaFeO3'
        when 'lfo' then 'LaFeO3'
        when 'lsmo' then 'LaSrMnO3'
        when 'nf' then 'NiFe2O4'
        when 'fn' then 'FeNi'

        -- manganites / manganese oxide
        when 'manganese dioxide' then 'MnO2'
        when 'manganese ferrite' then 'MnFe2O4'
        when 'manganese oxide' then 'MnOx'
        when 'manganese zinc ferrite' then 'MnZnFe2O4'
        when 'mn-co-nio-based heterostructured nanocrystals' then 'MnCoNiOx (heterostructure)'

        -- nickel family
        when 'metallic ni and nickel monoxide' then 'Ni + NiO'
        when 'nickel monoxide' then 'NiO'
        when 'nickel oxide' then 'NiO'

        -- copper family
        when 'copper oxide' then 'CuO'
        when 'cr ion-doped ag' then 'Ag:Cr doped'

        -- zinc family
        when 'zinc oxide' then 'ZnO'

        -- misc
        when 'double perovskite' then 'Double perovskite'
        when 'fe-doped nio' then 'Fe:NiO'
        when 'ho-doped neodymium orthoferrite' then 'Ho:NdFeO3'
        when 'ti-doped maghemite' then 'Ti:γ-Fe2O3'
        when 'barium ferrite' then 'BaFe12O19'

        -- sample types or unclear
        when 'name' then 'unknown'
        when '' then 'unknown'
        else {{ column }}
    end
{% endmacro %}
