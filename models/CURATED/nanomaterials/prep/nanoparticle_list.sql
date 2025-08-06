-- Справочник уникальных наночастиц (nanoparticle_list).
-- Каждая строка — уникальная комбинация состава, формы, покрытия и среднего размера наночастицы,
-- встречающаяся хотя бы в одном из обработанных датасетов (cytotox, seltox, synergy).
-- Для каждой комбинации формируется уникальный идентификатор nanoparticle_id и строковый ключ canonical_name.
-- Используется для связи с факт-таблицами и другими справочниками.

with unique_nanoparticles as (
    -- Собираем все уникальные комбинации ключевых полей из всех обработанных датасетов
    select distinct
        nanoparticle,
        normalized_shape,
        has_coating,
        np_size_avg_nm
    from {{ ref('cur_cytotox') }}
    union
    select distinct
        nanoparticle,
        normalized_shape,
        has_coating,
        np_size_avg_nm
    from {{ ref('cur_seltox') }}
    union
    select distinct
        nanoparticle,
        normalized_shape,
        has_coating,
        np_size_avg_nm
    from {{ ref('cur_synergy') }}
)

select
    *,
    -- Строковый ключ для join и диагностики (NULL заменяются на строку 'NULL')
    {{ generate_canonical_name() }} as canonical_name,
    -- Уникальный surrogate key для каждой комбинации
    row_number() over (
        order by nanoparticle, normalized_shape, has_coating, np_size_avg_nm
    ) as nanoparticle_id
from unique_nanoparticles
