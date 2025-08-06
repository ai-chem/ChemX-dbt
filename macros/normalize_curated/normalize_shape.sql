{#
    Приводит значения формы наночастицы к стандартным категориям для аналитики.
    Использует список часто встречающихся синонимов и группирует их в основные типы
    (spherical, rod, cube, hexagon и др.).
    Если форма не распознана или не указана явно, возвращает 'other' или 'unclear'.

    Аргумент:
     column: имя столбца с исходным значением формы

    Пример:
      'sphere', 'spheroidal', 'round' → 'spherical'
#}


{% macro normalize_shape(column) %}
    case lower(trim({{ column }}))
        -- spherical family
        when 'spherical' then 'spherical'
        when 'sphere' then 'spherical'
        when 'round' then 'spherical'
        when 'spheroidal' then 'spherical'
        when 'pseudo-spherical' then 'spherical'
        when 'quasi-spherical' then 'spherical'
        when 'semi-spherical' then 'spherical'
        when 'spherical cluster' then 'spherical'
        when 'spherical nano clusters' then 'spherical'
        when 'globular' then 'spherical'
        when 'sph/hex/ova' then 'spherical'

        -- rod family
        when 'rod' then 'rod'
        when 'rod-shaped' then 'rod'
        when 'nanorods' then 'rod'
        when 'cylindrical' then 'rod'
        when 'rho/sph' then 'rod'
        when 'sph/rho/rod' then 'rod'

        -- cube family
        when 'cubic' then 'cube'
        when 'cube' then 'cube'
        when 'cubic crystalline' then 'cube'

        -- hexagon family
        when 'hexagon' then 'hexagon'
        when 'hexagonal' then 'hexagon'
        when 'quasi-hexagonal' then 'hexagon'
        when 'sph/tri/hex' then 'hexagon'

        -- triangle family
        when 'triangular' then 'triangle'
        when 'sph/tri' then 'triangle'

        -- tube family
        when 'nanotube' then 'tube'
        when 'tubular' then 'tube'

        -- wire family
        when 'nanowire' then 'wire'
        when 'wire' then 'wire'

        -- oval / ellipsoid
        when 'oval' then 'oval'
        when 'ellipsoidal' then 'oval'
        when 'lentil' then 'oval'

        -- flat / plate family
        when 'flat' then 'plate'
        when 'nanoplates' then 'plate'

        -- rectangle / square
        when 'rectangular' then 'rectangle'
        when 'square' then 'rectangle'

        -- special / rare shapes
        when 'snowflake' then 'snowflake'
        when 'spindle' then 'spindle'
        when 'star' then 'star'
        when 'circular' then 'circular'
        when 'irregular' then 'irregular'
        when 'polygonal' then 'other' -- uncommon, not enough data
        when 'polycrystalline' then 'other' -- not a shape, rather a material property

        -- unclear / mixed
        when 'spherical and triangular' then 'unclear'
        when 'spherical, triangular and hexagonal' then 'unclear'
        when 'trianglular, hexahedral' then 'unclear'  -- likely typo
        when 'nanorods and triangles' then 'unclear'
        when 'rho/qua' then 'unclear'

        -- fallback
        else 'other'
    end
{% endmacro %}
