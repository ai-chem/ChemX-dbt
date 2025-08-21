-- dim_cocrystals_cocrystal_definitions.sql
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (cocrystal_definition_id)"
) }}

WITH unique_cocrystal_definitions AS (
    -- Находим все уникальные комбинации полей, описывающих сокристалл
    SELECT DISTINCT
        name_cocrystal,
        name_cocrystal_type_file,
        name_cocrystal_page,
        name_cocrystal_origin,
        ratio_cocrystal,
        ratio_cocrystal_page,
        "ratio_cocrystal_page.1",
        ratio_cocrystal_origin,
        ratio_component_1,
        ratio_component_2
    FROM
        {{ ref('final_cur_co_crystals') }}
)

-- Генерируем суррогатный ключ для каждой уникальной комбинации
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            name_cocrystal,
            ratio_cocrystal
    ) AS cocrystal_definition_id,

    name_cocrystal,
    name_cocrystal_type_file,
    name_cocrystal_page,
    name_cocrystal_origin,
    ratio_cocrystal,
    ratio_cocrystal_page,
    "ratio_cocrystal_page.1",
    ratio_cocrystal_origin,
    ratio_component_1,
    ratio_component_2
FROM
    unique_cocrystal_definitions
