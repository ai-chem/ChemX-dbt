-- Справочник клеточных линий (dim_cell_line).
-- Каждая строка — уникальная клеточная линия, определяемая по типу, источнику, ткани, морфологии, возрасту и происхождению (человек/животное).
-- Используется для связи с факт-таблицей через cell_line_id.

{{ config(
    materialized='table',
    schema='star_schema',
     post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (cell_line_id)"
) }}

with unique_cell_lines as (
    select distinct
        cell_type,
        human_animal,
        cell_source,
        cell_tissue,
        cell_morphology,
        cell_age,
        is_human
    from {{ ref('final_cur_cytotox') }}
)

select
    row_number() over (
        order by {{ generate_cell_line_name() }}
    ) as cell_line_id,
    {{ generate_cell_line_name() }} as canonical_cell_line_name,
    cell_type,
    human_animal,
    cell_source,
    cell_tissue,
    cell_morphology,
    cell_age,
    is_human
from unique_cell_lines