-- Справочник наночастиц.
-- Каждая строка — уникальная наночастица по совокупности морфологических и кристаллографических характеристик.
-- Используется для связи с факт-таблицей через nanoparticle_id.

{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (nanoparticle_id)"
) }}

with unique_nanoparticles as (
    select distinct
        nanoparticle,
        syngony,
        surface,
        length_lower,
        length_upper,
        length_mean,
        width_lower,
        width_upper,
        width_mean,
        depth_lower,
        depth_upper,
        depth_mean
    from {{ ref('final_cur_nanozymes') }}
)

, first_row as (
    select
        *,
        row_number() over (
            partition by
                nanoparticle,
                syngony,
                surface,
                length_lower,
                length_upper,
                length_mean,
                width_lower,
                width_upper,
                width_mean,
                depth_lower,
                depth_upper,
                depth_mean
            order by id
        ) as row_num_in_group
    from {{ ref('final_cur_nanozymes') }}
)

select
    -- surrogate key
    row_number() over (
        order by
            unique_nanoparticles.nanoparticle,
            unique_nanoparticles.syngony,
            unique_nanoparticles.surface,
            unique_nanoparticles.length_lower,
            unique_nanoparticles.length_upper,
            unique_nanoparticles.length_mean,
            unique_nanoparticles.width_lower,
            unique_nanoparticles.width_upper,
            unique_nanoparticles.width_mean,
            unique_nanoparticles.depth_lower,
            unique_nanoparticles.depth_upper,
            unique_nanoparticles.depth_mean
    ) as nanoparticle_id,

    -- ключевые поля
    unique_nanoparticles.nanoparticle,
    unique_nanoparticles.syngony,
    unique_nanoparticles.surface,
    unique_nanoparticles.length_lower,
    unique_nanoparticles.length_upper,
    unique_nanoparticles.length_mean,
    unique_nanoparticles.width_lower,
    unique_nanoparticles.width_upper,
    unique_nanoparticles.width_mean,
    unique_nanoparticles.depth_lower,
    unique_nanoparticles.depth_upper,
    unique_nanoparticles.depth_mean,

    -- дополнительные характеристики (если нужны, например, средние значения)
    first_row.length,
    first_row.width,
    first_row.depth

from unique_nanoparticles
left join first_row
    on unique_nanoparticles.nanoparticle = first_row.nanoparticle
    and unique_nanoparticles.syngony = first_row.syngony
    and unique_nanoparticles.surface = first_row.surface
    and unique_nanoparticles.length_lower = first_row.length_lower
    and unique_nanoparticles.length_upper = first_row.length_upper
    and unique_nanoparticles.length_mean = first_row.length_mean
    and unique_nanoparticles.width_lower = first_row.width_lower
    and unique_nanoparticles.width_upper = first_row.width_upper
    and unique_nanoparticles.width_mean = first_row.width_mean
    and unique_nanoparticles.depth_lower = first_row.depth_lower
    and unique_nanoparticles.depth_upper = first_row.depth_upper
    and unique_nanoparticles.depth_mean = first_row.depth_mean
    and first_row.row_num_in_group = 1