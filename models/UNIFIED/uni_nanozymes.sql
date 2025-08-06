{{ config(
    materialized='view',
    schema='unified',
    unique_key='id'
) }}

with base as (

    SELECT

        -- Переименование formula → nanoparticle
        formula AS nanoparticle,

        -- Остальные поля
        activity,
        syngony,
        length,
        width,
        depth,
        surface,
        km_value,
        km_unit,
        vmax_value,
        vmax_unit,
        target_source,
        reaction_type,
        c_min,
        c_max,
        c_const,
        c_const_unit,
        ccat_value,
        ccat_unit,
        ph,
        temperature,
        doi,
        pdf,
        access,
        title,
        journal,
        year,

        -- Метаданные DBT
        current_timestamp AS dbt_loaded_at,
        'nanozymes' AS source_table

    FROM {{ source('raw', 'nanozymes') }}
)

select
    *,
    row_number() over () as id
from base
