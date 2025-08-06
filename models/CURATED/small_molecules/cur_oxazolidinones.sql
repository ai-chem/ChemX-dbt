{{ config(
    materialized='table',
    schema='curated'
) }}

with dedup_oxazolidinones as (

    select *
    from {{ ref('uni_oxazolidinones') }}
    where
        target_value IS NULL
        OR target_value ~ '^\d+(,\d+)?$'  -- только числа с запятой
)

select
    dedup_oxazolidinones.*,

    {{ bool_from_int('access') }} as access_bool,
    {{ parse_decimal_comma_to_float('target_value') }} as target_value_parsed

from dedup_oxazolidinones
