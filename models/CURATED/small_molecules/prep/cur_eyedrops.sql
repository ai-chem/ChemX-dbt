{{ config(
    materialized='table',
    schema='curated'
) }}

with dedup_eyedrops as (

    {{ deduplicate_model('uni_eyedrops') }}

)

select
    dedup_eyedrops.*,

    -- Обработка logp_original: приведение значения "-5,54" к числу -5.54
    {{ parse_decimal_comma_to_float('logp_original') }} as logp_cleaned

from dedup_eyedrops
