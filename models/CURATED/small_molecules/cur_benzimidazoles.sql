{{ config(
    materialized='table',
    schema='curated'
) }}

with dedup_benzimidazoles as (

    {{ deduplicate_model('uni_benzimidazoles') }}

)

select
    dedup_benzimidazoles.*,
    {{ parse_numeric_with_error('target_value') }} AS target_value_parsed,
    {{ parse_error_component('target_value') }} AS target_value_error
from dedup_benzimidazoles
