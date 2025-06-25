{{ config(
    materialized='table',
    schema='curated'
) }}

with base as (

    {{ deduplicate_model('uni_co_crystals') }}

)

select
    base.*,
    {{ parse_ratio_component_1('ratio_cocrystal') }} as ratio_component_1,
    {{ parse_ratio_component_2('ratio_cocrystal') }} as ratio_component_2

from base