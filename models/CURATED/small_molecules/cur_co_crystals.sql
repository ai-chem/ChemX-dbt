{{ config(
    materialized='table',
    schema='curated'
) }}

with dedup_co_crystals as (

    {{ deduplicate_model('uni_co_crystals') }}

)

select
    dedup_co_crystals.*,
    {{ parse_ratio_component_1('ratio_cocrystal') }} as ratio_component_1,
    {{ parse_ratio_component_2('ratio_cocrystal') }} as ratio_component_2

from dedup_co_crystals
