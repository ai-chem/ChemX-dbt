{{ config(
    materialized='table',
    schema='curated'
) }}

with dedup_eyedrops as (

    {{ deduplicate_model('uni_eyedrops') }}

)

select
    dedup_eyedrops.*

from dedup_eyedrops
