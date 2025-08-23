{{ config(
    materialized='table',
    schema='curated'
) }}

with dedup_complexes as (

    {{ deduplicate_model('uni_complexes') }}

)

select
    dedup_complexes.*,

    -- Перевод supplementary в boolean
    case
      when supplementary = 1 then true
      when supplementary = 0 then false
      else null
    end as supplementary_bool
from dedup_complexes
