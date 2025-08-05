-- Справочник лекарств/антибиотиков (dim_synergy_drug).
-- Каждая строка — уникальное лекарство, встречающееся в экспериментах.

{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (drug_id)"
) }}

with unique_drugs as (
    select distinct
        drug
    from {{ ref('final_cur_synergy') }}
    where drug is not null and drug <> ''
)

select
    row_number() over (order by drug) as drug_id,
    drug
from unique_drugs