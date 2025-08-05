-- Справочник валидации данных (dim_nanomag_validation).
-- Каждая строка — уникальная комбинация параметров валидации/проверки.
-- Используется для связи с факт-таблицей через validation_id.

{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (validation_id)"
) }}

with unique_validation as (
    select distinct
        verified_by,
        verification_date,
        has_mistake_in_matadata,
        verification_required,
        comment
    from {{ ref('final_cur_nanomag') }}
)

select
    row_number() over (
        order by
            verified_by,
            verification_date,
            has_mistake_in_matadata,
            verification_required,
            comment
    ) as validation_id,
    verified_by,
    verification_date,
    has_mistake_in_matadata,
    verification_required,
    comment
from unique_validation