-- Присоединение идентификатора наночастицы (nanoparticle_id) к данным по селективной токсичности (seltox).
-- Для каждой строки из cur_seltox определяется уникальный идентификатор наночастицы на основе справочника nanoparticle_list.
-- Используется оператор IS NOT DISTINCT FROM для корректного сопоставления строк даже при наличии NULL в ключевых полях.

{{ config(
    materialized='table',
    schema='curated',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (serial_number)"
) }}

with cur_seltox as (
    select * from {{ ref('cur_seltox') }}
),

np_dim as (
    select * from {{ ref('nanoparticle_list') }}
)

select
    cur_seltox.*,
    np_dim.nanoparticle_id
from cur_seltox
left join np_dim
    -- Сопоставляем по всем ключевым полям, учитывая возможные NULL
    on cur_seltox.nanoparticle IS NOT DISTINCT FROM np_dim.nanoparticle
    and cur_seltox.normalized_shape IS NOT DISTINCT FROM np_dim.normalized_shape
    and cur_seltox.has_coating IS NOT DISTINCT FROM np_dim.has_coating
    and cur_seltox.np_size_avg_nm IS NOT DISTINCT FROM np_dim.np_size_avg_nm
