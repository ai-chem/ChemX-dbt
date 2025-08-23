{{ config(
    materialized='table',
    schema='serving'
) }}

WITH joined_data AS (
    SELECT
        fact.measurement_id,
        fact.perm_numeric,
        fact.logp_numeric,
        mol.smiles,
        mol.name,
        pub.publisher,
        pub.year,
        pub.access,
        fact.page,
        fact.origin
    FROM {{ ref('fact_eyedrops_measurements') }} AS fact
    LEFT JOIN {{ ref('dim_eyedrops_molecules') }} AS mol ON fact.molecule_id = mol.molecule_id
    LEFT JOIN {{ ref('dim_eyedrops_publications') }} AS pub ON fact.publication_id = pub.publication_id
)

SELECT
    measurement_id,
    smiles,
    name,
    publisher,
    year,
    access,
    page,
    origin,

    -- Числовые с пропусками
    {{ impute_numeric('perm_numeric') }},
    {{ impute_numeric('logp_numeric') }}

FROM
    joined_data
