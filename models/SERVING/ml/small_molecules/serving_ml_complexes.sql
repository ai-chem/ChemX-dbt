{{ config(
    materialized='table',
    schema='serving'
) }}

WITH joined_data AS (
    SELECT
        fact.experiment_id,
        fact.target_numeric,
        pub.supplementary,
        pub.supplementary_bool,
        pub.publisher,
        pub.year,
        pub.access,
        mol.smiles,
        mol.smiles_type,
        mol.compound_name,
        mol.page_smiles,
        mol.origin_smiles,
        metal.metal,
        metal.page_metal,
        metal.origin_metal,
        fact.compound_id,
        fact.page_target_value,
        fact.origin_target_value
    FROM {{ ref('fact_complexes_experiments') }} AS fact
    LEFT JOIN {{ ref('dim_complexes_publications') }} AS pub ON fact.publication_id = pub.publication_id
    LEFT JOIN {{ ref('dim_complexes_molecules') }} AS mol ON fact.molecule_id = mol.molecule_id
    LEFT JOIN {{ ref('dim_complexes_metals') }} AS metal ON fact.metal_id = metal.metal_id
)

SELECT
    experiment_id,
    supplementary,
    supplementary_bool,
    publisher,
    year,
    access,
    smiles,
    smiles_type,
    page_smiles,
    origin_smiles,
    metal,
    page_metal,
    origin_metal,

    -- Числовые с пропусками
    {{ impute_numeric('target_numeric') }},
    {{ impute_numeric('page_target_value') }},

    -- Категориальные с пропусками
    {{ impute_categorical('origin_target_value') }},
    {{ impute_categorical('compound_id') }},
    {{ impute_categorical('compound_name') }}
FROM
    joined_data
