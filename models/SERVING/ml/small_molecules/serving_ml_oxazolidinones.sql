{{ config(
    materialized='table',
    schema='serving'
) }}

WITH joined_data AS (
    SELECT
        fact.experiment_id,
        fact.target_value_parsed,
        mol.smiles,
        mol.page_scaffold,
        mol.origin_scaffold,
        mol.page_residue,
        mol.origin_residue,
        bact.bacteria_name_unified,
        bact.bacteria_info,
        bact.page_bacteria,
        bact.origin_bacteria,
        bact.section_bacteria,
        pub.publisher,
        pub.year,
        pub.access,
        pub.access_bool,
        fact.compound_id,
        fact.target_type,
        fact.target_relation,
        fact.target_units,
        fact.page_target,
        fact.origin_target
    FROM {{ ref('fact_oxazolidinones_experiments') }} AS fact
    LEFT JOIN {{ ref('dim_oxazolidinones_molecules') }} AS mol ON fact.molecule_id = mol.molecule_id
    LEFT JOIN {{ ref('dim_oxazolidinones_bacteria') }} AS bact ON fact.bacteria_id = bact.bacteria_id
    LEFT JOIN {{ ref('dim_oxazolidinones_publications') }} AS pub ON fact.publication_id = pub.publication_id
)

SELECT
    experiment_id,
    target_value_parsed,
    smiles,
    page_scaffold,
    origin_scaffold,
    bacteria_name_unified,
    page_bacteria,
    origin_bacteria,
    publisher,
    year,
    access,
    access_bool,
    compound_id,
    target_type,
    target_relation,
    target_units,
    page_target,
    origin_target,

    -- Числовые с пропусками
    {{ impute_numeric('page_residue') }},

    -- Категориальные с пропусками
    {{ impute_categorical('section_bacteria') }},
    {{ impute_categorical('origin_residue') }},
    {{ impute_categorical('bacteria_info') }}
FROM
    joined_data
