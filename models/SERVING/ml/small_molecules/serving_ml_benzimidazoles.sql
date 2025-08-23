{{ config(
    materialized='table',
    schema='serving'
) }}

WITH joined_data AS (
    SELECT
        fact.experiment_id,
        fact.target_value_parsed,

        mol.smiles,
        mol.compound_id,
        mol.origin_scaffold,
        mol.origin_residue,
        mol.page_scaffold,
        mol.page_residue,
        bact.bacteria_unified,
        bact.page_bacteria,
        bact.origin_bacteria,
        bact.section_bacteria,
        bact.subsection_bacteria,
        pub.publisher,
        pub.year,
        pub.access,
        fact.target_value_error,
        fact.target_type,
        fact.target_relation,
        fact.target_units,
        fact.page_target,
        fact.origin_target,
        fact.section_target,
        fact.subsection_target
    FROM
        {{ ref('fact_benzimidazoles_experiments') }} AS fact
    LEFT JOIN {{ ref('dim_benzimidazoles_molecules') }} AS mol
        ON fact.molecule_id = mol.molecule_id
    LEFT JOIN {{ ref('dim_benzimidazoles_bacteria') }} AS bact
        ON fact.bacteria_id = bact.bacteria_id
    LEFT JOIN {{ ref('dim_benzimidazoles_publications') }} AS pub
        ON fact.publication_id = pub.publication_id
)

SELECT
    experiment_id,
    target_value_parsed,
    -- Признаки без пропусков
    smiles,
    compound_id,
    origin_scaffold,
    page_scaffold,
    page_bacteria,
    origin_bacteria,
    publisher,
    year,
    access,
    target_type,
    target_relation,
    target_units,
    page_target,
    origin_target,

    -- Обработка числовых колонок с пропусками
    {{ impute_numeric('target_value_error') }},
    {{ impute_numeric('page_residue') }},

    -- Обработка текстовых колонок с пропусками
    {{ impute_categorical('subsection_target') }},
    {{ impute_categorical('subsection_bacteria') }},
    {{ impute_categorical('section_target') }},
    {{ impute_categorical('section_bacteria') }},
    {{ impute_categorical('bacteria_unified') }},
    {{ impute_categorical('origin_residue') }}

FROM
    joined_data
