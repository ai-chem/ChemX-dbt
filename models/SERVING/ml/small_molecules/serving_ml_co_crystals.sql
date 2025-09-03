{{ config(
    materialized='table',
    schema='serving'
) }}

WITH joined_data AS (
    SELECT
        fact.experiment_id,
        fact.photostability_change,
        drug.smiles AS smiles_drug,
        drug.molecule_name AS name_drug,
        coformer.smiles AS smiles_coformer,
        coformer.molecule_name AS name_coformer,
        def.name_cocrystal,
        def.name_cocrystal_type_file,
        def.name_cocrystal_page,
        def.name_cocrystal_origin,
        def.ratio_component_1,
        def.ratio_component_2,
        def.ratio_cocrystal_page,
        def.ratio_cocrystal_page_1,
        def.ratio_cocrystal_origin,
        pub.year,
        pub.access,
        fact.photostability_change_type_file,
        fact.photostability_change_origin,
        fact.photostability_change_page,
        fact.name_drug_type_file,
        fact.name_drug_origin,
        fact.name_drug_page,
        fact.smiles_drug_type_file,
        fact.smiles_drug_origin,
        fact.smiles_drug_page,
        fact.name_coformer_type_file,
        fact.name_coformer_origin,
        fact.name_coformer_page,
        fact.smiles_coformer_type_file,
        fact.smiles_coformer_origin,
        fact.smiles_coformer_page
    FROM {{ ref('fact_cocrystals_experiments') }} AS fact
    LEFT JOIN {{ ref('dim_cocrystals_cocrystal_definitions') }} AS def ON fact.cocrystal_definition_id = def.cocrystal_definition_id
    LEFT JOIN {{ ref('dim_cocrystals_molecules') }} AS drug ON fact.drug_molecule_id = drug.molecule_id
    LEFT JOIN {{ ref('dim_cocrystals_molecules') }} AS coformer ON fact.coformer_molecule_id = coformer.molecule_id
    LEFT JOIN {{ ref('dim_cocrystals_publications') }} AS pub ON fact.publication_id = pub.publication_id
)

SELECT
    experiment_id,
    smiles_drug,
    name_drug,
    name_cocrystal,
    name_cocrystal_type_file,
    name_cocrystal_page,
    name_cocrystal_origin,
    year,
    access,
    name_drug_type_file,
    name_drug_origin,
    name_drug_page,

    -- Числовые с пропусками
    {{ impute_numeric('ratio_component_1') }},
    {{ impute_numeric('ratio_component_2') }},
    {{ impute_numeric('ratio_cocrystal_origin') }},
    {{ impute_numeric('smiles_coformer_page') }},
    {{ impute_numeric('smiles_drug_page') }},
    {{ impute_numeric('name_coformer_page') }},
    {{ impute_numeric('photostability_change_page') }},

    -- Категориальные с пропусками
    {{ impute_categorical('photostability_change') }},
    {{ impute_categorical('smiles_coformer') }},
    {{ impute_categorical('name_coformer') }},
    {{ impute_categorical('ratio_cocrystal_page') }},
    {{ impute_categorical('ratio_cocrystal_page_1') }},
    {{ impute_categorical('smiles_coformer_origin') }},
    {{ impute_categorical('smiles_coformer_type_file') }},
    {{ impute_categorical('smiles_drug_origin') }},
    {{ impute_categorical('smiles_drug_type_file') }},
    {{ impute_categorical('name_coformer_origin') }},
    {{ impute_categorical('name_coformer_type_file') }},
    {{ impute_categorical('photostability_change_type_file') }},
    {{ impute_categorical('photostability_change_origin') }}
FROM
    joined_data
