
{{ config(
    materialized='table',
    schema='serving',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (experiment_id)"
) }}

SELECT
    -- Ключ из таблицы фактов
    fact.experiment_id,
    -- Все поля из справочника определений сокристаллов
    def.cocrystal_definition_id,
    def.name_cocrystal,
    def.name_cocrystal_type_file,
    def.name_cocrystal_page,
    def.name_cocrystal_origin,
    def.ratio_cocrystal,
    def.ratio_cocrystal_page,
    def."ratio_cocrystal_page.1",
    def.ratio_cocrystal_origin,
    def.ratio_component_1,
    def.ratio_component_2,

    -- Все поля из справочника молекул для драгов
    drug.molecule_id AS drug_molecule_id,
    drug.smiles AS smiles_drug,
    drug.molecule_name AS name_drug,

    -- Все поля из справочника молекул для коформеров
    coformer.molecule_id AS coformer_molecule_id,
    coformer.smiles AS smiles_coformer,
    coformer.molecule_name AS name_coformer,

    -- Все поля из справочника публикаций
    pub.publication_id,
    pub.pdf,
    pub.doi,
    pub.supplementary,
    pub.authors,
    pub.title,
    pub.journal,
    pub.year,
    pub.page,
    pub.access,

    -- Все поля из справочника источников
    src.source_id,
    src.source_table,
    src.dbt_loaded_at,
    src.dbt_curated_at,

    -- Все оставшиеся поля из таблицы фактов
    fact.photostability_change,
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

FROM
    {{ ref('fact_cocrystals_experiments') }} AS fact

LEFT JOIN {{ ref('dim_cocrystals_cocrystal_definitions') }} AS def
    ON fact.cocrystal_definition_id = def.cocrystal_definition_id

LEFT JOIN {{ ref('dim_cocrystals_molecules') }} AS drug
    ON fact.drug_molecule_id = drug.molecule_id

LEFT JOIN {{ ref('dim_cocrystals_molecules') }} AS coformer
    ON fact.coformer_molecule_id = coformer.molecule_id

LEFT JOIN {{ ref('dim_cocrystals_publications') }} AS pub
    ON fact.publication_id = pub.publication_id

LEFT JOIN {{ ref('dim_cocrystals_source') }} AS src
    ON fact.source_id = src.source_id
