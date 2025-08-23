{{ config(
    materialized='table',
    schema='serving',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (experiment_id)"
) }}

SELECT
    -- Ключ из таблицы фактов
    fact.experiment_id,

    -- Все поля из справочника публикаций
    pub.publication_id,
    pub.pdf,
    pub.doi,
    pub.doi_sourse,
    pub.supplementary,
    pub.supplementary_bool,
    pub.title,
    pub.publisher,
    pub.year,
    pub.access,

    -- Все поля из справочника молекул
    mol.molecule_id,
    mol.smiles,
    mol.smiles_type,
    mol.compound_name,
    mol.page_smiles,
    mol.origin_smiles,

    -- Все поля из справочника металлов
    metal.metal_id,
    metal.metal,
    metal.page_metal,
    metal.origin_metal,

    -- Все поля из справочника источников
    src.source_id,
    src.source_table,
    src.dbt_loaded_at,
    src.dbt_curated_at,

    -- Все оставшиеся поля из таблицы фактов
    fact.compound_id,
    fact.target_numeric,
    fact.target_original,
    fact.page_target_value,
    fact.origin_target_value

FROM
    {{ ref('fact_complexes_experiments') }} AS fact

-- Присоединяем все справочники по их ID
LEFT JOIN {{ ref('dim_complexes_publications') }} AS pub
    ON fact.publication_id = pub.publication_id

LEFT JOIN {{ ref('dim_complexes_molecules') }} AS mol
    ON fact.molecule_id = mol.molecule_id

LEFT JOIN {{ ref('dim_complexes_metals') }} AS metal
    ON fact.metal_id = metal.metal_id

LEFT JOIN {{ ref('dim_complexes_source') }} AS src
    ON fact.source_id = src.source_id
