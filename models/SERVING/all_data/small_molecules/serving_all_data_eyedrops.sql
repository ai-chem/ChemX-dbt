{{ config(
    materialized='table',
    schema='serving',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (measurement_id)"
) }}

SELECT
    -- Ключ из таблицы фактов
    fact.measurement_id,
    -- Все поля из справочника молекул
    mol.molecule_id,
    mol.smiles,
    mol.name,

    -- Все поля из справочника публикаций
    pub.publication_id,
    pub.doi,
    pub.pmid,
    pub.title,
    pub.publisher,
    pub.year,
    pub.access,

    -- Все поля из справочника источников
    src.source_id,
    src.source_table,
    src.dbt_loaded_at,
    src.dbt_curated_at,

    -- Все оставшиеся поля из таблицы фактов
    fact.perm_original,
    fact.perm_numeric,
    fact.logp_original,
    fact.logp_numeric,
    fact.logp_cleaned,
    fact.page,
    fact.origin

FROM
    {{ ref('fact_eyedrops_measurements') }} AS fact

LEFT JOIN {{ ref('dim_eyedrops_molecules') }} AS mol
    ON fact.molecule_id = mol.molecule_id

LEFT JOIN {{ ref('dim_eyedrops_publications') }} AS pub
    ON fact.publication_id = pub.publication_id

LEFT JOIN {{ ref('dim_eyedrops_source') }} AS src
    ON fact.source_id = src.source_id
