
{{ config(
    materialized='table',
    schema='serving',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (experiment_id)"
) }}

SELECT
    -- Ключ из таблицы фактов
    fact.experiment_id,
    -- Все поля из справочника молекул
    mol.molecule_id,
    mol.smiles,
    mol.page_scaffold,
    mol.origin_scaffold,
    mol.section_scaffold,
    mol.page_residue,
    mol.origin_residue,
    mol.section_residue,

    -- Все поля из справочника бактерий
    bact.bacteria_id,
    bact.bacteria,
    bact.bacteria_name_unified,
    bact.bacteria_info,
    bact.page_bacteria,
    bact.origin_bacteria,
    bact.section_bacteria,
    bact.subsection_bacteria,

    -- Все поля из справочника публикаций
    pub.publication_id,
    pub.pdf,
    pub.doi,
    pub.title,
    pub.publisher,
    pub.year,
    pub.access,
    pub.access_bool,

    -- Все поля из справочника источников
    src.source_id,
    src.source_table,
    src.dbt_loaded_at,

    -- Все оставшиеся поля из таблицы фактов
    fact.compound_id,
    fact.target_type,
    fact.target_value,
    fact.target_value_parsed,
    fact.target_relation,
    fact.target_units,
    fact.page_target,
    fact.origin_target,
    fact.section_target,
    fact.column_prop,
    fact.line_prop

FROM
    {{ ref('fact_oxazolidinones_experiments') }} AS fact

LEFT JOIN {{ ref('dim_oxazolidinones_molecules') }} AS mol
    ON fact.molecule_id = mol.molecule_id

LEFT JOIN {{ ref('dim_oxazolidinones_bacteria') }} AS bact
    ON fact.bacteria_id = bact.bacteria_id

LEFT JOIN {{ ref('dim_oxazolidinones_publications') }} AS pub
    ON fact.publication_id = pub.publication_id

LEFT JOIN {{ ref('dim_oxazolidinones_source') }} AS src
    ON fact.source_id = src.source_id
