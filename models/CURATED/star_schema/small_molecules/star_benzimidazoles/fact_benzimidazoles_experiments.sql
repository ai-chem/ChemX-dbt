    -- fact_benzimidazoles_experiments.sql
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (experiment_id)"
) }}

SELECT
    -- Создаем первичный ключ для самой таблицы фактов
    ROW_NUMBER() OVER (ORDER BY cur.smiles, cur.doi, cur.bacteria_unified) AS experiment_id,

    -- Добавляем внешние ключи из присоединенных справочников
    dim_mol.molecule_id,
    dim_bact.bacteria_id,
    dim_pub.publication_id,
    dim_src.source_id,

    -- Добавляем все оставшиеся столбцы - меры и контекст эксперимента
    cur.target_value_parsed,
    cur.target_value_error,
    cur.target_value,
    cur.compound_id,
    cur.target_type,
    cur.target_relation,
    cur.target_units,
    cur.page_target,
    cur.origin_target,
    cur.section_target,
    cur.subsection_target

FROM
    {{ ref('final_cur_benzimidazoles') }} AS cur

-- Присоединяем справочник молекул по ПОЛНОМУ набору полей
LEFT JOIN {{ ref('dim_benzimidazoles_molecules') }} AS dim_mol
    ON cur.smiles IS NOT DISTINCT FROM dim_mol.smiles
    AND cur.compound_id IS NOT DISTINCT FROM dim_mol.compound_id
    AND cur.origin_scaffold IS NOT DISTINCT FROM dim_mol.origin_scaffold
    AND cur.origin_residue IS NOT DISTINCT FROM dim_mol.origin_residue
    AND cur.page_scaffold IS NOT DISTINCT FROM dim_mol.page_scaffold
    AND cur.page_residue IS NOT DISTINCT FROM dim_mol.page_residue

LEFT JOIN {{ ref('dim_benzimidazoles_bacteria') }} AS dim_bact
    ON cur.bacteria IS NOT DISTINCT FROM dim_bact.bacteria
    AND cur.bacteria_unified IS NOT DISTINCT FROM dim_bact.bacteria_unified
    AND cur.page_bacteria IS NOT DISTINCT FROM dim_bact.page_bacteria
    AND cur.origin_bacteria IS NOT DISTINCT FROM dim_bact.origin_bacteria
    AND cur.section_bacteria IS NOT DISTINCT FROM dim_bact.section_bacteria
    AND cur.subsection_bacteria IS NOT DISTINCT FROM dim_bact.subsection_bacteria

LEFT JOIN {{ ref('dim_benzimidazoles_publications') }} AS dim_pub
    ON cur.doi IS NOT DISTINCT FROM dim_pub.doi
    AND cur.title IS NOT DISTINCT FROM dim_pub.title
    AND cur.publisher IS NOT DISTINCT FROM dim_pub.publisher
    AND cur.year IS NOT DISTINCT FROM dim_pub.year
    AND cur.access IS NOT DISTINCT FROM dim_pub.access
    AND cur.pdf IS NOT DISTINCT FROM dim_pub.pdf

LEFT JOIN {{ ref('dim_benzimidazoles_source') }} AS dim_src
    ON cur.source_table IS NOT DISTINCT FROM dim_src.source_table
    AND cur.dbt_loaded_at IS NOT DISTINCT FROM dim_src.dbt_loaded_at
    AND cur.dbt_curated_at IS NOT DISTINCT FROM dim_src.dbt_curated_at
