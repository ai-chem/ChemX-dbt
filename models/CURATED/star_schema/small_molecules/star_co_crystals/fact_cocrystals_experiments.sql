-- fact_cocrystals_experiments.sql
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (experiment_id)"
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY cur.doi, cur.name_cocrystal) AS experiment_id,

    -- Добавляем внешние ключи из справочников
    dim_pub.publication_id,
    dim_cocrystal_def.cocrystal_definition_id,
    dim_src.source_id,

    dim_mol_drug.molecule_id AS drug_molecule_id,
    dim_mol_coformer.molecule_id AS coformer_molecule_id,

    cur.photostability_change,
    cur.photostability_change_type_file,
    cur.photostability_change_origin,
    cur.photostability_change_page,
    cur.name_drug_type_file,
    cur.name_drug_origin,
    cur.name_drug_page,
    cur.smiles_drug_type_file,
    cur.smiles_drug_origin,
    cur.smiles_drug_page,
    cur.name_coformer_type_file,
    cur.name_coformer_origin,
    cur.name_coformer_page,
    cur.smiles_coformer_type_file,
    cur.smiles_coformer_origin,
    cur.smiles_coformer_page

FROM
    {{ ref('final_cur_co_crystals') }} AS cur

-- Присоединяем справочник публикаций
LEFT JOIN {{ ref('dim_cocrystals_publications') }} AS dim_pub
    ON cur.pdf IS NOT DISTINCT FROM dim_pub.pdf
    AND cur.doi IS NOT DISTINCT FROM dim_pub.doi
    AND cur.supplementary IS NOT DISTINCT FROM dim_pub.supplementary
    AND cur.authors IS NOT DISTINCT FROM dim_pub.authors
    AND cur.title IS NOT DISTINCT FROM dim_pub.title
    AND cur.journal IS NOT DISTINCT FROM dim_pub.journal
    AND cur.year IS NOT DISTINCT FROM dim_pub.year
    AND cur.page IS NOT DISTINCT FROM dim_pub.page
    AND cur.access IS NOT DISTINCT FROM dim_pub.access

-- Присоединяем справочник определений сокристаллов
LEFT JOIN {{ ref('dim_cocrystals_cocrystal_definitions') }} AS dim_cocrystal_def
    ON cur.name_cocrystal IS NOT DISTINCT FROM dim_cocrystal_def.name_cocrystal
    AND cur.name_cocrystal_type_file IS NOT DISTINCT FROM dim_cocrystal_def.name_cocrystal_type_file
    AND cur.name_cocrystal_page IS NOT DISTINCT FROM dim_cocrystal_def.name_cocrystal_page
    AND cur.name_cocrystal_origin IS NOT DISTINCT FROM dim_cocrystal_def.name_cocrystal_origin
    AND cur.ratio_cocrystal IS NOT DISTINCT FROM dim_cocrystal_def.ratio_cocrystal
    AND cur.ratio_cocrystal_page IS NOT DISTINCT FROM dim_cocrystal_def.ratio_cocrystal_page
    AND cur.ratio_cocrystal_page_1 IS NOT DISTINCT FROM dim_cocrystal_def.ratio_cocrystal_page_1
    AND cur.ratio_cocrystal_origin IS NOT DISTINCT FROM dim_cocrystal_def.ratio_cocrystal_origin
    AND cur.ratio_component_1 IS NOT DISTINCT FROM dim_cocrystal_def.ratio_component_1
    AND cur.ratio_component_2 IS NOT DISTINCT FROM dim_cocrystal_def.ratio_component_2

-- Присоединяем справочник источников
LEFT JOIN {{ ref('dim_cocrystals_source') }} AS dim_src
    ON cur.source_table IS NOT DISTINCT FROM dim_src.source_table
    AND cur.dbt_loaded_at IS NOT DISTINCT FROM dim_src.dbt_loaded_at
    AND cur.dbt_curated_at IS NOT DISTINCT FROM dim_src.dbt_curated_at



-- =================================================================================================
-- Пояснение к коду ниже:
-- В каждой строке таблицы фактов у нас есть две роли для молекул:
-- одна молекула выступает как лекарство, а другая как коформер.
-- Нам нужно найти уникальный ID для каждой из них в нашем едином справочнике.
-- ==========================================================================================


-- Присоединяем справочник молекул для smiles_drug
LEFT JOIN {{ ref('dim_cocrystals_molecules') }} AS dim_mol_drug
    ON cur.smiles_drug IS NOT DISTINCT FROM dim_mol_drug.smiles
    AND cur.name_drug IS NOT DISTINCT FROM dim_mol_drug.molecule_name

-- Присоединяем справочник молекул для smiles_coformer
LEFT JOIN {{ ref('dim_cocrystals_molecules') }} AS dim_mol_coformer
    ON cur.smiles_coformer IS NOT DISTINCT FROM dim_mol_coformer.smiles
    AND cur.name_coformer IS NOT DISTINCT FROM dim_mol_coformer.molecule_name
