-- models/UNIFIED/uni_co_crystals.sql
{{ config(
    materialized='view',
    schema='unified'
) }}

SELECT
    -- Все исходные поля с корректными типами данных
    pdf,
    doi,
    supplementary,
    authors,
    title,
    journal,
    year,
    CAST(page AS INTEGER) AS page, -- Преобразуем в INTEGER
    access,
    name_cocrystal,
    name_cocrystal_type_file,
    name_cocrystal_page,
    name_cocrystal_origin,
    ratio_cocrystal,
    ratio_cocrystal_page,
    "ratio_cocrystal_page.1" AS ratio_cocrystal_page_1,
    CAST(ratio_cocrystal_origin AS INTEGER) AS ratio_cocrystal_origin, -- Преобразуем в INTEGER
    name_drug,
    name_drug_type_file,
    name_drug_origin,
    name_drug_page,
    smiles_drug,
    smiles_drug_type_file,
    smiles_drug_origin,
    CAST(smiles_drug_page AS INTEGER) AS smiles_drug_page, -- Преобразуем в INTEGER
    name_coformer,
    name_coformer_type_file,
    name_coformer_origin,
    CAST(name_coformer_page AS INTEGER) AS name_coformer_page, -- Преобразуем в INTEGER
    smiles_coformer,
    smiles_coformer_type_file,
    smiles_coformer_origin,
    CAST(smiles_coformer_page AS INTEGER) AS smiles_coformer_page, -- Преобразуем в INTEGER
    photostability_change,
    photostability_change_type_file,
    photostability_change_origin,
    CAST(photostability_change_page AS INTEGER) AS photostability_change_page, -- Преобразуем в INTEGER

    -- Добавляем метаданные
    current_timestamp AS dbt_loaded_at,
    'co_crystals' AS source_table
FROM
    {{ source('raw', 'co_crystals') }}
