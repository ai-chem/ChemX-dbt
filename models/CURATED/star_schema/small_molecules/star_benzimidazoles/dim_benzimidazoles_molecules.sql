-- dim_benzimidazoles_molecules.sql
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (molecule_id)"
) }}

WITH unique_molecule_references AS (
    -- Находим все уникальные комбинации полей, относящихся к молекулам
    SELECT DISTINCT
        smiles,
        compound_id,
        origin_scaffold,
        origin_residue,
        page_scaffold,
        page_residue
    FROM
        {{ ref('final_cur_benzimidazoles') }}
)

-- Генерируем ключ для каждой уникальной комбинации
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            smiles,
            compound_id,
            page_scaffold,
            page_residue
    ) AS molecule_id,

    -- Включаем в справочник ВСЕ поля, относящиеся к молекулам
    smiles,
    compound_id,
    origin_scaffold,
    origin_residue,
    page_scaffold,
    page_residue
FROM
    unique_molecule_references
