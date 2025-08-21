-- dim_cocrystals_molecules.sql
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (molecule_id)"
) }}



-- ==============================================================================================
-- ЦЕЛЬ: Создать ЕДИНЫЙ справочник для всех молекул участвующих в экспериментах
-- В исходных данных одна и та же молекула может выступать в разных ролях
-- как лекарство 'drug' или коформер 'coformer'.
-- Представим, что мы строим стену из кирпича красного и чёрного цвета, в нашем случае молекула
-- может быть как красным, так и чёрным кирпичом, в зависимости от контекста.
-- =================================================================================================


WITH all_molecules AS (
    -- Собираем все молекулы-лекарства
    SELECT
        smiles_drug AS smiles,
        name_drug AS molecule_name
    FROM
        {{ ref('final_cur_co_crystals') }}
    WHERE
        smiles_drug IS NOT NULL

    UNION ALL

    -- Добавляем к ним все молекулы коформеры
    SELECT
        smiles_coformer AS smiles,
        name_coformer AS molecule_name
    FROM
        {{ ref('final_cur_co_crystals') }}
    WHERE
        smiles_coformer IS NOT NULL
),

unique_molecules AS (
    -- Находим уникальные комбинации SMILES и имени
    SELECT DISTINCT
        smiles,
        molecule_name
    FROM
        all_molecules
)

-- Генерируем ключ для каждой уникальной молекулы
SELECT
    ROW_NUMBER() OVER (ORDER BY smiles, molecule_name) AS molecule_id,
    smiles,
    molecule_name
FROM
    unique_molecules
