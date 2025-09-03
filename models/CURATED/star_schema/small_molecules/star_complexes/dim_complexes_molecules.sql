{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (molecule_id)"
) }}

WITH unique_molecule_references AS (
    SELECT DISTINCT
        smiles,
        smiles_type,
        compound_name,
        page_smiles,
        origin_smiles
    FROM
        {{ ref('final_cur_complexes') }}
    WHERE
        smiles IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            smiles,
            compound_name,
            page_smiles
    ) AS molecule_id,

    smiles,
    smiles_type,
    compound_name,
    page_smiles,
    origin_smiles
FROM
    unique_molecule_references
