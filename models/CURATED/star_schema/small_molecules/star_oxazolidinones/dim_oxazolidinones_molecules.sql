
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (molecule_id)"
) }}

WITH unique_molecule_references AS (
    SELECT DISTINCT
        smiles,
        page_scaffold,
        origin_scaffold,
        section_scaffold,
        page_residue,
        origin_residue,
        section_residue
    FROM
        {{ ref('final_cur_oxazolidinones') }}
    WHERE
        smiles IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            smiles,
            page_scaffold,
            page_residue
    ) AS molecule_id,

    smiles,
    page_scaffold,
    origin_scaffold,
    section_scaffold,
    page_residue,
    origin_residue,
    section_residue
FROM
    unique_molecule_references
