
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (molecule_id)"
) }}

WITH unique_molecule_references AS (
    SELECT DISTINCT
        smiles,
        name
    FROM
        {{ ref('final_cur_eyedrops') }}
    WHERE
        smiles IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            smiles,
            name
    ) AS molecule_id,
    smiles,
    name
FROM
    unique_molecule_references
