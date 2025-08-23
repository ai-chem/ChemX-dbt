-- models/STAR_SCHEMA/fact_eyedrops_measurements.sql
{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (measurement_id)"
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY cur.smiles, cur.doi) AS measurement_id,

    dim_mol.molecule_id,
    dim_pub.publication_id,
    dim_src.source_id,

    cur.perm_original,
    cur.perm_numeric,
    cur.logp_original,
    cur.logp_numeric,
    cur.page,
    cur.origin

FROM
    {{ ref('final_cur_eyedrops') }} AS cur

LEFT JOIN {{ ref('dim_eyedrops_molecules') }} AS dim_mol
    ON cur.smiles IS NOT DISTINCT FROM dim_mol.smiles
    AND cur.name IS NOT DISTINCT FROM dim_mol.name

LEFT JOIN {{ ref('dim_eyedrops_publications') }} AS dim_pub
    ON cur.doi IS NOT DISTINCT FROM dim_pub.doi
    AND cur.pmid IS NOT DISTINCT FROM dim_pub.pmid
    AND cur.title IS NOT DISTINCT FROM dim_pub.title
    AND cur.publisher IS NOT DISTINCT FROM dim_pub.publisher
    AND cur.year IS NOT DISTINCT FROM dim_pub.year
    AND cur.access IS NOT DISTINCT FROM dim_pub.access

LEFT JOIN {{ ref('dim_eyedrops_source') }} AS dim_src
    ON cur.source_table IS NOT DISTINCT FROM dim_src.source_table
    AND cur.dbt_loaded_at IS NOT DISTINCT FROM dim_src.dbt_loaded_at
    AND cur.dbt_curated_at IS NOT DISTINCT FROM dim_src.dbt_curated_at
