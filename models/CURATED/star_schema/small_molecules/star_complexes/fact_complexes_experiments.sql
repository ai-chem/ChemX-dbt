{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (experiment_id)"
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY cur.doi, cur.smiles, cur.metal) AS experiment_id,

    dim_pub.publication_id,
    dim_mol.molecule_id,
    dim_metal.metal_id,
    dim_src.source_id,

    cur.compound_id,
    cur.target_numeric,
    cur.target_original,
    cur.target_cleaned,
    cur.page_target_value,
    cur.origin_target_value

FROM
    {{ ref('final_cur_complexes') }} AS cur

LEFT JOIN {{ ref('dim_complexes_publications') }} AS dim_pub
    ON cur.pdf IS NOT DISTINCT FROM dim_pub.pdf
    AND cur.doi IS NOT DISTINCT FROM dim_pub.doi
    AND cur.doi_sourse IS NOT DISTINCT FROM dim_pub.doi_sourse
    AND cur.supplementary IS NOT DISTINCT FROM dim_pub.supplementary
    AND cur.supplementary_bool IS NOT DISTINCT FROM dim_pub.supplementary_bool
    AND cur.title IS NOT DISTINCT FROM dim_pub.title
    AND cur.publisher IS NOT DISTINCT FROM dim_pub.publisher
    AND cur.year IS NOT DISTINCT FROM dim_pub.year
    AND cur.access IS NOT DISTINCT FROM dim_pub.access

LEFT JOIN {{ ref('dim_complexes_molecules') }} AS dim_mol
    ON cur.smiles IS NOT DISTINCT FROM dim_mol.smiles
    AND cur.smiles_type IS NOT DISTINCT FROM dim_mol.smiles_type
    AND cur.compound_name IS NOT DISTINCT FROM dim_mol.compound_name
    AND cur.page_smiles IS NOT DISTINCT FROM dim_mol.page_smiles
    AND cur.origin_smiles IS NOT DISTINCT FROM dim_mol.origin_smiles

LEFT JOIN {{ ref('dim_complexes_metals') }} AS dim_metal
    ON cur.metal IS NOT DISTINCT FROM dim_metal.metal
    AND cur.page_metal IS NOT DISTINCT FROM dim_metal.page_metal
    AND cur.origin_metal IS NOT DISTINCT FROM dim_metal.origin_metal

LEFT JOIN {{ ref('dim_complexes_source') }} AS dim_src
    ON cur.source_table IS NOT DISTINCT FROM dim_src.source_table
    AND cur.dbt_loaded_at IS NOT DISTINCT FROM dim_src.dbt_loaded_at
    AND cur.dbt_curated_at IS NOT DISTINCT FROM dim_src.dbt_curated_at
