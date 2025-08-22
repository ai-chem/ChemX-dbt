{{ config(
    materialized='table',
    schema='star_schema',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (experiment_id)"
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY cur.smiles, cur.doi, cur.bacteria_name_unified) AS experiment_id,

    dim_mol.molecule_id,
    dim_bact.bacteria_id,
    dim_pub.publication_id,
    dim_src.source_id,

    cur.target_value_parsed,
    cur.compound_id,
    cur.target_type,
    cur.target_value,
    cur.target_relation,
    cur.target_units,
    cur.page_target,
    cur.origin_target,
    cur.section_target,
    cur.column_prop,
    cur.line_prop

FROM
    {{ ref('final_cur_oxazolidinones') }} AS cur

LEFT JOIN {{ ref('dim_oxazolidinones_molecules') }} AS dim_mol
    ON cur.smiles IS NOT DISTINCT FROM dim_mol.smiles
    AND cur.page_scaffold IS NOT DISTINCT FROM dim_mol.page_scaffold
    AND cur.origin_scaffold IS NOT DISTINCT FROM dim_mol.origin_scaffold
    AND cur.section_scaffold IS NOT DISTINCT FROM dim_mol.section_scaffold
    AND cur.page_residue IS NOT DISTINCT FROM dim_mol.page_residue
    AND cur.origin_residue IS NOT DISTINCT FROM dim_mol.origin_residue
    AND cur.section_residue IS NOT DISTINCT FROM dim_mol.section_residue

LEFT JOIN {{ ref('dim_oxazolidinones_bacteria') }} AS dim_bact
    ON cur.bacteria IS NOT DISTINCT FROM dim_bact.bacteria
    AND cur.bacteria_name_unified IS NOT DISTINCT FROM dim_bact.bacteria_name_unified
    AND cur.bacteria_info IS NOT DISTINCT FROM dim_bact.bacteria_info
    AND cur.page_bacteria IS NOT DISTINCT FROM dim_bact.page_bacteria
    AND cur.origin_bacteria IS NOT DISTINCT FROM dim_bact.origin_bacteria
    AND cur.section_bacteria IS NOT DISTINCT FROM dim_bact.section_bacteria
    AND cur.subsection_bacteria IS NOT DISTINCT FROM dim_bact.subsection_bacteria

LEFT JOIN {{ ref('dim_oxazolidinones_publications') }} AS dim_pub
    ON cur.pdf IS NOT DISTINCT FROM dim_pub.pdf
    AND cur.doi IS NOT DISTINCT FROM dim_pub.doi
    AND cur.title IS NOT DISTINCT FROM dim_pub.title
    AND cur.publisher IS NOT DISTINCT FROM dim_pub.publisher
    AND cur.year IS NOT DISTINCT FROM dim_pub.year
    AND cur.access IS NOT DISTINCT FROM dim_pub.access
    AND cur.access_bool IS NOT DISTINCT FROM dim_pub.access_bool

LEFT JOIN {{ ref('dim_oxazolidinones_source') }} AS dim_src
    ON cur.source_table IS NOT DISTINCT FROM dim_src.source_table
    AND cur.dbt_loaded_at IS NOT DISTINCT FROM dim_src.dbt_loaded_at
