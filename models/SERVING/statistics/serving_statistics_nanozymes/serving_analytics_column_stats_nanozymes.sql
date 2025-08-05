{{ config(materialized='table', schema='serving') }}

{{ cur_column_stats('serving_all_data_nanozymes') }}