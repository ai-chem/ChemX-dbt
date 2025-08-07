-- 1. Строки без пропущенных значений
{{ cur_get_rows_without_nulls('cur_benzimidazoles') }}

-- 2. Первые 5 столбцов
{{ cur_get_first_n_columns('cur_benzimidazoles') }}

-- 3. Кол-во пропущенных значений
{{ cur_column_stats('cur_benzimidazoles') }}

{#-- 4. Кол-во дубликатов#}
{#-- {{ cur_count_full_duplicates('cur_benzimidazoles') }}#}
