-- 1. Строки без пропущенных значений
{{ cur_get_rows_without_nulls('final_cur_co_crystals') }}

-- 2. Первые 5 столбцов
{{ cur_get_first_n_columns('final_cur_co_crystals') }}

-- 3. Кол-во пропущенных значений
{{ cur_column_stats('final_cur_co_crystals') }}

{#-- 4. Кол-во дубликатов#}
{#-- {{ cur_count_full_duplicates('cur_co_crystals') }}#}
