-- 1. Строки без пропущенных значений
{{ raw_get_rows_without_nulls('raw', 'benzimidazoles') }}

-- 2. Первые 5 столбцов
{{ raw_get_first_n_columns('raw', 'benzimidazoles') }}

-- 3. кол-во пропущенных значений
{{ raw_column_stats('raw', 'benzimidazoles') }}

{#-- 4. кол-во дубликатов#}
{##}
{#-- Подсчет дубликатов в таблице benzimidazoles#}
{#{{ raw_count_full_duplicates('raw', 'benzimidazoles') }}#}
