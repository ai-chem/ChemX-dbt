-- 1. Строки без пропущенных значений
{{ raw_get_rows_without_nulls('raw', 'oxazolidinones') }}

-- 2. Первые 5 столбцов
{{ raw_get_first_n_columns('raw', 'oxazolidinones') }}

-- 3. кол-во пропущенных значений
{{ raw_column_stats('raw', 'oxazolidinones') }}
