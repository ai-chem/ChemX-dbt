{% macro standardize_vmax(value_column, unit_column) %}
    (
        -- Сначала проверяем, не является ли юнит массовым (mg, g).
        -- Если да, мы не можем его стандартизировать и возвращаем NULL.
        case
            when lower({{ unit_column }}) like '%mg%' or lower({{ unit_column }}) like '%g/%'
            then null
            else
                {{ value_column }} -- Берем исходное значение

                -- Умножаем на коэффициент множителя (10^-x)
                * case
                    when replace(replace(lower({{ unit_column }}), '−', '-'), ' ', '') like '%10-3%' then 1e-3
                    when replace(replace(lower({{ unit_column }}), '−', '-'), ' ', '') like '%10-4%' then 1e-4
                    when replace(replace(lower({{ unit_column }}), '−', '-'), ' ', '') like '%10-5%' then 1e-5
                    when replace(replace(lower({{ unit_column }}), '−', '-'), ' ', '') like '%10-6%' then 1e-6
                    when replace(replace(lower({{ unit_column }}), '−', '-'), ' ', '') like '%10-7%' then 1e-7
                    when replace(replace(lower({{ unit_column }}), '−', '-'), ' ', '') like '%10-8%' then 1e-8
                    when replace(replace(lower({{ unit_column }}), '−', '-'), ' ', '') like '%10-9%' then 1e-9
                    else 1.0
                end

                -- Умножаем на коэффициент концентрации
                * case
                    when replace(replace(lower({{ unit_column }}), '−', '-'), ' ', '') like '%mm%' then 1e-3
                    when replace(replace(lower({{ unit_column }}), '−', '-'), ' ', '') like '%μm%' or replace(replace(lower({{ unit_column }}), '−', '-'), ' ', '') like '%um%' then 1e-6
                    when replace(replace(lower({{ unit_column }}), '−', '-'), ' ', '') like '%nm%' then 1e-9
                    else 1.0
                  end

                -- Делим на коэффициент времени
                / case
                    when replace(replace(lower({{ unit_column }}), '−', '-'), ' ', '') like '%min%' then 60.0
                    else 1.0
                  end
        end
    )
{% endmacro %}
