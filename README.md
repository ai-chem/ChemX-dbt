# ChemX_dbt

Полный ELT-проект на базе **dbt** для обработки, нормализации и аналитики данных по наноматериалам и малым молекулам.  
Реализована слоистая архитектура: raw - unified - curated - serving (statistics + ML).

---

## Требования и установка

1. Клонировать репозиторий и перейти в папку:
   ```bash
   git clone https://github.com/ai-chem/ChemX_dbt.git
   cd ChemX_dbt
   ```
2. Настроить соединение с БД в `profiles.yml`:
   ```yaml
   your_profile:
     target: dev
     outputs:
       dev:
         type: postgres
         host: ...
         user: ...
         password: ...
         dbname: ...
         schema: staging
         threads: 4
   ```
3. Поставить `dbt-core` и плагины:
   ```bash
   pip install dbt-core dbt-postgres
   ```

---

## Структура проекта

```text
.
├─ data/
│  └─ raw/                      # Исходные CSV + скрипт загрузки
├─ macros/                      # Jinja-макросы для парсинга, нормализации и тестов
├─ models/
│  ├─ CURATED/                  # Очистка и подготовка
│  ├─ UNIFIED/                  # Оригинальные данные с небольшими изменениями
│  └─ SERVING/                  # statistics + ML tables + all_data
├─ analyses/                    # SQL-скрипты для доп. аналитики
├─ tests/                       # Пользовательские проверки (sql)
├─ seeds/                       # Не используется
├─ snapshots/                   # Не используется
├─ dbt_project.yml              # Основной конфиг dbt
├─ profiles.yml                 # Профиль подключения
```

Глубина папок внутри `models` подробно описана ниже.

---

## Основные директории и модели

### 1. data/raw  
- `*.csv` — исходники по 10 темам (Benzimidazoles, Cytotox, …, Synergy).  
- `scripts/load_csv_to_postgres.py` — загружает raw CSV в СУБД.

### 2. macros  
- **analyses_raw** / **analyses_curated** — макросы для автоматического сбора базовой статистики (таблица/столбцы).  
- **parsing_curated** — парсинг чисел, дат, сложных текстовых полей.  
- **normalize_curated** — стандартизация имен частиц, форм, единиц.  
- Общие макросы: `deduplicate_model`, `test_no_duplicates`, `generate_canonical_name` и т.п.

### 3. models/CURATED  
#### 3.1. nanomaterials & small_molecules  
- **prep/** — промежуточные модели (`cur_<topic>.sql`)  
- **final/** — итоговые очищенные таблицы + `schema.yml` с описанием колонок  

#### 3.2. star_schema  
Для каждой темы:
- `dim_*` — измерения (наночастица, публикация, источник, бактерия и т.д.)  
- `fact_*_experiments` — события/измерения, ссылаются на `dim_*`

Каждый каталог содержит SQL-файлы и `schema.yml`.

### 4. models/UNIFIED  
- `uni_<topic>.sql` — объединяет данные из **CURATED** по всем источникам в одну витрину.  
- Один общий `schema.yml` для всех unified-моделей.

### 5. models/SERVING  
#### 5.1. all_data  
Сводные витрины: `serving_all_data_<topic>.sql`.

#### 5.2. ml  
Таблицы, готовые к обучению ML: `serving_ml_<topic>.sql` + `schema.yml`.

#### 5.3. statistics  
Для каждой темы — отдельная папка:
- `schema.yml`  
- `serving_analytics_column_stats_<topic>.sql`  
- `serving_analytics_row_stats_<topic>.sql`  
- `serving_analytics_top_categories_<topic>.sql`


---

## Запуск и основные команды

```bash

# Построить весь пайплайн
dbt build

# Запустить тесты
dbt test

# Сгенерировать и просмотреть документацию
dbt docs generate
dbt docs serve
```

---

## Особенности проекта

- **Составной ключ для наночастицы**  
  nanoparticle + normalized_shape + has_coating + np_size_avg_nm  
  используется во всех dim-таблицах кроме nanomag и nanozymes, чтобы гарантировать уникальность и однозначность.

---

> README будет регулярно обновляться по мере роста проекта.