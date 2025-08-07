

| №   | column_name  | description                                                                                               |
| --- | ------------ | --------------------------------------------------------------------------------------------------------- |
| 0   | surface      | Type of surface modification of the nanoparticle (e.g., functionalization, coating, doping molecule name) |
| 1   | syngony      | The crystal system of the nanozyme (e.g., cubic, hexagonal, monoclinic, etc.)                             |
| 2   | c_const_unit | Unit of measurement for the constant concentration (e.g., mM, µM)                                         |
| 3   | c_const      | Value of the constant concentration of an unchanged parameter in the reaction                             |
| 4   | length       | Length of the nanoparticle in nanometers                                                                  |
| 5   | length_lower | Lower bound of nanoparticle length in nanometers                                                          |
| 6   | length_mean  | Mean value of nanoparticle length in nanometers                                                           |
| 7   | length_upper | Upper bound of nanoparticle length in nanometers                                                          |
| 8   | width        | Width of the nanoparticle in nanometers                                                                   |
| 9   | width_lower  | Lower bound of nanoparticle width in nanometers                                                           |
| 10  | width_mean   | Mean value of nanoparticle width in nanometers                                                            |
| 11  | width_upper  | Upper bound of nanoparticle width in nanometers                                                           |
| 12  | depth        | Depth/thickness of the nanoparticle in nanometers                                                         |
| 13  | depth_lower  | Lower bound of nanoparticle depth in nanometers                                                           |
| 14  | depth_mean   | Mean value of nanoparticle depth in nanometers                                                            |
| 15  | depth_upper  |  Upper bound of nanoparticle depth in nanometers                                                          |
| 16  | temperature  | The temperature at which the reaction was conducted (in °C)                                               |

---

### **Кинетика и условия реакции**

| №  | column_name      | description                                                                                   |
|----|------------------|----------------------------------------------------------------------------------------------|
| 17 | ccat_unit        | Unit of measurement for the catalyst concentration (e.g., mM, µM, mg/ml)                     |
| 18 | ccat_value       | Value of the catalyst (nanozyme) concentration                                               |
| 19 | c_max            | Maximum substrate concentration in millimolar                                                |
| 20 | c_min            | Minimum substrate concentration in millimolar                                                |
| 21 | vmax_unit        | Unit of measurement for Vmax (e.g., mM/s, µM/min)                                            |
| 22 | vmax_value       | The numerical value of the maximum reaction rate (Vmax)                                      |
| 23 | ph               | pH value at which the reaction was conducted                                                 |
| 24 | km_unit          | Unit of measurement for the Michaelis constant (e.g., mM, µM)                               |
| 25 | km_value         | The numerical value of the Michaelis constant (Km)                                           |

---

### **Метаданные и публикации**

| №   | column_name    | description                                                        |
| --- | -------------- | ------------------------------------------------------------------ |
| 26  | access         | Type of access to the publication (e.g., open, closed)             |
| 27  | access_bool    | Boolean flag indicating open access (1) or closed (0)              |
| 28  | activity       | Type of catalytic activity of the nanozyme                         |
| 29  | dbt_curated_at | Timestamp when the record was curated in dbt                       |
| 30  | dbt_loaded_at  | Timestamp when the record was loaded into dbt                      |
| 31  | doi            | DOI of the article                                                 |
| 32  | nanoparticle   | The chemical formula of the nanozyme (e.g., Fe₃O₄, CeO₂, Au, etc.) |
| 33  | journal        | Name of the journal where the article was published                |
| 34  | pdf            | Link or path to the PDF of the article                             |
| 35  | reaction_type  | Type of catalyzed reaction (e.g., TMB + H₂O₂ or TMB, etc.)         |
| 36  | source_table   | Name of the source table from which the data was extracted         |
| 37  | target_source  | Source or origin of the target data                                |
| 38  | title          | Title of the article                                               |
| 39  | year           | Publication year                                                   |


**Одна строка = результат каталитического эксперимента с одной наночастицей (nanozyme)**,
при определённых условиях — температуре, pH, концентрации;
с измерениями свойств типа **Vmax, Km**, активности, и т.п.

## **Какие сущности здесь описаны?**

| Сущность                      | Представлена через поля                                        |
| ----------------------------- | -------------------------------------------------------------- |
| 🧪 **Наночастица (nanozyme)** | `formula`, `surface`, `syngony`, `length`, `width`, `depth`    |
| ⚗ **Реакция**                 | `reaction_type`, `c_min`, `c_max`, `ccat_value`, `temperature` |
| 📊 **Кинетика**               | `vmax_value`, `km_value` и их `unit`                           |
| 🪄 **Активность**             | `activity` = peroxidase / catalase / oxidase и т.п.            |
| 📰 **Статья**                 | `doi`, `journal`, `year`, `pdf`, `title`, `access`             |
| 🧱 **DBT-системные**          | `dbt_loaded_at`, `dbt_curated_at`, `source_table`              |
