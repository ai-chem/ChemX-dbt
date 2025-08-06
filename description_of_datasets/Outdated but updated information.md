# Подробное описание датасетов проекта ChemX

## 1. Benzimidazoles

**Описание**: Данные о бензимидазолах, извлеченные из научных публикаций, включая их антибактериальную активность против различных бактериальных штаммов.

**Структура таблицы**:

| Название столбца | Описание |
|-----------------|----------|
| subsection_target | Подраздел, связанный с целевым объектом |
| subsection_bacteria | Подраздел, связанный с бактериями |
| section_target | Раздел, связанный с целевым объектом |
| section_bacteria | Раздел, связанный с бактериями |
| target_value_error | Погрешность целевого значения |
| bacteria_unified | Унифицированное название бактерии |
| origin_residue | Исходный остаток |
| page_residue | Номер страницы с информацией об остатке |
| access | Идентификатор доступа |
| bacteria | Название бактерии |
| compound_id | Идентификатор соединения |
| dbt_curated_at | Временная метка курирования данных |
| dbt_loaded_at | Временная метка загрузки данных |
| doi | Цифровой идентификатор объекта (DOI) |
| origin_bacteria | Исходное название бактерии |
| origin_scaffold | Исходный каркас |
| origin_target | Исходная цель |
| page_bacteria | Номер страницы с информацией о бактерии |
| page_scaffold | Номер страницы с информацией о каркасе |
| page_target | Номер страницы с информацией о цели |
| pdf | Ссылка на PDF-документ |
| publisher | Издатель |
| smiles | SMILES-представление молекулы |
| source_table | Исходная таблица |
| target_relation | Отношение к цели |
| target_type | Тип цели |
| target_units | Единицы измерения целевого значения |
| target_value | Целевое значение (текст) |
| target_value_parsed | Обработанное целевое значение (число) |
| title | Заголовок |
| year | Год |

Хотите ли вы получить более подробное описание каких-либо конкретных столбцов?

**Пример данных**:

```
"Cc1ccc(C2CC(c3nc4ccccc4[nH]3)=NN2C(=O)c2ccncc2)cc1"	"10.3390/antibiotics10081002"	"Synthesis of Antimicrobial Benzimidazole–Pyrazole Compounds and Their Biological Activities"	"MDPI AG"	2021	1	"3h"	"MIC"	"'='"	"25"	"µg mL-1"	"E. coli"	"Escherichia coli"	2	"text"	"2. Synthesis, Antimicrobial Activities of Benzimidazole–Pyrazole Compounds. Benzimidazole–Pyrazole Compounds as Potent DNA Gyrase and Topoisomerase IV Inhibitors"	"2.1. Benzimidazoles Substituted in the "2" Position with Pyrazole Moiety"	2	"text"	"2. Synthesis, Antimicrobial Activities of Benzimidazole–Pyrazole Compounds. Benzimidazole–Pyrazole Compounds as Potent DNA Gyrase and Topoisomerase IV Inhibitors"	"2.1. Benzimidazoles Substituted in the "2" Position with Pyrazole Moiety"	2	"scheme 1"	2	"scheme 1"	"antibiotics10081002"
```

**Особенности и проблемы**:

- `target_value` содержит числовые значения с погрешностью (например, "8 ± 1,15")
- Единицы измерения в `target_units` могут быть в разных форматах (например, "µg mL-1", "µg/mL")

## 2. Co-crystals

**Описание**: Данные о фотостабильных лекарственных молекулах и их многокомпонентных кристаллических формах (сокристаллах или солях).

**Структура таблицы**:

```
"column_name"	"data_type"
"pdf"	"text"                  # Имя PDF-файла
"doi"	"text"                  # Цифровой идентификатор объекта
"supplementary"	"bigint"       # Указывает местоположение источника: 0 = основной; 1 = дополнительный
"authors"	"text"              # Список авторов
"title"	"text"                 # Название статьи
"journal"	"text"              # Название журнала
"year"	"bigint"               # Год публикации
"page"	"integer"              # Номер страницы или ID статьи, где были найдены данные
"access"	"bigint"            # 1 = открытый доступ, 0 = закрытый доступ
"name_cocrystal"	"text"      # Название многокомпонентного кристалла
"name_cocrystal_type_file"	"text" # Тип источника (текст, рукопись, рисунок и т.д.)
"name_cocrystal_page"	"text"  # Номер страницы, где упоминается сокристалл
"name_cocrystal_origin"	"bigint" # Происхождение в статье (рисунок 1, таблица 2 и т.д.)
"ratio_cocrystal"	"text"      # Соотношение компонентов, используемых для образования сокристалла
"ratio_cocrystal_page"	"text"  # Номер страницы для указанного соотношения
"ratio_cocrystal_page.1"	"text" # Дополнительный номер страницы для указанного соотношения
"ratio_cocrystal_origin"	"integer" # Источник соотношения (например, текст, таблица)
"name_drug"	"text"             # Название лекарства, указанное в статье
"name_drug_type_file"	"text"  # Тип источника для названия лекарства
"name_drug_origin"	"text"     # Происхождение названия лекарства в статье
"name_drug_page"	"bigint"    # Номер страницы, где упоминается название лекарства
"smiles_drug"	"text"          # Канонический SMILES лекарственной молекулы
"smiles_drug_type_file"	"text" # Тип источника для SMILES лекарства
"smiles_drug_origin"	"text"  # Происхождение SMILES лекарства в статье
"smiles_drug_page"	"integer"  # Номер страницы, где упоминается SMILES лекарства
"name_coformer"	"text"         # Название коформера
"name_coformer_type_file"	"text" # Тип источника для названия коформера
"name_coformer_origin"	"text" # Происхождение названия коформера в статье
"name_coformer_page"	"integer" # Номер страницы, где упоминается название коформера
"smiles_coformer"	"text"      # Канонический SMILES молекулы коформера
"smiles_coformer_type_file"	"text" # Тип источника для SMILES коформера
"smiles_coformer_origin"	"text" # Происхождение SMILES коформера в статье
"smiles_coformer_page"	"integer" # Номер страницы, где упоминается SMILES коформера
"photostability_change"	"text" # Изменение фотостабильности ('увеличение', 'не изменяется', 'уменьшение')
"photostability_change_type_file"	"text" # Тип источника для изменения фотостабильности
"photostability_change_origin"	"text" # Происхождение информации об изменении фотостабильности (например, рисунок, таблица, текст)
"photostability_change_page"	"integer" # Номер страницы, где сообщается о фотостабильности
"dbt_loaded_at"	"timestamp"    # Временная метка загрузки данных в dbt
"source_table"	"text"         # Имя исходной таблицы
```

**Пример данных**:

```
"Wang2024"	"10.1016/j.molstruc.2024.139270"	1	"Z. Wang, S. Li, Y. Tao, R. Zheng, S. Yang, D. Yang, S. Wang, L. Zhang, J. Xing, G. Du, Y. Lu"	"The novel cocrystals of acacetin with 4,4 ?-bipyridine and 2,2-bipyridine: Crystal structures analysis, stability and dissolution evaluation"	"Journal of Molecular Structure"	2024	139270	0	"ACT-4,4′-BPY"	"manuscript"	"text"	2	"2:1"	"manuscript"	"text"	2	"Acacetin"	"manuscript"	"text"	1	"COc1ccc(-c2cc(=O)c3c(O)cc(O)cc3o2)cc1"	"manuscript"	"figure 1"	2	"4,4'-Bipyridine"	"manuscript"	"text"	2	"c1cc(-c2ccncc2)ccn1"	"manuscript"	"figure 1"	2	"does not change"	"manuscript"	"text"	9
```

**Особенности и проблемы**:

- `ratio_cocrystal` содержит соотношения в формате "1:1", "2:1" и т.д.
- Столбец `ratio_cocrystal_page.1` имеет точку в имени, что требует использования кавычек
- Некоторые числовые поля были преобразованы из double precision в integer

## 3. Complexes

**Описание**: Данные о термодинамических константах стабильности (lgK) хелатных комплексов, образованных различными металлами и хелатирующими лигандами.

**Структура таблицы**:

```
"column_name"	"data_type"
"pdf"	"text"                  # Имя PDF-файла в архиве
"doi"	"text"                  # DOI статьи
"doi_sourse"	"text"           # Исходный DOI, если запись цитируется из обзорной статьи
"supplementary"	"bigint"       # Флаг источника: 0 – основная статья, 1 – дополнительные материалы
"title"	"text"                 # Название статьи
"publisher"	"text"            # Название издателя
"year"	"bigint"               # Год публикации
"access"	"bigint"            # 1 = открытый доступ, 0 = закрытый
"compound_id"	"text"          # Идентификатор соединения в статье
"compound_name"	"text"         # Название соединения, как указано
"smiles"	"text"               # Канонический SMILES лиганда или лигандного окружения
"smiles_type"	"text"           # 'ligand' = только один лиганд, 'environment' = полный комплекс без металла
"metal"	"text"                 # Тип металла, образующего комплекс
"target"	"text"               # Константа стабильности (logK)
"page_smiles"	"bigint"         # Номер страницы, где находится структура лиганда
"origin_smiles"	"text"         # Источник извлечения SMILES (например, рисунок 2, схема 1, таблица 3)
"page_metal"	"bigint"         # Номер страницы, где упоминается металл
"origin_metal"	"text"          # Источник информации о металле (например, заголовок, подпись к таблице, рисунок)
"page_target_value"	"integer"  # Страница, где находится целевое значение (logK)
"origin_target_value"	"text"  # Источник целевого значения
"dbt_loaded_at"	"timestamp"    # Временная метка загрузки данных в dbt
"source_table"	"text"         # Имя исходной таблицы
```

**Пример данных**:

```
"wadas2010"	"10.1021/cr900325h"	"10.1021/ic980227j"	0	"Coordinating Radiometals of Copper, Gallium, Indium, Yttrium, and Zirconium for PET and SPECT Imaging of Disease"	"American Chemical Society (ACS)"	2010	1	"L2"		"Sc1ccccc1CN(Cc1ccccc1S)Cc1ccccc1S"	"ligand"	"Ga"	"20,5"	6	"figure 2"	12	"table 5 (caption)"	12	"table 5"
```

**Особенности и проблемы**:

- `target` содержит числовые значения с запятой в качестве десятичного разделителя (например, "20,5")
- `page_target_value` был преобразован из double precision в integer

## 4. Cytotox

**Описание**: Данные о токсичности неорганических наночастиц на различных нормальных и раковых клеточных линиях.

**Структура таблицы**:

```
"column_name"	"data_type"
"sn"	"bigint"                # Индекс образца (серийный номер)
"material"	"text"              # Состав наночастицы/материала (например, SiO₂, Ag)
"shape"	"text"                 # Физическая форма частицы (например, Sphere, Rod)
"coat_functional_group"	"text" # Поверхностное покрытие или функционализация (например, CTAB, PEG)
"synthesis_method"	"text"     # Метод синтеза (например, Precipitation, Commercial)
"surface_charge"	"text"      # Сообщаемый поверхностный заряд (например, Negative, Positive)
"size_in_medium_nm"	"double precision" # Гидродинамический размер в биологической среде (нм)
"zeta_in_medium_mv"	"double precision" # Дзета-потенциал в среде (мВ; пусто, если не измерялся)
"no_of_cells_cells_well"	"double precision" # Плотность клеток на лунку в анализе
"human_animal"	"text"         # Происхождение клеток (A = животное, H = человек)
"cell_source"	"text"          # Вид/организм (например, крыса, человек)
"cell_tissue"	"text"          # Тканевое происхождение клеточной линии (например, надпочечник, легкое)
"cell_morphology"	"text"      # Форма клетки (например, неправильная, эпителиальная)
"cell_age"	"text"             # Стадия развития клеток (например, взрослый, эмбриональный)
"time_hr"	"bigint"            # Продолжительность воздействия в часах
"concentration"	"double precision" # Тестируемая концентрация материала (например, мкг/мл)
"test"	"text"                 # Тип анализа цитотоксичности (например, MTT, LDH)
"test_indicator"	"text"      # Измеряемый реагент (например, TetrazoliumSalt для MTT)
"viability_%"	"double precision" # Процент жизнеспособности клеток относительно контроля
"doi"	"text"                  # Цифровой идентификатор объекта (DOI) статьи
"article_list"	"bigint"       # Идентификатор статьи в наборе данных
"core_nm"	"double precision"  # Размер первичной частицы (нм)
"hydrodynamic_nm"	"double precision" # Размер в растворе, включая покрытия (нм)
"potential_mv"	"double precision" # Поверхностный заряд в растворе (мВ)
"cell_type"	"text"             # Конкретное название клеточной линии (например, PC12, A549)
"journal_name"	"text"         # Название журнала-издателя
"publisher"	"text"            # Издатель статьи
"year"	"bigint"               # Год публикации
"title"	"text"                # Название статьи, из которой были извлечены данные
"journal_is_oa"	"boolean"     # Является ли журнал открытым доступом (TRUE/FALSE)
"is_oa"	"boolean"             # Является ли конкретная статья открытым доступом (TRUE/FALSE)
"oa_status"	"text"            # Статус открытого доступа (например, hybrid, gold, closed)
"pdf"	"text"                 # Имя соответствующего PDF-файла в архиве PDF
"access"	"bigint"           # Статус доступа (1 = открытый, 0 = закрытый)
"dbt_loaded_at"	"timestamp"   # Временная метка загрузки данных в dbt
"source_table"	"text"        # Имя исходной таблицы
```

**Пример данных**:

```
1	"SiO2"	"Sphere"	"CTAB"	"Precipitation"	"Negative"	90.09		5000	"A"	"Rat"				12	3.9	"MTT"	"TetrazoliumSalt"	97.265	"10.3109/15376516.2015.1070229"	1		386.9	-35.9	"PC12"	"Toxicology Mechanisms and Methods"	"Taylor & Francis"	2015	"Effect of mesoporous silica nanoparticles on cell viability and markers of oxidative stress"	false	"FASLE"	"hybrid"	"1_10.3109@15376516.2015.1070229.pdf"	0
```

**Особенности и проблемы**:

- Столбец `viability_%` содержит специальный символ `%` в имени
- Столбец `is_oa` содержит опечатку "FASLE" вместо "FALSE"
- Многие числовые поля могут содержать NULL-значения

## 5. Eyedrops

**Описание**: Данные о проницаемости роговицы для малых молекул, используемых в офтальмологической доставке лекарств.

**Структура таблицы**:

```
"column_name"	"data_type"
"smiles"	"text"               # SMILES молекулы. Вручную нарисованы и отредактированы на основе названия соединения
"name"	"text"                 # Название фармацевтического соединения
"perm_(cm/s)"	"text"           # Проницаемость роговицы (прямая или рассчитанная)
"logp"	"text"                 # Логарифм проницаемости роговицы (прямой или рассчитанный)
"doi"	"text"                  # DOI статьи (если доступно)
"pmid"	"bigint"               # PubMed ID (если доступно)
"title"	"text"                 # Название исходной статьи
"publisher"	"text"            # Издатель статьи
"year"	"bigint"               # Год публикации
"access"	"bigint"            # Статус доступа: 1 = открытый, 0 = закрытый
"page"	"bigint"               # Номер страницы, где были найдены данные
"origin"	"text"              # Расположение источника в статье (например, table 3)
"dbt_loaded_at"	"timestamp"    # Временная метка загрузки данных в dbt
"source_table"	"text"         # Имя исходной таблицы
"perm_original"	"text"         # Исходное значение проницаемости роговицы в текстовом формате
"perm_numeric"	"float"        # Числовое представление проницаемости роговицы (см/с)
"logp_original"	"text"         # Исходное значение логарифма проницаемости роговицы в текстовом формате
"logp_numeric"	"float"        # Числовое представление логарифма проницаемости роговицы
```

**Пример данных**:

```
"CC(C)NCC(O)COc1ccc(CC(N)=O)cc1"	"Atenolol"		"-6,17"		14609285	"Prediction of corneal permeability using artificial neural networks"	"Govi-Verlag Pharmazeutischer Verlag GmbH"	2003	0	5	"table 3"
"CC(C)(C)NCC(O)COc1cccc2c1CC(O)C(O)C2"	"nadolol"	"0,0000014"		"10.1021/js9802594"		"Permeability of cornea, sclera, and conjunctiva: a literature analysis for drug delivery to the eye"	"American Geophysical Union (AGU)"	1998	0	3	"table 1"
```

**Особенности и проблемы**:

- Столбец `perm_(cm/s)` содержит скобки в имени
- Столбцы `perm_(cm/s)` и `logp` содержат числовые значения с запятой в качестве десятичного разделителя
- Добавлены дополнительные столбцы для числовых представлений значений проницаемости и логарифма проницаемости

## 6. Nanomag

**Описание**: Комплексные данные о магнитных наночастицах, включая структуры ядро-оболочка и их магнитные свойства.

**Структура таблицы**:

```
"column_name"	"data_type"
"name"	"text"                 # Название образца или материала (если указано)
"np_shell_2"	"text"           # Материал второго слоя оболочки (опционально)
"np_hydro_size"	"double precision" # Гидродинамический размер (нм)
"xrd_scherrer_size"	"double precision" # Размер по XRD с использованием уравнения Шеррера
"zfc_h_meas"	"double precision" # Поле, используемое для измерения ZFC (кЭ)
"htherm_sar"	"double precision" # Удельная скорость поглощения для гипертермии (Вт/г)
"mri_r1"	"double precision"   # Скорость релаксации МРТ r1 (мМ⁻¹·с⁻¹)
"mri_r2"	"double precision"   # Скорость релаксации МРТ r2 (мМ⁻¹·с⁻¹)
"emic_size"	"double precision"  # Размер по электронной микроскопии
"instrument"	"text"           # Инструмент, используемый для измерения (например, SQUID)
"core_shell_formula"	"text"  # Комбинированная формула для системы ядро-оболочка
"np_core"	"text"              # Материал ядра наночастицы
"np_shell"	"text"             # Материал оболочки
"space_group_core"	"text"    # Кристаллографическая пространственная группа ядра
"space_group_shell"	"text"   # Кристаллографическая пространственная группа оболочки
"squid_h_max"	"double precision" # Максимальное магнитное поле в SQUID (кЭ)
"fc_field_t"	"text"           # Напряженность поля охлаждения в поле (Т)
"squid_temperature"	"text"    # Температура измерения SQUID (К)
"squid_sat_mag"	"text"         # Намагниченность насыщения (эму/г)
"coercivity"	"text"           # Коэрцитивная сила (кЭ)
"squid_rem_mag"	"text"         # Остаточная намагниченность (эму/г)
"exchange_bias_shift_oe"	"text" # Сдвиг поля обменного смещения (Э)
"vertical_loop_shift_m_vsl_emu_g"	"text" # Вертикальный сдвиг петли (эму/г)
"hc_koe"	"double precision"  # Коэрцитивное поле из петли гистерезиса (кЭ)
"doi"	"text"                  # DOI публикации
"pdf"	"text"                  # Имя связанного PDF
"supp"	"text"                 # Индикатор дополнительной информации
"journal"	"text"              # Название журнала
"publisher"	"text"            # Название издателя
"year"	"bigint"               # Год публикации
"title"	"text"                 # Название статьи
"access"	"integer"           # Статус доступа: 1 = OA, 0 = закрытый
"verification_required"	"double precision" # Требуется ли ручная проверка
"verified_by"	"double precision" # Имя валидатора
"verification_date"	"double precision" # Дата проверки
"has_mistake_in_matadata"	"double precision" # Есть ли ошибка в метаданных
"comment"	"double precision"  # Примечания валидатора
"article_name_folder"	"text"  # Внутреннее имя папки для статьи
"supp_info_name_folder"	"text" # Имя папки с дополнительной информацией
"dbt_loaded_at"	"timestamp"    # Временная метка загрузки данных в dbt
"source_table"	"text"         # Имя исходной таблицы
```

**Пример данных**:

```
"CFO"	"hexamethyldisilazane"	50	8.35	20	250	10.67	140.87	12.5	"VSM"	"DyCrTiO5"	"Fe1"	"citric acid"	"pbam"	"0"	20	"6.0"	"300.0"	"211.0"	"190.0"	"50.0"	"-1290.0"	"0.0"	60	"10.1002/anie.201609477"	"anie.201609477.pdf"	"anie.201609477_SI.pdf"	"Angewandte Chemie International Edition"	"Wiley"	2016	"Magnetically Induced Continuous CO<sub>2</sub> Hydrogenation Using Composite Iron Carbide Nanoparticles of Exceptionally High Heating Power"	0						"dataset1_1"	"dataset1_1_supp"
```

**Особенности и проблемы**:

- Многие числовые значения хранятся как текст (`squid_temperature`, `squid_sat_mag`, `coercivity` и т.д.)
- Некоторые текстовые поля могут содержать HTML-теги (например, `<sub>` и `<i>` в заголовке)
- Столбцы `verification_required`, `verified_by`, `verification_date`, `has_mistake_in_matadata`, `comment` имеют тип `double precision`, хотя должны быть другого типа
## 7. Nanozymes

**Описание**: Данные о нанозимах, включая их химический состав, структурные свойства и каталитическую активность.

**Структура таблицы**:

```
"column_name"	"data_type"
"formula"	"text"              # Химическая формула нанозима (например, Fe₃O₄, CeO₂)
"activity"	"text"             # Тип каталитической активности (например, пероксидаза, оксидаза)
"syngony"	"text"              # Кристаллическая система (например, кубическая, гексагональная)
"length"	"text"               # Наночастица длина в нм (может быть диапазон, например "5-10")
"width"	"text"                # Ширина в нм (может быть диапазон)
"depth"	"text"                # Глубина или толщина в нм (может быть диапазон)
"surface"	"text"              # Функционализация или модификация поверхности (например, PEG, PVP)
"km_value"	"double precision" # Константа Михаэлиса (Km)
"km_unit"	"text"              # Единица для значения Km (например, мМ, мкМ)
"vmax_value"	"double precision" # Максимальная скорость реакции (Vmax)
"vmax_unit"	"text"             # Единица для значения Vmax (например, мкмоль/мин, мМ/с)
"target_source"	"text"         # Источник значения целевой активности в публикации
"reaction_type"	"text"         # Субстрат и ко-субстрат, используемые в реакции (например, TMB + H₂O₂)
"c_min"	"double precision"     # Минимальная концентрация субстрата (в мМ)
"c_max"	"double precision"     # Максимальная концентрация субстрата (в мМ)
"c_const"	"double precision"   # Постоянная концентрация ко-субстрата
"c_const_unit"	"text"          # Единица концентрации ко-субстрата (например, мМ, мкМ)
"ccat_value"	"double precision" # Концентрация катализатора (нанозима)
"ccat_unit"	"text"             # Единица концентрации катализатора (например, мг/мл)
"ph"	"double precision"       # pH, при котором проводилась реакция
"temperature"	"double precision" # Температура в градусах Цельсия
"doi"	"text"                  # DOI исходной статьи
"pdf"	"text"                  # Имя PDF-файла в наборе данных
"access"	"bigint"            # Статус доступа (1 = открытый доступ, 0 = закрытый доступ)
"title"	"text"                 # Название исходной публикации
"journal"	"text"              # Название журнала
"year"	"bigint"               # Год публикации
"dbt_loaded_at"	"timestamp"    # Временная метка загрузки данных в dbt
"source_table"	"text"         # Имя исходной таблицы
```

**Пример данных**:

```
"MnO2"	"oxidase"						0.027	"mM"	0.113		"text"	"TMB"	0.005	1			0.01	"mcg/ml"	3.8	25	"10.1080/03067319.2019.1599875"	"03067319.2019.1599875"	0	"MnO <sub>2</sub> nanozyme induced the chromogenic reactions of ABTS and TMB to visual detection of Fe <sup>2+</sup> and Pb <sup>2+</sup> ions in water"	"International Journal of Environmental Analytical Chemistry"	2019
"MnFe2O4"	"peroxidase"	"cubic"	"2.7"	"2.7"	"2.7"	"dopamine"	0.34	"10−3 mM"	11.92	"10−8 M·s−1"	"text"	"H2O2 + TMB"	0	0.0001	3.65	"mM"	20	"μg mL-1"	4.6	25	"10.1088/1361-6463/aa5bf6"	"AA5BF6"	0	"Facile method to synthesize dopamine-capped mixed ferrite nanoparticles and their peroxidase-like activity"	"Journal of Physics D: Applied Physics"	2017
"Cu0.89Zn0.11O"	"peroxidase"		"5-10"	"5-10"	"5-10"		0.01	"M"	2.877	"10-8 Ms-1"	"text"	"TMB + H2O2"	0	10	0.2	"M"	100	"μg mL−1"	4.65	25	"10.1021/acsami.6b05354"	"ACSAMI.6B05354"	0	"Cu<sub>0.89</sub>Zn<sub>0.11</sub>O, A New Peroxidase-Mimicking Nanozyme with High Sensitivity for Glucose and Antioxidant Detection"	"ACS Applied Materials &amp; Interfaces"	2016
```

**Особенности и проблемы**:

- Столбцы `length`, `width`, `depth` содержат текстовые значения, которые могут быть как одиночными числами, так и диапазонами (например, "5-10")
- Единицы измерения (`km_unit`, `vmax_unit`, `c_const_unit`, `ccat_unit`) могут быть в разных форматах
- Заголовки могут содержать HTML-теги (`<sub>`, `<sup>`)
- Некоторые поля могут быть пустыми (NULL)

## 8. Oxazolidinones

**Описание**: Данные об оксазолидиноновых антибиотиках и их ингибирующих концентрациях против различных бактериальных штаммов.

**Структура таблицы**:

```
"column_name"	"data_type"
"pdf"	"text"                  # Название PDF-файла, из которого были извлечены данные
"smiles"	"text"               # Изомерное представление SMILES соединения
"doi"	"text"                  # Цифровой идентификатор объекта исходной статьи
"title"	"text"                 # Название статьи
"publisher"	"text"            # Издатель журнала
"year"	"bigint"               # Год публикации
"access"	"bigint"            # Статус доступа (1 = открытый, 0 = закрытый)
"compound_id"	"text"          # ID соединения, используемый в статье
"target_type"	"text"          # Тип измерения (например, MIC, pMIC)
"target_relation"	"text"      # Символ неравенства (например, =, >, <)
"target_value"	"text"         # Числовое целевое значение
"target_units"	"text"         # Единицы измерения целевого значения (например, мкг/мл, моль/л)
"bacteria"	"text"             # Исходное название бактерии
"bacteria_name_unified"	"text" # Нормализованное / унифицированное название бактерии
"bacteria_info"	"text"         # Дополнительная информация о штамме/бактерии
"page_bacteria"	"bigint"       # Номер страницы, где указана бактерия
"origin_bacteria"	"text"     # Источник бактерии (текст, таблица, рис., изображение)
"section_bacteria"	"text"     # Раздел статьи (если применимо)
"subsection_bacteria"	"text"  # Подраздел статьи (если применимо)
"page_target"	"bigint"        # Страница статьи, где находится целевое значение
"origin_target"	"text"        # Источник целевого значения (таблица, текст, изображение)
"section_target"	"text"      # Раздел в статье для цели (если текст)
"subsection_target"	"double precision" # Подраздел в статье для цели
"column_prop"	"text"           # Индекс столбца таблицы или значение (опционально)
"line_prop"	"double precision" # Индекс строки таблицы или значение (опционально)
"page_scaffold"	"bigint"       # Страница с каркасом или полной молекулой
"origin_scaffold"	"text"     # Происхождение каркаса (таблица, рисунок, изображение)
"section_scaffold"	"text"     # Раздел для каркаса
"subsection_scaffold"	"double precision" # Подраздел для каркаса
"page_residue"	"double precision" # Страница для структур заместителей
"origin_residue"	"text"     # Происхождение структур остатка
"section_residue"	"text"     # Раздел статьи для структур заместителей
"dbt_loaded_at"	"timestamp"    # Временная метка загрузки данных в dbt
"source_table"	"text"         # Имя исходной таблицы
```

**Пример данных**:

```
"187152108785908820"	"CC(=O)NC[C@H]1CN(c2ccc3c(c2)CCC/C(=C\c2ccsc2)C3=O)C(=O)O1"	"10.2174/187152108785908820"	"Oxazolidinone Antibacterials and Our Experience"	"Bentham Science Publishers Ltd."	2008	0	"26b"	"MIC"	"'='"	"2025-02-01 00:00:00"	"μg/mL"	"Haemophilus influenza"	"Haemophilus influenzae"		4	"text"	"MODIFICATIONS IN THE REGION B AND AT THE C5"		4	"text"	"MODIFICATIONS IN THE REGION B AND AT THE C5"		"right"	15	4	"image"	"MODIFICATIONS IN THE REGION B AND AT THE C5"
```

**Особенности и проблемы**:

- `target_value` содержит значение в формате даты "2025-02-01 00:00:00", что необычно для MIC
- Столбцы `subsection_target`, `line_prop`, `subsection_scaffold`, `page_residue` имеют тип `double precision`, хотя, судя по их назначению, должны быть другого типа
- Некоторые поля могут быть пустыми (NULL)

## 9. Seltox

**Описание**: Данные о токсичности неорганических наночастиц, протестированных против различных бактериальных штаммов, включая как нормальные, так и мультирезистентные (MDR) типы.

**Структура таблицы**:
```
"column_name"	"data_type"
"sn"	"bigint"                # Внутренний индекс для записей
"np"	"text"                  # Название наночастицы (например, Ag, Au, ZnO)
"coating"	"bigint"            # Поверхностное покрытие или модификация (1 = наличие, 0 = отсутствие)
"bacteria"	"text"             # Название тестируемого бактериального штамма
"mdr"	"bigint"                # Индикатор мультирезистентного штамма (1 = MDR, 0 = не-MDR)
"strain"	"text"              # Конкретный идентификатор штамма (если предоставлен)
"np_synthesis"	"text"          # Метод синтеза наночастицы
"method"	"text"              # Тип используемого анализа (например, MIC, ZOI)
"mic_np_µg_ml"	"text"          # Значение MIC в мкг/мл для наночастицы
"concentration"	"double precision" # Концентрация, используемая для измерения ZOI
"zoi_np_mm"	"double precision"  # ZOI (Зона ингибирования) в миллиметрах
"np_size_min_nm"	"double precision" # Минимальный размер наночастицы (нм)
"np_size_max_nm"	"double precision" # Максимальный размер наночастицы (нм)
"np_size_avg_nm"	"double precision" # Средний размер наночастицы (нм)
"shape"	"text"                 # Морфология наночастицы (например, сферическая, стержень)
"time_set_hours"	"double precision" # Продолжительность антибактериального анализа в часах
"zeta_potential_mv"	"double precision" # Дзета-потенциал (в мВ); пусто, если не указано
"solvent_for_extract"	"text"  # Растворитель, используемый в зеленом синтезе (если применимо)
"temperature_for_extract_c"	"double precision" # Температура для приготовления экстракта (°C)
"duration_preparing_extract_min"	"double precision" # Продолжительность (в минутах) приготовления экстракта
"precursor_of_np"	"text"     # Химический предшественник, используемый для синтеза наночастиц (например, AgNO₃)
"concentration_of_precursor_mm"	"double precision" # Концентрация предшественника в миллимолярах
"hydrodynamic_diameter_nm"	"double precision" # Гидродинамический диаметр в нанометрах
"ph_during_synthesis"	"double precision" # pH во время синтеза наночастиц
"reference"	"text"             # Внешний источник или URL цитирования
"doi"	"text"                  # DOI публикации
"article_list"	"bigint"       # ID статьи в наборе данных
"journal_name"	"text"          # Название журнала
"publisher"	"text"            # Название издателя
"year"	"bigint"               # Год публикации
"title"	"text"                 # Название статьи
"journal_is_oa"	"boolean"      # Является ли журнал Open Access (True / False)
"is_oa"	"boolean"              # Является ли сама статья Open Access (True / False)
"oa_status"	"text"             # Статус Open Access (например, green, gold, hybrid, closed)
"pdf"	"text"                  # Имя файла PDF в архиве для проверки ссылок
"access"	"bigint"            # Статус доступа (1 = открытый доступ, 0 = закрытый доступ)
"dbt_loaded_at"	"timestamp"    # Временная метка загрузки данных в dbt
"source_table"	"text"         # Имя исходной таблицы
```

**Пример данных**:
```
4 "Ag" 0 "Salmonella typhi" 0 "green_synthesis" "MIC" "32" 10 40 20 "spherical" 24 "water (deionized)" 21 "AgNO3" 1 "http://www.nanomedicine-rj.com/article_38645.html" "10.22034/nmrj.2020.01.006" 38 "Nanomedicine Research Journal" "Tehran University of Medical Sciences" 2020 "Synergistic Activity of Green Silver Nanoparticles with Antibiotics" true true "green" "38_NMRJ_Volume 5_Issue 1_Pages 44-54.pdf" 1
```

**Особенности и проблемы**:
- Столбец `mic_np_µg_ml` содержит специальный символ `µ` в имени и хранится как текст, хотя содержит числовые значения
- Некоторые числовые поля могут содержать NULL-значения
- Столбец `reference` может содержать URL-адреса

## 10. Synergy

**Описание**: Данные об антибактериальной активности отдельных лекарств, наночастиц и их комбинаций против различных бактериальных штаммов.

**Структура таблицы**:
```
"column_name"	"data_type"
"sn"	"bigint"                # Внутренний индекс записи
"np"	"text"                  # Название наночастицы (Ag, Au, CuO и т.д.)
"bacteria"	"text"             # Вид бактерии
"strain"	"text"              # Идентификатор штамма (например, ATCC 25922, MTCC 443)
"np_synthesis"	"text"          # Метод синтеза наночастиц (например, зеленый, химический)
"drug"	"text"                 # Название тестируемого антибиотика
"drug_dose_µg_disk"	"double precision" # Доза препарата в анализе диффузии диска
"np_concentration_µg_ml"	"double precision" # Концентрация наночастиц в мкг/мл
"np_size_min_nm"	"text"       # Минимальный размер наночастиц в нанометрах
"np_size_max_nm"	"double precision" # Максимальный размер наночастиц в нанометрах
"np_size_avg_nm"	"double precision" # Средний размер наночастиц в нанометрах
"shape"	"text"                 # Морфология наночастиц
"method"	"text"              # Используемый метод анализа
"zoi_drug_mm_or_mic__µg_ml"	"double precision" # Зона ингибирования или MIC для препарата отдельно
"error_zoi_drug_mm_or_mic_µg_ml"	"double precision" # Стандартное отклонение для ZOI_drug_mm_or_MIC
"zoi_np_mm_or_mic_np_µg_ml"	"double precision" # ZOI или MIC для наночастиц отдельно
"error_zoi_np_mm_or_mic_np_µg_ml"	"double precision" # Стандартное отклонение для анализа наночастиц
"zoi_drug_np_mm_or_mic_drug_np_µg_ml"	"double precision" # ZOI или MIC для препарата + наночастиц
"error_zoi_drug_np_mm_or_mic_drug_np_µg_ml"	"double precision" # Стандартное отклонение для комбинации
"fold_increase_in_antibacterial_activity"	"double precision" # Кратное увеличение эффективности комбинации
"zeta_potential_mv"	"text"      # Поверхностный заряд в мВ
"mdr"	"text"                  # Указывает на множественную лекарственную устойчивость (R = устойчивый)
"fic"	"double precision"      # Индекс фракционной ингибирующей концентрации
"effect"	"text"              # Тип синергии (синергетический, аддитивный...)
"reference"	"text"             # Полная цитата или источник
"doi"	"text"                  # Цифровой идентификатор объекта
"article_list"	"bigint"       # Внутренний идентификатор статьи
"time_hr"	"double precision"  # Время воздействия в часах
"coating_with_antimicrobial_peptide_polymers"	"text" # Наличие или название покрытия
"combined_mic"	"double precision" # Значение MIC полной системы (наночастицы + покрытие)
"peptide_mic"	"double precision" # Значение MIC пептида отдельно
"viability_%"	"double precision" # Жизнеспособность бактериального образца
"viability_error"	"double precision" # Связанная ошибка для жизнеспособности
"journal_name"	"text"          # Название журнала-источника
"publisher"	"text"            # Издатель
"year"	"bigint"               # Год публикации
"title"	"text"                 # Название статьи
"journal_is_oa"	"boolean"      # Является ли журнал Open Access
"is_oa"	"boolean"              # Является ли статья OA
"oa_status"	"text"             # Уровень OA (green, hybrid)
"pdf"	"text"                  # Имя файла PDF в архиве
"access"	"bigint"            # 1 = OA, 0 = закрытый
"dbt_loaded_at"	"timestamp"    # Временная метка загрузки данных в dbt
"source_table"	"text"         # Имя исходной таблицы
```

**Пример данных**:
```
706	"Ag"	"Escherichia coli"		"green_synthesis by Klebsiella pneumoniae"	"Co-trimoxazole"	2	10	"5"	32	22.5	"spherical"	"disc_diffusion"	37				37		0					"https://www.sciencedirect.com/science/article/abs/pii/S1549963407000469"	"10.1016/j.nano.2007.02.001"	26							"Nanomedicine: Nanotechnology, Biology and Medicine"	"Elsevier BV"	2007	"Synthesis and effect of silver nanoparticles on the antibacterial activity of different antibiotics against Staphylococcus aureus and Escherichia coli"	false	false	"closed"	"26_shahverdi2007.pdf"	0
```

**Особенности и проблемы**:
- Столбец `np_size_min_nm` имеет тип `text`, хотя содержит числовые значения
- Столбец `zeta_potential_mv` имеет тип `text`, хотя должен содержать числовые значения
- Столбец `zoi_drug_mm_or_mic__µg_ml` содержит двойное подчеркивание и специальный символ `µ`
- Столбец `viability_%` содержит специальный символ `%`
- Многие поля могут содержать NULL-значения
- Столбец `reference` может содержать URL-адреса
