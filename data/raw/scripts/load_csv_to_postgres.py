import os
import pandas as pd
import sqlalchemy
from sqlalchemy import text

# Параметры подключения к PostgreSQL
DB_USER = "postgres"
DB_PASSWORD = "1321122Ar"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ChemX"

# Создаем подключение к БД
connection_string = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
engine = sqlalchemy.create_engine(connection_string)

# Создаем схему raw, если она не существует
with engine.connect() as connection:
    connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
    connection.commit()

# Прямой путь к папке с CSV файлами
csv_folder = "C:/Users/s-ars/Desktop/ChemX_dbt/data/raw"

# Получаем список всех CSV файлов в папке
csv_files = [f for f in os.listdir(csv_folder) if f.endswith(".csv")]

print(f"Найдено {len(csv_files)} CSV файлов для загрузки")

# Загружаем каждый CSV в отдельную таблицу в схему "raw"
for file in csv_files:
    # Получаем имя таблицы из имени файла (без расширения)
    table_name = os.path.splitext(file)[0].lower()

    # Заменяем проблемные символы в имени таблицы
    table_name = table_name.replace("-", "_").replace(" ", "_")

    # Полный путь к файлу
    file_path = os.path.join(csv_folder, file)

    print(f"Загрузка {file} в таблицу raw.{table_name}...")

    try:
        # Читаем CSV файл
        df = pd.read_csv(file_path, sep=",", encoding="utf-8")

        # Приводим имена колонок к нижнему регистру и заменяем пробелы на подчеркивания
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]

        # Загружаем данные в PostgreSQL
        df.to_sql(
            table_name,
            engine,
            schema="raw",
            if_exists="replace",  # 'replace' перезапишет таблицу, 'append' добавит данные
            index=False,
        )

        print(f"Успешно загружено {len(df)} строк в raw.{table_name}")

    except Exception as e:
        print(f"Ошибка при загрузке {file}: {str(e)}")

print("Загрузка завершена!")
