import os
import pandas as pd
from sqlalchemy import create_engine

# --- Конфигурация подключения к PostgreSQL ---
DB_USER = 'test'
DB_PASSWORD = '1'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'project1'
SCHEMA = 'ds'

# --- Путь к директории с CSV-файлами ---
CSV_DIR_PATH = './csv_files' 

# --- Подключение к базе данных ---
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

def import_csv_to_db(csv_path):
    # Имя таблицы — имя файла без расширения
    table_name = os.path.splitext(os.path.basename(csv_path))[0].lower()

    try:
        # Читаем заголовки с указанием кодировки
        header = pd.read_csv(csv_path, sep=';', nrows=0, encoding='utf-8').columns.str.lower()
    except UnicodeDecodeError:
        print(f"❌ Ошибка чтения заголовков в файле '{csv_path}' с кодировкой utf-8. Пробуем cp1251.")
        header = pd.read_csv(csv_path, sep=';', nrows=0, encoding='cp1251').columns.str.lower()

    #
    print(f"ℹ️ В файле '{csv_path}' найдены следущие столбцы: {header.to_list()}")

    # Ищем потенциальные дата-колонки
    date_cols = [col.upper() for col in header if 'date' in col]

    if not date_cols:
        print(f"⚠️ В файле '{csv_path}' не найдено столбцов с датой.")
        date_cols = None
    else:
        print(f"ℹ️ В файле '{csv_path}' найден(ы) столбец(ы) с датой: {date_cols}")

    # Попытка прочитать сам CSV-файл
    try:
        df = pd.read_csv(
            csv_path,
            sep=';',
            decimal='.',
            parse_dates = date_cols if date_cols else [],
            dayfirst=True,
            encoding='utf-8'  # сначала пробуем UTF-8
        )
    except UnicodeDecodeError:
        print(f"❌ Ошибка кодировки при чтении '{csv_path}' с utf-8. Пробуем cp1251.")
        df = pd.read_csv(
            csv_path,
            sep=';',
            decimal='.',
            parse_dates = date_cols if date_cols else [],
            dayfirst=True,
            encoding='cp1251'
        )
    df.columns = df.columns.str.lower()

    
    # Экспорт в PostgreSQL
    df.to_sql(table_name, engine, schema=SCHEMA, if_exists='replace', index=False)
    print(f"✅ Данные из '{csv_path}' успешно импортированы в таблицу '{SCHEMA}.{table_name}'")



# --- Основной цикл по файлам в директории ---
for filename in os.listdir(CSV_DIR_PATH):
    if filename.lower().endswith('.csv'):
        full_path = os.path.join(CSV_DIR_PATH, filename)
        try:
            import_csv_to_db(full_path)
            
        except Exception as e:
            print(f"❌ Ошибка при импорте файла '{filename}': {e}")
      
