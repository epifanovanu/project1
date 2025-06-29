import pandas as pd
from  sqlalchemy import create_engine

# --- Конфигурация подключения к PostgreSQL ---
DB_USER = 'test'
DB_PASSWORD = '1'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'project1'
TABLE_NAME = 'ft_balance_f'

# --- Путь к CSV-файлу ---
CSV_FILE_PATH = 'ft_balance_f.csv'

# --- Чтение CSV-файла ---
df = pd.read_csv(CSV_FILE_PATH, sep=';', decimal='.', parse_dates=['ON_DATE'],dayfirst=True)

# Приводим названия колонок к нижнему регистру
df.columns = df.columns.str.lower()

print(df.to_string(index=False))

# --- Подключение к базе данных ---
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# --- Экспорт данных в PostgreSQL ---
# Если таблица не существует — создастся автоматически
df.to_sql(TABLE_NAME, engine, schema='ds', if_exists='replace', index=False)



print(f"✅ Данные успешно импортированы в таблицу '{TABLE_NAME}'")
