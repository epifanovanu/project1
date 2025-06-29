import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine

# Конфигурация
DB_USER = 'test'
DB_PASSWORD = '1'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'project1'
SCHEMA = 'ds'
CSV_DIR_PATH = './csv_files'

# Подключение
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
conn.autocommit = True
cursor = conn.cursor()

def get_unique_columns(table_name):
    query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisunique = true;
    """
    cursor.execute(query, (f"{SCHEMA}.{table_name}",))
    return [row[0] for row in cursor.fetchall()]

def get_varchar_columns_length(table_name):
    query = """
        SELECT column_name, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND data_type = 'character varying'
    """
    cursor.execute(query, (SCHEMA, table_name))
    return {row[0]: row[1] for row in cursor.fetchall()}

def import_csv_to_db(csv_path):
    table_name = os.path.splitext(os.path.basename(csv_path))[0].lower()

    try:
        header = pd.read_csv(csv_path, sep=';', nrows=0, encoding='utf-8').columns.str.lower()
    except UnicodeDecodeError:
        print(f"❌ Ошибка чтения заголовков '{csv_path}' с utf-8. Пробуем cp1251.")
        header = pd.read_csv(csv_path, sep=';', nrows=0, encoding='cp1251').columns.str.lower()

    date_cols = [col.upper() for col in header if 'date' in col]

    if not date_cols:
        date_cols = None

    try:
        df = pd.read_csv(csv_path, sep=';', decimal='.', parse_dates=date_cols if date_cols else [],
                         dayfirst=True, encoding='utf-8')
    except UnicodeDecodeError:
        print(f"❌ Ошибка чтения '{csv_path}' с utf-8. Пробуем ISO-8859-1.")
        df = pd.read_csv(csv_path, sep=';', decimal='.', parse_dates=date_cols if date_cols else [],
                         dayfirst=False, encoding='ISO-8859-1')

    df.columns = df.columns.str.lower()

    if date_cols:
        for col in date_cols:
            col_lower = col.lower()
            df[col_lower] = pd.to_datetime(df[col_lower], errors='coerce').dt.date

    # Получаем ключи и varchar длины
    unique_keys = get_unique_columns(table_name)
    if not unique_keys:
        print(f"⚠️ Таблица '{SCHEMA}.{table_name}' не имеет уникальных ключей, пропускаем.")
        return

    varchar_lengths = get_varchar_columns_length(table_name)

    # Обрезаем varchar столбцы
    for col, max_len in varchar_lengths.items():
        if col in df.columns:
            df[col] = df[col].astype(str).str.slice(0, max_len)

    columns = list(df.columns)
    col_names_sql = sql.SQL(', ').join(map(sql.Identifier, columns))
    placeholders_sql = sql.SQL(', ').join(sql.Placeholder() * len(columns))
    conflict_cols_sql = sql.SQL(', ').join(map(sql.Identifier, unique_keys))

    update_assignments = sql.SQL(', ').join(
        sql.SQL(f"{col} = EXCLUDED.{col}") for col in columns if col not in unique_keys
    )

    insert_sql = sql.SQL("""
        INSERT INTO {schema}.{table} ({fields})
        VALUES ({values})
        ON CONFLICT ({conflict_cols}) DO UPDATE SET
        {update_assignments}
    """).format(
        schema=sql.Identifier(SCHEMA),
        table=sql.Identifier(table_name),
        fields=col_names_sql,
        values=placeholders_sql,
        conflict_cols=conflict_cols_sql,
        update_assignments=update_assignments
    )

    data_tuples = [tuple(x) for x in df.to_numpy()]

    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=1000)
        print(f"✅ UPSERT успешно завершён: '{csv_path}' → {SCHEMA}.{table_name} (ключи: {unique_keys})")
    except Exception as e:
        print(f"❌ Ошибка при UPSERT в '{table_name}': {e}")

# Основной цикл
for filename in os.listdir(CSV_DIR_PATH):
    if filename.lower().endswith('.csv'):
        full_path = os.path.join(CSV_DIR_PATH, filename)
        try:
            import_csv_to_db(full_path)
        except Exception as e:
            print(f"❌ Ошибка при импорте '{filename}': {e}")
