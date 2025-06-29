import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine

# Конфигурация
DB_USER = 'etl_user'
DB_PASSWORD = 'etl_pass'
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

LOGGER_USER = 'logger_user'
LOGGER_PASSWORD = 'logger_pass'

# Подключение к БД логгирования
loggerconn = psycopg2.connect(dbname=DB_NAME, user=LOGGER_USER, password=LOGGER_PASSWORD, host=DB_HOST, port=DB_PORT)
loggerconn.autocommit = True
loggercursor = loggerconn.cursor()

def log_etl(severity, message):
    insert_log = """
        INSERT INTO logs.etl_log (log_time, severity, message)
        VALUES (now(), %s, %s)
    """
    try:
        loggercursor.execute(insert_log, (severity, message))
    except Exception as e:
        print(f"❌ Ошибка при записи лога (logger_user): {e}")



def get_unique_columns(table_name):
    query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisunique = true;
    """
    cursor.execute(query, (f"{SCHEMA}.{table_name}",))
    return [row[0] for row in cursor.fetchall()]

def import_csv_to_db(csv_path):
    log_etl('INFO', f"Начата обработка файла: {csv_path}...")
    print(f"ℹ️  Начата обработка файла: {csv_path}...")

    table_name = os.path.splitext(os.path.basename(csv_path))[0].lower()
    log_etl('INFO', f"Определяем таблицу для экспорта как {SCHEMA}.{table_name}")
    print(f"ℹ️  Определяем таблицу для экспорта как {SCHEMA}.{table_name}")
     
    try:
        header = pd.read_csv(csv_path, sep=';', nrows=0, encoding='utf-8').columns.str.lower()
    except UnicodeDecodeError:
        log_etl('WARNING', f"Ошибка кодировки, испольуем ISO-8859-1")
        print(f"⚠️  Ошибка кодировки, испольуем ISO-8859-1")
        header = pd.read_csv(csv_path, sep=';', nrows=0, encoding='ISO-8859-1').columns.str.lower()

    log_etl('INFO', f"Заголовки из файла {csv_path}: {header.to_list()}")
    print(f"ℹ️  Заголовки из файла {csv_path}: {header.to_list()}")

    dtypes = {}
    
    for col in ['currency_code','code_iso_num','account_number']:
        if col in header:
            print(f"ℹ️  Переопределяем тип данных для столбца {col} = str")
            dtypes[col.upper()] = str

    date_cols = [col.upper() for col in header if 'date' in col]
    
    if not date_cols:
        date_cols = None
        log_etl('INFO', f"Столбцы с датой не найдены")
        print(f"ℹ️  Столбцы с датой не найдены")
    else:
        log_etl('INFO', f"Столбцы с датой: {date_cols}")
        print(f"ℹ️  Столбцы с датой: {date_cols}")
 
    try:
        df = pd.read_csv(csv_path, sep=';', decimal='.', parse_dates=date_cols if date_cols else [],
                         dayfirst=True, encoding='utf-8',dtype=dtypes)
    except UnicodeDecodeError:
        log_etl('WARNING',f"Ошибка кодировки, испольуем ISO-8859-1")
        print(f"⚠️  Ошибка кодировки, испольуем ISO-8859-1")
        df = pd.read_csv(csv_path, sep=';', decimal='.', parse_dates=date_cols if date_cols else [],
                         dayfirst=False, encoding='ISO-8859-1',dtype=dtypes)
    df.columns = df.columns.str.lower()

    records_count = len(df)
    log_etl('INFO', f"Количество записей в файле {csv_path}: {records_count}")
    print(f"ℹ️  Количество записей в файле {csv_path}: {records_count}")
  
    if date_cols:
        for col in date_cols:
            col_lower = col.lower()
            df[col_lower] = pd.to_datetime(df[col_lower], errors='coerce').dt.date

    # Получаем ключи и varchar длины
    unique_keys = get_unique_columns(table_name)
    if not unique_keys:
        log_etl('WARNING',f"Таблица '{SCHEMA}.{table_name}' не имеет уникальных ключей, пропускаем.")
        print(f"⚠️ Таблица '{SCHEMA}.{table_name}' не имеет уникальных ключей, пропускаем.")
        return
    else:
        log_etl('INFO',f"Таблица '{SCHEMA}.{table_name}' имеет следующие уникальные ключи: {unique_keys}")
        print(f"ℹ️  Таблица '{SCHEMA}.{table_name}' имеет следующие уникальные ключи: {unique_keys}")

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
        execute_batch(cursor, insert_sql, data_tuples, page_size=100000)
        log_etl('INFO',f"Загрузка файла {csv_path} завершена")        
        print(f"✅ Загрузка файла {csv_path} завершена")
    except Exception as e:
        log_etl('ERROR',f"Ошибка при загрузке файла'{csv_path}': {e}")        
        print(f"❌ Ошибка при загрузке файла'{csv_path}': {e}")

# Основной цикл
for filename in os.listdir(CSV_DIR_PATH):
    if filename.lower().endswith('.csv'):
        full_path = os.path.join(CSV_DIR_PATH, filename)
        try:
            import_csv_to_db(full_path)
        except Exception as e:
            log_etl('ERROR', f"Ошибка при импорте '{filename}': {e}")
            print(f"❌ Ошибка при импорте '{filename}': {e}")
            