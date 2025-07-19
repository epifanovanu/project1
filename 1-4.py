import psycopg2
import csv
import sys
import os
from datetime import datetime

DB_USER = 'etl_user'
DB_PASSWORD = 'etl_pass'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'project1'
SCHEMA = 'dm'
TABLE = 'dm_f101_round_f'
OUTPUT_CSV = 'dm_f101_round_f.csv'

LOG_SCHEMA = 'logs'
LOG_TABLE = 'etl_log'
LOGGER_USER = 'logger_user'
LOGGER_PASSWORD = 'logger_pass'


def log_message(severity, message):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=LOGGER_USER,
            password=LOGGER_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO {LOG_SCHEMA}.{LOG_TABLE} (log_time, severity, message)
            VALUES (NOW(), %s, %s)
        """, (severity, message))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        # Если логирование упало — печатаем в консоль, чтобы не потерять ошибку
        print(f"Ошибка при записи лога: {e}")

def clean_value(value):
    if isinstance(value, str) and value.strip().upper() == '0E-8':
        return '0'
    if isinstance(value, float):
        if abs(value) < 1e-8:
            return '0'
        return format(value, 'f')
    if hasattr(value, 'is_zero') and value.is_zero():
        return '0'
    return value

def export_to_csv():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        query = f"SELECT * FROM {SCHEMA}.{TABLE} ORDER BY from_date, ledger_account;"
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)  # заголовок
            for row in rows:
                cleaned_row = [clean_value(v) for v in row]
                writer.writerow(cleaned_row)

        cursor.close()
        conn.close()
        log_message('INFO', f"Данные выгружены в {OUTPUT_CSV}")
        print(f"Данные выгружены в {OUTPUT_CSV}")
    except Exception as e:
        log_message('ERROR', f"Ошибка при экспорте: {e}")
        print(f"Ошибка при экспорте: {e}")

def import_from_csv(csv_file=OUTPUT_CSV):
    if not os.path.exists(csv_file):
        message = f"Файл {csv_file} не найден"
        log_message('ERROR', message)
        print(message)
        sys.exit(1)

    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Очистка таблицы перед импортом
        cursor.execute(f"TRUNCATE TABLE {SCHEMA}.{TABLE} RESTART IDENTITY CASCADE;")
        conn.commit()
        log_message('INFO', f"Таблица {SCHEMA}.{TABLE} очищена перед импортом")

        with open(csv_file, mode='r', encoding='utf-8') as f:
            header = f.readline().strip()
            f.seek(0)
            cursor.copy_expert(f"""
                COPY {SCHEMA}.{TABLE} ({header})
                FROM STDIN WITH CSV HEADER DELIMITER ','
            """, f)

        conn.commit()
        cursor.close()
        conn.close()
        log_message('INFO', f"Данные из {csv_file} загружены в {SCHEMA}.{TABLE}")
        print(f"Данные из {csv_file} загружены в {SCHEMA}.{TABLE}")
    except Exception as e:
        log_message('ERROR', f"Ошибка при импорте: {e}")
        print(f"Ошибка при импорте: {e}")
        sys.exit(1)

def main():
    print("Выберите действие:")
    print("1 - Export (выгрузка данных в CSV)")
    print("2 - Import (загрузка данных из CSV)")
    choice = input("Введите 1 или 2: ").strip()
    if choice == '1':
        export_to_csv()
    elif choice == '2':
        import_from_csv()
    else:
        message = f"Неверный выбор: {choice}"
        log_message('ERROR', message)
        print(message)
        sys.exit(1)

if __name__ == '__main__':
    main()
