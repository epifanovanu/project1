import psycopg2
from datetime import datetime

# Конфигурация подключения
DB_NAME = 'project1'
DB_HOST = 'localhost'
DB_PORT = '5432'

DB_USER = 'etl_user'
DB_PASSWORD = 'etl_pass'

LOGGER_USER = 'logger_user'
LOGGER_PASSWORD = 'logger_pass'

SCHEMA_DM = 'dm'
LOG_SCHEMA = 'logs'
LOG_TABLE = 'etl_log'


def refresh_materialized_view(report_date):
    conn = None
    log_conn = None
    status = 'SUCCESS'
    error_message = None

    try:
        # Подключение для REFRESH
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Выполняем REFRESH
        print(f'Обновляем витрину на дату: {report_date}')
        cur.execute(f"REFRESH MATERIALIZED VIEW {SCHEMA_DM}.mv_101_report;")
        cur.close()

    except Exception as e:
        status = 'FAIL'
        error_message = str(e)
        print(f'Ошибка при обновлении витрины: {error_message}')

    finally:
        if conn:
            conn.close()

        # Логирование результата
        try:
            log_conn = psycopg2.connect(
                dbname=DB_NAME,
                user=LOGGER_USER,
                password=LOGGER_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            log_conn.autocommit = True
            log_cur = log_conn.cursor()

            log_cur.execute(f"""
                INSERT INTO {LOG_SCHEMA}.{LOG_TABLE} (log_time, process_name, process_date, status, message)
                VALUES (%s, %s, %s, %s, %s);
            """, (
                datetime.now(),
                'mv_101_report_refresh',
                report_date,
                status,
                error_message
            ))

            log_cur.close()
            print(f'Лог успешно записан: статус {status}')

        except Exception as log_e:
            print(f'Ошибка записи лога: {log_e}')

        finally:
            if log_conn:
                log_conn.close()


if __name__ == '__main__':
    input_date = input('Введите дату в формате YYYY-MM-DD: ').strip()

    try:
        datetime.strptime(input_date, '%Y-%m-%d')
        refresh_materialized_view(input_date)
    except ValueError:
        print('Ошибка: неверный формат даты. Используйте YYYY-MM-DD.')
