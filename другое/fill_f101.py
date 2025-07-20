import psycopg2
from datetime import datetime, date

# Параметры подключения
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'project1'
DB_USER = 'etl_user'
DB_PASSWORD = 'etl_pass'

# Ввод даты пользователем
user_input = input("Введите дату расчета 101 формы (дд.мм.гггг), по умолчанию 01.02.2018: ").strip()
if user_input:
    try:
        on_date = datetime.strptime(user_input, '%d.%m.%Y').date()
    except ValueError:
        print("❗ Некорректный формат даты. Используется дата по умолчанию: 2018-02-01")
        on_date = date(2018, 2, 1)
else:
    on_date = date(2018, 2, 1)


# SQL вызова процедуры
call_procedure_sql = "CALL dm.fill_f101_round_f(%s);"

# Подключение и выполнение
try:
    with psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(call_procedure_sql, (on_date,))
            print(f"✅ Процедура успешно выполнена за дату {on_date.strftime('%Y-%m-%d')}.")

except Exception as e:
    print(f"❌ Ошибка выполнения: {e}")
