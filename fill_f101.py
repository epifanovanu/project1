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
        print("Некорректный формат даты. Используется дата по умолчанию: 01.02.2018")
        on_date = date(2018, 2, 1)
else:
    on_date = date(2018, 2, 1)

# SQL создания процедуры
create_procedure_sql = """
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_FromDate DATE := (i_OnDate - INTERVAL '1 month')::DATE;
    v_ToDate   DATE := (i_OnDate - INTERVAL '1 day')::DATE;
BEGIN
    DELETE FROM dm.dm_f101_round_f WHERE from_date = v_FromDate AND to_date = v_ToDate;

    INSERT INTO dm.dm_f101_round_f (
        from_date, to_date, chapter, ledger_account, characteristic,
        balance_in_rub, balance_in_val, balance_in_total,
        turn_deb_rub, turn_deb_val, turn_deb_total,
        turn_cre_rub, turn_cre_val, turn_cre_total,
        balance_out_rub, balance_out_val, balance_out_total
    )
    SELECT
        v_FromDate,
        v_ToDate,
        COALESCE(la.chapter, 'X'),
        LEFT(acc.account_number, 5),
        acc.char_type,
        SUM(CASE WHEN acc.currency_code IN ('643', '810') THEN bal_start.balance_out_rub ELSE 0 END),
        SUM(CASE WHEN acc.currency_code NOT IN ('643', '810') THEN bal_start.balance_out_rub ELSE 0 END),
        SUM(bal_start.balance_out_rub),
        SUM(CASE WHEN acc.currency_code IN ('643', '810') THEN COALESCE(turn.debet_amount_rub,0) ELSE 0 END),
        SUM(CASE WHEN acc.currency_code NOT IN ('643', '810') THEN COALESCE(turn.debet_amount_rub,0) ELSE 0 END),
        SUM(COALESCE(turn.debet_amount_rub,0)),
        SUM(CASE WHEN acc.currency_code IN ('643', '810') THEN COALESCE(turn.credit_amount_rub,0) ELSE 0 END),
        SUM(CASE WHEN acc.currency_code NOT IN ('643', '810') THEN COALESCE(turn.credit_amount_rub,0) ELSE 0 END),
        SUM(COALESCE(turn.credit_amount_rub,0)),
        SUM(CASE WHEN acc.currency_code IN ('643', '810') THEN bal_end.balance_out_rub ELSE 0 END),
        SUM(CASE WHEN acc.currency_code NOT IN ('643', '810') THEN bal_end.balance_out_rub ELSE 0 END),
        SUM(bal_end.balance_out_rub)
    FROM ds.md_account_d acc
    LEFT JOIN dm.dm_account_balance_f bal_start
        ON bal_start.account_rk = acc.account_rk
        AND bal_start.on_date = (v_FromDate - INTERVAL '1 day')::DATE
    LEFT JOIN dm.dm_account_balance_f bal_end
        ON bal_end.account_rk = acc.account_rk
        AND bal_end.on_date = v_ToDate
    LEFT JOIN dm.dm_account_turnover_f turn
        ON turn.account_rk = acc.account_rk
        AND turn.on_date BETWEEN v_FromDate AND v_ToDate
    LEFT JOIN ds.md_ledger_account_s la
        ON la.ledger_account = LEFT(acc.account_number, 5)::INTEGER
    WHERE acc.data_actual_date <= v_ToDate
      AND (acc.data_actual_end_date IS NULL OR acc.data_actual_end_date >= v_FromDate)
    GROUP BY la.chapter, LEFT(acc.account_number, 5), acc.char_type;

    INSERT INTO logs.etl_log (severity, message)
    VALUES ('INFO', 'Витрина dm_f101_round_f рассчитана за период ' || v_FromDate || ' - ' || v_ToDate);
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO logs.etl_log (severity, message)
        VALUES ('ERROR', 'Ошибка расчета dm_f101_round_f: ' || SQLERRM);
        RAISE;
END;
$$;
"""

# Вызов процедуры
call_procedure_sql = "CALL dm.fill_f101_round_f(%s);"

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(create_procedure_sql)
    print("✅ Процедура успешно создана или обновлена.")

    cur.execute(call_procedure_sql, (on_date,))
    print(f"✅ Процедура успешно выполнена за дату {on_date.strftime('%d.%m.%Y')}.")

except Exception as e:
    print(f"❌ Ошибка: {e}")

finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
