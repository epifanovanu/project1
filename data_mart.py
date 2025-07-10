#1
import psycopg2

#2
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

#3 
report_date = input("Введите дату в формате YYYY-MM-DD: ")

#4
def log_event(severity, message):
    try:
        with psycopg2.connect(
            dbname=DB_NAME,
            user=LOGGER_USER,
            password=LOGGER_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        ) as log_conn:
            with log_conn.cursor() as log_cur:
                log_cur.execute(f"""
                    INSERT INTO {LOG_SCHEMA}.{LOG_TABLE} (severity, message)
                    VALUES (%s, %s);
                """, (severity, message))
            log_conn.commit()
    except Exception as log_error:
        print(f"Ошибка записи лога: {log_error}")

#5
try:
    with psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    ) as conn:
        with conn.cursor() as cur:
#6
            # Удаляем старое materialized view
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {SCHEMA_DM}.mv_101_report;")
#7
            # Создаём materialized view с правильным SQL
            create_mv_sql = f"""
                CREATE MATERIALIZED VIEW {SCHEMA_DM}.mv_101_report AS
                WITH posting_with_accounts AS (
                    SELECT
                        p.oper_date::date AS oper_date,
                        p.debet_account_rk AS account_rk,
                        p.debet_amount AS amount,
                        'D' AS debit_credit
                    FROM ds.ft_posting_f p
                    WHERE p.debet_amount IS NOT NULL
                    AND p.oper_date::date <= DATE %s

                    UNION ALL

                    SELECT
                        p.oper_date::date AS oper_date,
                        p.credit_account_rk AS account_rk,
                        p.credit_amount AS amount,
                        'C' AS debit_credit
                    FROM ds.ft_posting_f p
                    WHERE p.credit_amount IS NOT NULL
                    AND p.oper_date::date <= DATE %s
                ),
                posting_enriched AS (
                    SELECT
                        p.oper_date,
                        p.amount,
                        p.debit_credit,
                        acc.account_rk,
                        acc.account_number,
                        acc.currency_code,
                        COALESCE(led.ledger_account, CAST(LEFT(acc.account_number, 5) AS NUMERIC)) AS ledger_account
                    FROM posting_with_accounts p
                    INNER JOIN ds.md_account_d acc
                        ON acc.account_rk = p.account_rk
                        AND acc.data_actual_date <= p.oper_date
                        AND (acc.data_actual_end_date IS NULL OR acc.data_actual_end_date >= p.oper_date)
                    LEFT JOIN ds.md_ledger_account_s led
                        ON led.ledger_account = CAST(LEFT(acc.account_number, 5) AS NUMERIC)
                        AND led.start_date <= p.oper_date
                ),
                daily_posting AS (
                    SELECT
                        account_rk,
                        account_number,
                        currency_code,
                        ledger_account,
                        oper_date AS report_date,
                        SUM(CASE WHEN debit_credit = 'D' THEN amount ELSE 0 END) AS debit_turnover,
                        SUM(CASE WHEN debit_credit = 'C' THEN amount ELSE 0 END) AS credit_turnover,
                        SUM(CASE WHEN debit_credit = 'D' THEN amount ELSE -amount END) AS net_turnover
                    FROM posting_enriched
                    GROUP BY account_rk, account_number, currency_code, ledger_account, oper_date
                ),
                balances_with_opening AS (
                    SELECT
                        dp.*,
                        COALESCE(SUM(net_turnover) OVER (
                            PARTITION BY account_rk
                            ORDER BY report_date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ), 0) AS opening_balance
                    FROM daily_posting dp
                )
                SELECT
                    account_rk,
                    account_number,
                    currency_code,
                    ledger_account,
                    report_date,
                    opening_balance,
                    debit_turnover,
                    credit_turnover,
                    opening_balance + debit_turnover - credit_turnover AS closing_balance
                FROM balances_with_opening
                WHERE report_date = DATE %s;
            """
#11
            # Выполняем создание materialized view, передаем дату 3 раза
            cur.execute(create_mv_sql, (report_date, report_date, report_date))
#12
        conn.commit()
    print(f"Materialized view mv_101_report успешно создана за дату {report_date}")
    log_event('INFO', f'mv_101_report обновлена успешно на {report_date}')
#13
except Exception as e:
    print(f"Ошибка: {e}")
    log_event('ERROR', f'Ошибка обновления mv_101_report: {e}')