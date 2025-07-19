CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
BEGIN
    -- Логируем старт
    INSERT INTO logs.etl_log(severity, message)
    VALUES ('INFO', 'fill_account_turnover_f started for date ' || i_OnDate);

    DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;

    INSERT INTO dm.dm_account_turnover_f (
        on_date,
        account_rk,
        credit_amount,
        credit_amount_rub,
        debet_amount,
        debet_amount_rub
    )
    SELECT
        i_OnDate,
        t.account_rk,
        SUM(t.credit_amount),
        SUM(t.credit_amount) * COALESCE(r.reduced_cource, 1),
        SUM(t.debet_amount),
        SUM(t.debet_amount) * COALESCE(r.reduced_cource, 1)
    FROM (
        SELECT 
            credit_account_rk AS account_rk, 
            credit_amount, 
            0::NUMERIC AS debet_amount
        FROM ds.ft_posting_f
        WHERE oper_date = i_OnDate AND credit_account_rk IS NOT NULL

        UNION ALL

        SELECT 
            debet_account_rk AS account_rk, 
            0::NUMERIC AS credit_amount, 
            debet_amount
        FROM ds.ft_posting_f
        WHERE oper_date = i_OnDate AND debet_account_rk IS NOT NULL
    ) t
    LEFT JOIN ds.md_account_d a ON a.account_rk = t.account_rk
        AND i_OnDate BETWEEN a.data_actual_date AND a.data_actual_end_date
    LEFT JOIN ds.md_exchange_rate_d r ON r.currency_rk = a.currency_rk
        AND i_OnDate BETWEEN r.data_actual_date AND r.data_actual_end_date
    GROUP BY t.account_rk, r.reduced_cource
    HAVING SUM(t.credit_amount) > 0 OR SUM(t.debet_amount) > 0;

    -- Логируем успешное завершение и время выполнения
    INSERT INTO logs.etl_log(severity, message)
    VALUES ('INFO', 'fill_account_turnover_f finished for date ' || i_OnDate || 
           '. Duration: ' || EXTRACT(EPOCH FROM clock_timestamp() - v_start_time) || ' seconds');

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO logs.etl_log(severity, message)
        VALUES ('ERROR', 'fill_account_turnover_f failed for date ' || i_OnDate || 
                '. Error: ' || SQLERRM);
        RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
BEGIN
    INSERT INTO logs.etl_log(severity, message)
    VALUES ('INFO', 'fill_account_balance_f started for date ' || i_OnDate);

    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;

    INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        i_OnDate,
        a.account_rk,
        CASE 
            WHEN a.char_type = 'А' THEN
                COALESCE(prev.balance_out, 0) 
                + COALESCE(turnover.debet_amount, 0) 
                - COALESCE(turnover.credit_amount, 0)
            WHEN a.char_type = 'П' THEN
                COALESCE(prev.balance_out, 0) 
                - COALESCE(turnover.debet_amount, 0) 
                + COALESCE(turnover.credit_amount, 0)
            ELSE
                0
        END AS balance_out,
        CASE 
            WHEN a.char_type = 'А' THEN
                COALESCE(prev.balance_out_rub, 0) 
                + COALESCE(turnover.debet_amount_rub, 0) 
                - COALESCE(turnover.credit_amount_rub, 0)
            WHEN a.char_type = 'П' THEN
                COALESCE(prev.balance_out_rub, 0) 
                - COALESCE(turnover.debet_amount_rub, 0) 
                + COALESCE(turnover.credit_amount_rub, 0)
            ELSE
                0
        END AS balance_out_rub
    FROM ds.md_account_d a
    LEFT JOIN dm.dm_account_balance_f prev ON prev.account_rk = a.account_rk AND prev.on_date = i_OnDate - INTERVAL '1 day'
    LEFT JOIN dm.dm_account_turnover_f turnover ON turnover.account_rk = a.account_rk AND turnover.on_date = i_OnDate
    WHERE i_OnDate BETWEEN a.data_actual_date AND a.data_actual_end_date;

    INSERT INTO logs.etl_log(severity, message)
    VALUES ('INFO', 'fill_account_balance_f finished for date ' || i_OnDate || 
           '. Duration: ' || EXTRACT(EPOCH FROM clock_timestamp() - v_start_time) || ' seconds');

END;
$$;


DO $$
DECLARE
    d DATE := DATE '2018-01-01';
BEGIN
    WHILE d <= DATE '2018-01-31' LOOP
        CALL ds.fill_account_turnover_f(d);
        CALL ds.fill_account_balance_f(d);
        d := d + INTERVAL '1 day';
    END LOOP;
END $$ LANGUAGE plpgsql;
