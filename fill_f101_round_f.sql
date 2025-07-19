SET ROLE etl_user;

CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(IN i_ondate date)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $$
DECLARE
    v_FromDate DATE := (i_OnDate - INTERVAL '1 month')::DATE;
    v_ToDate   DATE := (i_OnDate - INTERVAL '1 day')::DATE;
BEGIN
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
        COALESCE(la.chapter, 'X') AS chapter,
        SUBSTRING(acc.account_number FROM 1 FOR 5) AS ledger_account,
        acc.char_type AS characteristic,

        -- Начальный баланс
        SUM(CASE WHEN acc.currency_code IN ('643', '810') THEN COALESCE(bal_start.balance_out, 0) ELSE 0 END) AS balance_in_rub,
        SUM(CASE WHEN acc.currency_code NOT IN ('643', '810') THEN COALESCE(bal_start.balance_out, 0) ELSE 0 END) AS balance_in_val,
        SUM(COALESCE(bal_start.balance_out, 0)) AS balance_in_total,

        -- Обороты по дебету
        SUM(COALESCE(turn_deb_rub.turn_deb_rub, 0)) AS turn_deb_rub,
        SUM(COALESCE(turn_deb_val.turn_deb_val, 0)) AS turn_deb_val,
        SUM(COALESCE(turn_deb_rub.turn_deb_rub, 0) + COALESCE(turn_deb_val.turn_deb_val, 0)) AS turn_deb_total,

        -- Обороты по кредиту
        SUM(COALESCE(turn_cre_rub.turn_cre_rub, 0)) AS turn_cre_rub,
        SUM(COALESCE(turn_cre_val.turn_cre_val, 0)) AS turn_cre_val,
        SUM(COALESCE(turn_cre_rub.turn_cre_rub, 0) + COALESCE(turn_cre_val.turn_cre_val, 0)) AS turn_cre_total,

        -- Конечный баланс
        SUM(CASE WHEN acc.currency_code IN ('643', '810') THEN COALESCE(bal_end.balance_out, 0) ELSE 0 END) AS balance_out_rub,
        SUM(CASE WHEN acc.currency_code NOT IN ('643', '810') THEN COALESCE(bal_end.balance_out, 0) ELSE 0 END) AS balance_out_val,
        SUM(COALESCE(bal_end.balance_out, 0)) AS balance_out_total
    FROM ds.md_account_d acc
    LEFT JOIN ds.ft_balance_f bal_start
        ON bal_start.account_rk = acc.account_rk
        AND bal_start.on_date = (v_FromDate - INTERVAL '1 day')::DATE
    LEFT JOIN ds.ft_balance_f bal_end
        ON bal_end.account_rk = acc.account_rk
        AND bal_end.on_date = v_ToDate
    LEFT JOIN (
        SELECT debet_account_rk AS account_rk,
               SUM(debet_amount) AS turn_deb_rub
        FROM ds.ft_posting_f p
        JOIN ds.md_account_d a ON a.account_rk = p.debet_account_rk
        WHERE p.oper_date BETWEEN v_FromDate AND v_ToDate
          AND a.currency_code IN ('643', '810')
        GROUP BY debet_account_rk
    ) turn_deb_rub ON turn_deb_rub.account_rk = acc.account_rk
    LEFT JOIN (
        SELECT debet_account_rk AS account_rk,
               SUM(debet_amount) AS turn_deb_val
        FROM ds.ft_posting_f p
        JOIN ds.md_account_d a ON a.account_rk = p.debet_account_rk
        WHERE p.oper_date BETWEEN v_FromDate AND v_ToDate
          AND a.currency_code NOT IN ('643', '810')
        GROUP BY debet_account_rk
    ) turn_deb_val ON turn_deb_val.account_rk = acc.account_rk
    LEFT JOIN (
        SELECT credit_account_rk AS account_rk,
               SUM(credit_amount) AS turn_cre_rub
        FROM ds.ft_posting_f p
        JOIN ds.md_account_d a ON a.account_rk = p.credit_account_rk
        WHERE p.oper_date BETWEEN v_FromDate AND v_ToDate
          AND a.currency_code IN ('643', '810')
        GROUP BY credit_account_rk
    ) turn_cre_rub ON turn_cre_rub.account_rk = acc.account_rk
    LEFT JOIN (
        SELECT credit_account_rk AS account_rk,
               SUM(credit_amount) AS turn_cre_val
        FROM ds.ft_posting_f p
        JOIN ds.md_account_d a ON a.account_rk = p.credit_account_rk
        WHERE p.oper_date BETWEEN v_FromDate AND v_ToDate
          AND a.currency_code NOT IN ('643', '810')
        GROUP BY credit_account_rk
    ) turn_cre_val ON turn_cre_val.account_rk = acc.account_rk
    LEFT JOIN ds.md_ledger_account_s la
        ON la.ledger_account = SUBSTRING(acc.account_number FROM 1 FOR 5)::INTEGER
    WHERE acc.data_actual_date <= v_ToDate
      AND (acc.data_actual_end_date IS NULL OR acc.data_actual_end_date >= v_FromDate)
    GROUP BY la.chapter, SUBSTRING(acc.account_number FROM 1 FOR 5), acc.char_type

    ON CONFLICT (from_date, to_date, chapter, ledger_account, characteristic) DO UPDATE
    SET 
        balance_in_rub = EXCLUDED.balance_in_rub,
        balance_in_val = EXCLUDED.balance_in_val,
        balance_in_total = EXCLUDED.balance_in_total,
        turn_deb_rub = EXCLUDED.turn_deb_rub,
        turn_deb_val = EXCLUDED.turn_deb_val,
        turn_deb_total = EXCLUDED.turn_deb_total,
        turn_cre_rub = EXCLUDED.turn_cre_rub,
        turn_cre_val = EXCLUDED.turn_cre_val,
        turn_cre_total = EXCLUDED.turn_cre_total,
        balance_out_rub = EXCLUDED.balance_out_rub,
        balance_out_val = EXCLUDED.balance_out_val,
        balance_out_total = EXCLUDED.balance_out_total;

    -- Логируем успешное завершение
    INSERT INTO logs.etl_log (severity, message)
    VALUES ('INFO', 'Витрина dm_f101_round_f рассчитана за период ' ||
            TO_CHAR(v_FromDate, 'YYYY-MM-DD') || ' - ' || TO_CHAR(v_ToDate, 'YYYY-MM-DD'));

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO logs.etl_log (severity, message)
        VALUES ('ERROR', 'Ошибка расчета dm_f101_round_f: ' || SQLERRM);
        RAISE;
END;
$$;
