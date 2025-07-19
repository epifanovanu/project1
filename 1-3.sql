CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_from_date DATE := (i_OnDate - INTERVAL '1 month')::DATE;
    v_to_date DATE := (i_OnDate - INTERVAL '1 day')::DATE;
    v_prev_date DATE := (v_from_date - INTERVAL '1 day')::DATE;
BEGIN
    DELETE FROM dm.dm_f101_round_f WHERE from_date = v_from_date AND to_date = v_to_date;

    INSERT INTO dm.dm_f101_round_f (
        from_date,
        to_date,
        chapter,
        ledger_account,
        characteristic,
        balance_in_rub,
        balance_in_val,
        balance_in_total,
        turn_deb_rub,
        turn_deb_val,
        turn_deb_total,
        turn_cre_rub,
        turn_cre_val,
        turn_cre_total,
        balance_out_rub,
        balance_out_val,
        balance_out_total
    )
    SELECT
        v_from_date,
        v_to_date,
        LEFT(l.chapter, 1) AS chapter,
        LEFT(a.account_number, 5) AS ledger_account,
        a.char_type AS characteristic,

        COALESCE(SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b_prev.balance_out_rub ELSE 0 END), 0) AS balance_in_rub,
        COALESCE(SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b_prev.balance_out_rub ELSE 0 END), 0) AS balance_in_val,
        COALESCE(SUM(COALESCE(b_prev.balance_out_rub, 0)), 0) AS balance_in_total,

        COALESCE(SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.turnover_debet ELSE 0 END), 0) AS turn_deb_rub,
        COALESCE(SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.turnover_debet ELSE 0 END), 0) AS turn_deb_val,
        COALESCE(SUM(COALESCE(t.turnover_debet, 0)), 0) AS turn_deb_total,

        COALESCE(SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.turnover_credit ELSE 0 END), 0) AS turn_cre_rub,
        COALESCE(SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.turnover_credit ELSE 0 END), 0) AS turn_cre_val,
        COALESCE(SUM(COALESCE(t.turnover_credit, 0)), 0) AS turn_cre_total,

        COALESCE(SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b_last.balance_out_rub ELSE 0 END), 0) AS balance_out_rub,
        COALESCE(SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b_last.balance_out_rub ELSE 0 END), 0) AS balance_out_val,
        COALESCE(SUM(COALESCE(b_last.balance_out_rub, 0)), 0) AS balance_out_total

    FROM ds.md_account_d a
    LEFT JOIN ds.md_ledger_account_s l ON l.ledger_account = (LEFT(a.account_number, 5))::integer
    LEFT JOIN dm.dm_account_balance_f b_prev ON b_prev.account_rk = a.account_rk AND b_prev.on_date = v_prev_date
    LEFT JOIN dm.dm_account_balance_f b_last ON b_last.account_rk = a.account_rk AND b_last.on_date = v_to_date
    LEFT JOIN (
        SELECT account_rk,
            SUM(debet_amount_rub) AS turnover_debet,
            SUM(credit_amount_rub) AS turnover_credit
        FROM dm.dm_account_turnover_f
        WHERE on_date BETWEEN v_from_date AND v_to_date
        GROUP BY account_rk
    ) t ON t.account_rk = a.account_rk
    WHERE a.data_actual_date <= v_to_date AND a.data_actual_end_date >= v_from_date
    GROUP BY LEFT(a.account_number, 5), LEFT(l.chapter, 1), a.char_type;

END;
$$;


DO $$
DECLARE
    d DATE := DATE '2018-02-01';
BEGIN
    CALL dm.fill_f101_round_f(d);
END;
$$ LANGUAGE plpgsql;




