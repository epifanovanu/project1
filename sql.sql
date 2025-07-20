DROP SCHEMA DS CASCADE;
DROP SCHEMA DM CASCADE;
DROP SCHEMA LOGS CASCADE;

CREATE SCHEMA IF NOT EXISTS DS;
CREATE SCHEMA IF NOT EXISTS DM;
CREATE SCHEMA IF NOT EXISTS LOGS;

-- =======================
-- Таблицы схемы DS
-- =======================

-- FT_BALANCE_F
CREATE TABLE DS.FT_BALANCE_F (
    ON_DATE DATE NOT NULL,
    ACCOUNT_RK NUMERIC NOT NULL,
    CURRENCY_RK NUMERIC,
    BALANCE_OUT DOUBLE PRECISION,
    PRIMARY KEY (ON_DATE, ACCOUNT_RK)
);

-- FT_POSTING_F
CREATE TABLE DS.FT_POSTING_F (
    ID SERIAL PRIMARY KEY,
    OPER_DATE DATE NOT NULL,
    CREDIT_ACCOUNT_RK NUMERIC NOT NULL,
    DEBET_ACCOUNT_RK NUMERIC NOT NULL,
    CREDIT_AMOUNT DOUBLE PRECISION,
    DEBET_AMOUNT DOUBLE PRECISION
);

-- MD_ACCOUNT_D
CREATE TABLE DS.MD_ACCOUNT_D (
    DATA_ACTUAL_DATE DATE NOT NULL,
    DATA_ACTUAL_END_DATE DATE NOT NULL,
    ACCOUNT_RK NUMERIC NOT NULL,
    ACCOUNT_NUMBER VARCHAR(20) NOT NULL,
    CHAR_TYPE VARCHAR(1) NOT NULL,
    CURRENCY_RK NUMERIC NOT NULL,
    CURRENCY_CODE VARCHAR(3) NOT NULL,
    PRIMARY KEY (DATA_ACTUAL_DATE, ACCOUNT_RK)
);

-- MD_CURRENCY_D
CREATE TABLE DS.MD_CURRENCY_D (
    CURRENCY_RK NUMERIC NOT NULL,
    DATA_ACTUAL_DATE DATE NOT NULL,
    DATA_ACTUAL_END_DATE DATE,
    CURRENCY_CODE VARCHAR(3),
    CODE_ISO_CHAR VARCHAR(3),
    PRIMARY KEY (CURRENCY_RK, DATA_ACTUAL_DATE)
);

-- MD_EXCHANGE_RATE_D
CREATE TABLE DS.MD_EXCHANGE_RATE_D (
    DATA_ACTUAL_DATE DATE NOT NULL,
    DATA_ACTUAL_END_DATE DATE,
    CURRENCY_RK NUMERIC NOT NULL,
    REDUCED_COURCE DOUBLE PRECISION,
    CODE_ISO_NUM VARCHAR(3),
    PRIMARY KEY (DATA_ACTUAL_DATE, CURRENCY_RK)
);

-- MD_LEDGER_ACCOUNT_S
CREATE TABLE DS.MD_LEDGER_ACCOUNT_S (
    CHAPTER CHAR(1),
    CHAPTER_NAME VARCHAR(16),
    SECTION_NUMBER INTEGER,
    SECTION_NAME VARCHAR(22),
    SUBSECTION_NAME VARCHAR(21),
    LEDGER1_ACCOUNT INTEGER,
    LEDGER1_ACCOUNT_NAME VARCHAR(47),
    LEDGER_ACCOUNT INTEGER NOT NULL,
    LEDGER_ACCOUNT_NAME VARCHAR(153),
    CHARACTERISTIC CHAR(1),
    IS_RESIDENT INTEGER,
    IS_RESERVE INTEGER,
    IS_RESERVED INTEGER,
    IS_LOAN INTEGER,
    IS_RESERVED_ASSETS INTEGER,
    IS_OVERDUE INTEGER,
    IS_INTEREST INTEGER,
    PAIR_ACCOUNT VARCHAR(5),
    START_DATE DATE NOT NULL,
    END_DATE DATE,
    IS_RUB_ONLY INTEGER,
    MIN_TERM VARCHAR(1),
    MIN_TERM_MEASURE VARCHAR(1),
    MAX_TERM VARCHAR(1),
    MAX_TERM_MEASURE VARCHAR(1),
    LEDGER_ACC_FULL_NAME_TRANSLIT VARCHAR(1),
    IS_REVALUATION VARCHAR(1),
    IS_CORRECT CHAR(1),
    PRIMARY KEY (LEDGER_ACCOUNT, START_DATE)
);


-- =======================
-- Таблицы схемы DM
-- =======================

-- DM_ACCOUNT_TURNOVER_F
CREATE TABLE DM.DM_ACCOUNT_TURNOVER_F (
    ON_DATE DATE,
    ACCOUNT_RK NUMERIC,
    CREDIT_AMOUNT NUMERIC(23,8),
    CREDIT_AMOUNT_RUB NUMERIC(23,8),
    DEBET_AMOUNT NUMERIC(23,8),
    DEBET_AMOUNT_RUB NUMERIC(23,8),
    PRIMARY KEY (ON_DATE, ACCOUNT_RK)
);


CREATE TABLE DM.DM_ACCOUNT_BALANCE_F (
    ON_DATE          DATE           NOT NULL,   -- дата, за которую актуален остаток
    ACCOUNT_RK       NUMERIC         NOT NULL,   -- идентификатор счета
    BALANCE_OUT      NUMERIC(23,8),              -- остаток в валюте счета
    BALANCE_OUT_RUB  NUMERIC(23,8),              -- остаток в рублях
    PRIMARY KEY (ON_DATE, ACCOUNT_RK)
);


--======1-3
CREATE TABLE dm.dm_f101_round_f (
    from_date DATE NOT NULL,               -- Начало интервала расчета
    to_date DATE NOT NULL,                 -- Конец интервала расчета
    chapter CHAR(1) NOT NULL,              -- Глава баланса (1 символ)
    ledger_account CHAR(5) NOT NULL,       -- Балансовый счет (первые 5 символов account_number)
    characteristic CHAR(1) NOT NULL,       -- Характеристика счета (char_type)
    
    balance_in_rub NUMERIC(23,8) DEFAULT 0,    -- Входящий остаток для рублевых счетов
    balance_in_val NUMERIC(23,8) DEFAULT 0,    -- Входящий остаток для валютных счетов
    balance_in_total NUMERIC(23,8) DEFAULT 0,  -- Входящий остаток - итого
    
    turn_deb_rub NUMERIC(23,8) DEFAULT 0,      -- Сумма дебетовых оборотов для рублевых счетов
    turn_deb_val NUMERIC(23,8) DEFAULT 0,      -- Сумма дебетовых оборотов для валютных счетов
    turn_deb_total NUMERIC(23,8) DEFAULT 0,    -- Сумма дебетовых оборотов - итого
    
    turn_cre_rub NUMERIC(23,8) DEFAULT 0,      -- Сумма кредитовых оборотов для рублевых счетов
    turn_cre_val NUMERIC(23,8) DEFAULT 0,      -- Сумма кредитовых оборотов для валютных счетов
    turn_cre_total NUMERIC(23,8) DEFAULT 0,    -- Сумма кредитовых оборотов - итого
    
    balance_out_rub NUMERIC(23,8) DEFAULT 0,   -- Сумма исходящего остатка для рублевых счетов
    balance_out_val NUMERIC(23,8) DEFAULT 0,   -- Сумма исходящего остатка для валютных счетов
    balance_out_total NUMERIC(23,8) DEFAULT 0, -- Сумма исходящего остатка - итого
    
    PRIMARY KEY (from_date, to_date, ledger_account)
);



-- =======================
-- Таблица логов (схема LOGS)
-- =======================

CREATE TABLE IF NOT EXISTS LOGS.ETL_LOG (
    ID SERIAL PRIMARY KEY,
    LOG_TIME TIMESTAMP DEFAULT now(),
    SEVERITY TEXT,
    MESSAGE TEXT
);


-- ==========================
-- Пользователи
-- ==========================
-- etl_user
DROP USER etl_user;
CREATE USER etl_user WITH PASSWORD 'etl_pass';
GRANT USAGE ON SCHEMA ds TO etl_user;
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA ds TO etl_user;
GRANT USAGE, SELECT ON SEQUENCE ds.ft_posting_f_id_seq TO etl_user;

GRANT USAGE ON SCHEMA dm TO etl_user;
GRANT CREATE ON SCHEMA dm TO etl_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dm TO etl_user;
ALTER MATERIALIZED VIEW dm.mv_101_report OWNER TO etl_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE dm.dm_f101_round_f TO etl_user;
GRANT TRUNCATE ON TABLE dm.dm_f101_round_f TO etl_user;

-- logger_user
DROP USER logger_user;
CREATE USER logger_user WITH PASSWORD 'logger_pass';
GRANT USAGE ON SCHEMA logs TO logger_user;
GRANT SELECT,INSERT,UPDATE ON logs.etl_log TO logger_user;
GRANT USAGE, SELECT ON SEQUENCE logs.etl_log_id_seq TO logger_user;


