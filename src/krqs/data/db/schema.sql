CREATE TABLE IF NOT EXISTS dart_raw_responses (
    corp_code   VARCHAR   NOT NULL,
    report_code VARCHAR   NOT NULL,
    bsns_year   INTEGER   NOT NULL,
    endpoint    VARCHAR   NOT NULL,
    fetched_at  TIMESTAMP NOT NULL,
    raw_json    JSON      NOT NULL,
    PRIMARY KEY (corp_code, report_code, bsns_year, endpoint)
);

CREATE TABLE IF NOT EXISTS corps (
    corp_code     VARCHAR PRIMARY KEY,
    stock_code    VARCHAR,
    corp_name     VARCHAR NOT NULL,
    sector_krx    VARCHAR,
    sector_custom VARCHAR,
    market        VARCHAR,
    last_updated  TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_corps_stock ON corps(stock_code);
CREATE INDEX IF NOT EXISTS idx_corps_name  ON corps(corp_name);

CREATE TABLE IF NOT EXISTS financials_quarterly (
    corp_code              VARCHAR NOT NULL,
    fiscal_year            INTEGER NOT NULL,
    fiscal_quarter         INTEGER NOT NULL,
    period_end             DATE,
    revenue                BIGINT,
    cogs                   BIGINT,
    gross_profit           BIGINT,
    sga                    BIGINT,
    operating_income       BIGINT,
    interest_expense       BIGINT,
    net_income             BIGINT,
    total_assets           BIGINT,
    cash_and_equivalents   BIGINT,
    short_term_investments BIGINT,
    total_debt             BIGINT,
    total_equity           BIGINT,
    total_liabilities      BIGINT,
    depreciation           BIGINT,
    ppe                    BIGINT,
    retained_earnings      BIGINT,
    gpm                    DOUBLE,
    opm                    DOUBLE,
    roe                    DOUBLE,
    debt_ratio             DOUBLE,
    ebitda                 BIGINT,
    ebitda_margin          DOUBLE,
    revenue_yoy            DOUBLE,
    opm_yoy                DOUBLE,
    source_updated_at      TIMESTAMP NOT NULL,
    PRIMARY KEY (corp_code, fiscal_year, fiscal_quarter)
);

CREATE INDEX IF NOT EXISTS idx_fin_year ON financials_quarterly(fiscal_year);

CREATE TABLE IF NOT EXISTS screener_signals (
    corp_code   VARCHAR NOT NULL,
    signal_type VARCHAR NOT NULL,
    detected_at TIMESTAMP NOT NULL,
    score       DOUBLE,
    evidence    JSON,
    PRIMARY KEY (corp_code, signal_type)
);
