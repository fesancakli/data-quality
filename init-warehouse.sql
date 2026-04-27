-- --------------------------------------------------------------------------
-- Warehouse init script
-- Compose ilk açıldığında /docker-entrypoint-initdb.d altında çalışır.
-- Volume doluyken tekrar çalışmaz; fresh start için: docker compose down -v
-- --------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS source_db;
CREATE SCHEMA IF NOT EXISTS dwh;

-- --------------------------------------------------------------------------
-- Source
-- --------------------------------------------------------------------------
CREATE TABLE source_db.raw_transactions (
    transaction_id  BIGINT        NOT NULL,
    event_date      DATE          NOT NULL,
    customer_id     BIGINT,
    amount          NUMERIC(14,2),
    currency        VARCHAR(8),
    status          VARCHAR(32),
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------------------------------------
-- Target
-- --------------------------------------------------------------------------
CREATE TABLE dwh.fact_transactions (
    transaction_id  BIGINT        NOT NULL,
    event_date      DATE          NOT NULL,
    customer_id     BIGINT        NOT NULL,
    amount          NUMERIC(14,2) NOT NULL,
    currency        VARCHAR(8)    NOT NULL,
    status          VARCHAR(32)   NOT NULL,
    created_at      TIMESTAMP     NOT NULL,
    load_ts         TIMESTAMP     NOT NULL
);
CREATE INDEX ix_fact_transactions_event_date
    ON dwh.fact_transactions (event_date);

-- --------------------------------------------------------------------------
-- DQ metadata tablosu
-- --------------------------------------------------------------------------
CREATE TABLE dwh.dq_metrics (
    id             BIGSERIAL PRIMARY KEY,
    dag_id         VARCHAR(128),
    run_id         VARCHAR(256),
    run_date       DATE,
    metric_name    VARCHAR(128),
    metric_value   NUMERIC(20,6),
    threshold      NUMERIC(20,6),
    status         VARCHAR(16),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ix_dq_metrics_run_date_metric
    ON dwh.dq_metrics (run_date, metric_name);

-- --------------------------------------------------------------------------
-- Seed: son 10 günün verisi, ~50000 satır
-- NOT: generate_series'i AS t(n) ile alias'lıyoruz ki "n" açık bir kolon olsun.
-- --------------------------------------------------------------------------
INSERT INTO source_db.raw_transactions
  (transaction_id, event_date, customer_id, amount, currency, status)
SELECT
    t.n                                                              AS transaction_id,
    (CURRENT_DATE - (t.n % 10))                                      AS event_date,
    (1000 + (random() * 2000)::int)                                  AS customer_id,
    CASE WHEN random() < 0.03 THEN NULL
         ELSE (random() * 500 + 10)::numeric(14,2) END               AS amount,
    (ARRAY['USD','EUR','TRY','GBP'])[1 + (random()*3)::int]          AS currency,
    (ARRAY['active','completed','refunded','pending','cancelled'])[1 + (random()*4)::int] AS status
FROM generate_series(1, 50000) AS t(n);