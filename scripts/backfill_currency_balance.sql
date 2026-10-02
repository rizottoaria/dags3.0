-- Помесячный бэкфилл petbuddy_clean.mart_currency_balance_daily (та же логика, что в dbt-модели).
-- Вся история одним запросом не влезает в лимит памяти ClickHouse (toString(properties) по каждой
-- строке), поэтому — по месяцу за раз. Перед вставкой месяц удаляется (идемпотентно).
--
--   clickhouse-client --param_d_from=2026-06-01 --param_d_to=2026-06-30 \
--       --multiquery < scripts/backfill_currency_balance.sql

ALTER TABLE petbuddy_clean.mart_currency_balance_daily
    DELETE WHERE event_date BETWEEN {d_from:Date} AND {d_to:Date}
    SETTINGS mutations_sync = 2;

INSERT INTO petbuddy_clean.mart_currency_balance_daily
    (event_date, currency, players, avg_balance, median_balance, min_balance, max_balance)
WITH snapshots AS (
    SELECT
        event_date,
        player_id,
        kv.1 AS currency,
        kv.2 AS balance
    FROM petbuddy.events FINAL
    ARRAY JOIN
        JSONExtractKeysAndValues(
            JSONExtractRaw(toString(properties), 'balanceSnapshot'),
            'Int64'
        ) AS kv
    WHERE event_date BETWEEN {d_from:Date} AND {d_to:Date}
      AND JSONExtractRaw(toString(properties), 'balanceSnapshot') != ''
)
SELECT
    event_date,
    currency,
    uniqExact(player_id)     AS players,
    round(avg(balance), 2)   AS avg_balance,
    median(balance)          AS median_balance,
    min(balance)             AS min_balance,
    max(balance)             AS max_balance
FROM snapshots
GROUP BY event_date, currency
SETTINGS max_threads = 1;
