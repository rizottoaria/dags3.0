# petbuddy_dbt

dbt-проект для витрин по таблице `petbuddy.events` (ClickHouse на 185.182.9.18).
Все модели материализуются в схему **`petbuddy_clean`**.

## Установка

```bash
pip install dbt-clickhouse
```

## Настройка подключения

Секреты — через переменные окружения (см. `profiles.yml`):

```bash
export CH_HOST=185.182.9.18
export CH_USER=<user>
export CH_PASSWORD=<password>
```

## Запуск

```bash
cd petbuddy_dbt
dbt deps          # если добавите пакеты
dbt run  --profiles-dir .     # соберёт все витрины в petbuddy_clean
dbt test --profiles-dir .     # прогонит тесты качества
dbt docs generate --profiles-dir . && dbt docs serve
```

## Слои

| Слой | Модель | Материализация | Что делает |
|------|--------|----------------|------------|
| staging | `stg_events` | view | Дедуп (FINAL) + плоские поля из properties |
| intermediate | `int_events__revenue` | ephemeral | Платёжные события |
| intermediate | `int_events__economy` | ephemeral | Разворот валют по событиям экономики |
| **mart / core** | `dim_players` | table | Профиль игрока + LTV |
| **mart / core** | `mart_daily_active_users` | table | DAU, new/returning, ARPDAU |
| **mart / finance** | `fct_revenue_events` | table | Факт платежей |
| **mart / finance** | `mart_revenue_daily` | table | Выручка/ARPU/ARPPU по дням |
| **mart / finance** | `fct_economy_flows` | table | Движение валют (source/sink) |
| **mart / finance** | `mart_currency_flows_daily` | table | Дневные source/sink по валютам |
| **mart / finance** | `mart_currency_balance_daily` | table | Средние остатки валют (balanceSnapshot) |

## Заметки по данным

- `events` — `ReplacingMergeTree(received_at)`; в `stg_events` применяется `FINAL`.
- Валютные дельты лежат в топ-уровневых ключах `Currency_*` / `Chests_*`,
  снимок баланса — во вложенном `balanceSnapshot`.
- `revenue.type`: `ad_reward` (реклама) и `purchase` (реальные покупки).

## Дальнейшее развитие

- Перевести `stg_events` на `incremental` по `event_date` при росте объёма.
- Добавить `mart_player_retention` (когорты D1/D7/D30) и `mart_ab_test_summary`.
- Rolling WAU/MAU поверх `mart_daily_active_users`.
