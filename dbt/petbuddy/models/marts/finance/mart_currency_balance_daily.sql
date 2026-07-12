{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['event_date', 'currency'],
    order_by='(event_date, currency)',
    query_settings={'max_threads': 1}
) }}

-- Средний остаток валют у игроков по дням (из properties.balanceSnapshot).
--
-- Инкрементально + прямое чтение из источника: balanceSnapshot достаётся через
-- toString(properties) (материализует весь JSON построчно) — по всей истории это
-- упирается в лимит памяти ClickHouse. При обычном run обрабатываем только
-- последние event_date (окно 2 дня), delete+insert по (event_date, currency)
-- заменяет эти дни. Полный пересбор: --full-refresh.
with snapshots as (
    select
        toDate(event_at) as event_date,
        player_id,
        kv.1 as currency,
        kv.2 as balance
    from {{ source('petbuddy', 'events') }} final
    array join
        JSONExtractKeysAndValues(
            JSONExtractRaw(toString(properties), 'balanceSnapshot'),
            'Int64'
        ) as kv
    where JSONExtractRaw(toString(properties), 'balanceSnapshot') != ''

    {% if is_incremental() %}
      and toDate(event_at) >= (select max(event_date) from {{ this }}) - toIntervalDay(2)
    {% endif %}
)

select
    event_date,
    currency,
    uniqExact(player_id)                     as players,
    round(avg(balance), 2)                   as avg_balance,
    median(balance)                          as median_balance,
    min(balance)                             as min_balance,
    max(balance)                             as max_balance
from snapshots
group by event_date, currency
order by event_date, currency
