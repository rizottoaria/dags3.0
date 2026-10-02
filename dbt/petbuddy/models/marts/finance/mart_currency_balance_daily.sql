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
-- заменяет эти дни.
--
-- Фильтр — по event_date (MATERIALIZED toDate(event_at), первый столбец ключа сортировки
-- events): так ClickHouse отсекает гранулы. По toDate(event_at) отсечения НЕТ — каждый
-- прогон сканировал всю таблицу (~30 мин).
-- Пустая таблица: max() даёт 1970-01-01, а 1970-01-01 - 2 дня в Date переворачивается
-- в 2149-06-05 -> фильтр ничего не находил и витрина 2,5 месяца оставалась пустой.
-- Поэтому на пустой таблице берём только последние 2 дня; историю грузить помесячно
-- (scripts/backfill_currency_balance.sql), не --full-refresh (вся история не влезает в память).
with snapshots as (
    select
        event_date,
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
      and event_date >= (select if(count() = 0, today() - 2, max(event_date) - 2) from {{ this }})
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
