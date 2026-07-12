{{ config(
    materialized='table',
    order_by='(event_date, currency)',
    query_settings={'max_threads': 2, 'max_bytes_before_external_group_by': 2000000000}
) }}

-- Средний остаток валют у игроков по дням (из properties.balanceSnapshot).
-- balanceSnapshot — вложенный объект, поэтому достаём его через JSONExtractRaw.
with snapshots as (
    select
        event_date,
        player_id,
        kv.1 as currency,
        kv.2 as balance
    from {{ ref('stg_events') }}
    array join
        JSONExtractKeysAndValues(
            JSONExtractRaw(toString(properties), 'balanceSnapshot'),
            'Int64'
        ) as kv
    where JSONExtractRaw(toString(properties), 'balanceSnapshot') != ''
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
