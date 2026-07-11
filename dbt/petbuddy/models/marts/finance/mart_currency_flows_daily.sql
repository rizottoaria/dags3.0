{{ config(materialized='table', order_by='(event_date, currency)') }}

-- Дневной баланс экономики: сколько каждой валюты выдано (source) и потрачено (sink).
select
    event_date,
    currency,
    sumIf(amount, direction = 'in')                       as total_in,
    sumIf(amount, direction = 'out')                      as total_out,
    sum(delta)                                            as net_flow,
    countIf(direction = 'in')                             as in_events,
    countIf(direction = 'out')                            as out_events,
    uniqExact(player_id)                                  as players
from {{ ref('fct_economy_flows') }}
group by event_date, currency
order by event_date, currency
