{{ config(materialized='table', order_by='(event_date, revenue_type)') }}

-- Выручка по дням и типу (ad_reward / purchase) + ARPU / ARPPU.
with rev as (
    select * from {{ ref('int_events__revenue') }}
),

dau as (
    select event_date, dau
    from {{ ref('mart_daily_active_users') }}
)

select
    r.event_date                                          as event_date,
    coalesce(r.revenue_type, 'unknown')                   as revenue_type,
    count()                                               as transactions,
    uniqExact(r.player_id)                                as paying_users,
    round(sum(r.revenue_amount), 4)                       as revenue,
    round(avg(r.revenue_amount), 6)                       as avg_transaction,
    any(d.dau)                                            as dau,
    round(sum(r.revenue_amount) / nullIf(any(d.dau), 0), 4)          as arpu,
    round(sum(r.revenue_amount) / nullIf(uniqExact(r.player_id), 0), 4) as arppu
from rev r
left join dau d using (event_date)
group by r.event_date, coalesce(r.revenue_type, 'unknown')
order by r.event_date, revenue_type
