{{ config(materialized='table', order_by='(event_date, revenue_type)') }}

-- Выручка по дням и типу (ad_reward / purchase) в USD + ARPU / ARPPU.
-- ВАЖНО: purchase-выручку НЕЛЬЗЯ брать сырой из событий (revenue_amount у покупок в
-- исходной валюте: USD/PHP/UAH/CHF/... — суммирование смешает валюты). Поэтому:
--   * ad_reward — из fct_revenue_events.revenue_amount_usd (ad-доход уже в USD);
--   * purchase  — из int_iap_usd (авторитетный USD-гибрид monetization + конверсия
--                 старых событий по курсам), одна строка = одна покупка.

with ad as (
    select
        event_date,
        count()                        as transactions,
        uniqExact(player_id)           as paying_users,
        sum(revenue_amount_usd)        as revenue
    from {{ ref('fct_revenue_events') }}
    where revenue_type = 'ad_reward'
    group by event_date
),

iap as (
    select
        purchase_date                  as event_date,
        count()                        as transactions,
        uniqExact(player_id)           as paying_users,
        sum(usd_amount)                as revenue
    from {{ ref('int_iap_usd') }}
    group by purchase_date
),

unioned as (
    select event_date, 'ad_reward' as revenue_type, transactions, paying_users, revenue from ad
    union all
    select event_date, 'purchase'  as revenue_type, transactions, paying_users, revenue from iap
),

dau as (
    select event_date, dau from {{ ref('mart_daily_active_users') }}
)

select
    u.event_date                                          as event_date,
    u.revenue_type                                        as revenue_type,
    u.transactions                                        as transactions,
    u.paying_users                                        as paying_users,
    round(u.revenue, 4)                                   as revenue,
    round(u.revenue / nullIf(u.transactions, 0), 6)       as avg_transaction,
    d.dau                                                 as dau,
    round(u.revenue / nullIf(d.dau, 0), 4)                as arpu,
    round(u.revenue / nullIf(u.paying_users, 0), 4)       as arppu
from unioned u
left join dau d using (event_date)
order by u.event_date, u.revenue_type
