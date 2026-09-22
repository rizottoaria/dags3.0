{{ config(materialized='table', order_by='(event_date, player_id)') }}

-- Факт-таблица платёжных событий (одна строка = один платёж).
-- ВНИМАНИЕ по валютам: revenue_amount у purchase — в ИСХОДНОЙ валюте (currency),
-- клиент шлёт локализованную сумму (USD/PHP/UAH/CHF/...). Складывать revenue_amount
-- между валютами НЕЛЬЗЯ — для агрегатов используем revenue_amount_usd. ad_reward
-- приходит уже в USD (currency='USD'). Конверсия покупок в USD — по int_currency_rates_usd
-- (курс на дату <= даты покупки; fallback — самый ранний курс валюты; USD -> 1.0).

with ev as (
    select
        event_id, player_id, session_id, event_at, event_date,
        country, app_version, ab_version,
        revenue_type, revenue_source, revenue_currency, revenue_amount
    from {{ ref('int_events__revenue') }}
),

rate_le as (   -- курс на последнюю business_date <= даты события (per event_id)
    select ev.event_id as event_id,
           argMaxIf(cr.usd_per_unit, cr.business_date, cr.business_date <= ev.event_date) as rate
    from ev
    left join {{ ref('int_currency_rates_usd') }} cr on cr.currency = ev.revenue_currency
    group by ev.event_id
),

first_rate as (   -- самый ранний курс валюты (fallback для дат до начала курсов)
    select currency, argMin(usd_per_unit, business_date) as rate0
    from {{ ref('int_currency_rates_usd') }} group by currency
)

select
    ev.event_id   as event_id,
    ev.player_id  as player_id,
    ev.session_id as session_id,
    ev.event_at,
    ev.event_date,
    ev.country,
    ev.app_version,
    ev.ab_version,
    ev.revenue_type,
    ev.revenue_source,
    -- ad_reward уже в USD; для покупок — исходная валюта (по умолчанию USD)
    if(ev.revenue_type = 'ad_reward', 'USD', ifNull(ev.revenue_currency, 'USD')) as currency,
    round(ev.revenue_amount, 6) as revenue_amount,
    round(
        if(ev.revenue_type = 'ad_reward',
           ev.revenue_amount,
           ev.revenue_amount * coalesce(
               nullIf(rl.rate, 0.0),
               fr.rate0,
               if(ifNull(ev.revenue_currency, 'USD') = 'USD', 1.0, null)
           )
        ), 6) as revenue_amount_usd,
    ifNull(pc.campaign_name, '(unknown)') as marketing_campaign
from ev
left join rate_le rl using (event_id)
left join first_rate fr on fr.currency = ifNull(ev.revenue_currency, 'USD')
left join {{ ref('int_player_campaign') }} pc using (player_id)

union all

-- Ручной бэкфилл IAP, не попавших в источник событий (seed iap_backfill; суммы в USD).
select
    'backfill_' || b.player_id || '_' || toString(b.event_date) as event_id,
    b.player_id,
    ''                                                          as session_id,
    toDateTime(b.event_date)                                    as event_at,
    b.event_date,
    p.country,
    p.app_version,
    p.ab_version,
    'purchase'                                                  as revenue_type,
    'manual_backfill'                                           as revenue_source,
    'USD'                                                       as currency,
    round(b.revenue_amount, 6)                                  as revenue_amount,
    round(b.revenue_amount, 6)                                  as revenue_amount_usd,
    ifNull(p.marketing_campaign, '(unknown)')                   as marketing_campaign
from {{ ref('iap_backfill') }} b
left join {{ ref('dim_players') }} p using (player_id)
