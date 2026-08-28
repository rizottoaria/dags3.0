{{ config(materialized='view') }}
{#- Покупки (IAP) в USD — ГИБРИД по дате:
    - с даты покрытия monetization_transactions (min occurredAt, ~2026-07-25) и позже —
      авторитетно usdAmount из monetization_transactions;
    - ДО неё — событийные purchase (petbuddy.events), сконвертированные в USD по
      int_currency_rates_usd (курс на дату покупки; для дат до начала курсов — ближайший
      доступный курс валюты). Одна строка = одна покупка, ключ player_id (= profileId). -#}

with mt as (
    select
        JSONExtractString(data, 'profileId')                                        as player_id,
        toDate(parseDateTimeBestEffortOrNull(JSONExtractString(data, 'occurredAt'))) as purchase_date,
        JSONExtractFloat(data, 'usdAmount')                                         as usd_amount
    from {{ source('raw', 'ben_monetization_transactions') }}
    where JSONExtractString(data, 'type') = 'IAP' and JSONExtractString(data, 'status') = 'RECORDED'
),
mt_start as (select min(purchase_date) as d0 from mt),
ev as (   -- покупки из событий ДО покрытия mt
    select
        id,
        player_id,
        event_date as purchase_date,
        toFloat64OrNull(replaceAll(properties.revenue::String, ',', '.')) as amount,
        nullIf(properties.currency::String, '')                          as currency
    from {{ source('petbuddy', 'events') }}
    where name = 'revenue' and properties.type::String = 'purchase'
      and event_date < (select d0 from mt_start)
),
rate_le as (   -- курс на последнюю business_date <= даты покупки (per purchase id)
    select ev.id as id,
           argMaxIf(r.usd_per_unit, r.business_date, r.business_date <= ev.purchase_date) as rate
    from ev left join {{ ref('int_currency_rates_usd') }} r on r.currency = ev.currency
    group by ev.id
),
first_rate as (   -- самый ранний курс валюты (fallback для дат до начала курсов)
    select currency, argMin(usd_per_unit, business_date) as rate0
    from {{ ref('int_currency_rates_usd') }} group by currency
),
ev_usd as (
    select
        e.player_id,
        e.purchase_date,
        e.amount * coalesce(nullIf(rl.rate, 0.0), fr.rate0, if(e.currency = 'USD', 1.0, null)) as usd_amount
    from ev e
    left join rate_le rl on rl.id = e.id
    left join first_rate fr on fr.currency = e.currency
    where e.amount is not null
)
select player_id, purchase_date, usd_amount from mt     where usd_amount is not null
union all
select player_id, purchase_date, usd_amount from ev_usd where usd_amount is not null
