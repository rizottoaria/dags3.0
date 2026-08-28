{{ config(materialized='view') }}

-- Курсы валют, приведённые к «USD за 1 единицу валюты», по каждой дате.
-- Источник: petbuddy.currency_rates (база EUR, ReplacingMergeTree) — берём последнюю
-- версию строки через argMax(update_at). usd_per_unit(code) = rate(EUR→USD) / rate(EUR→code).
-- Для code = USD получаем ровно 1.0.

with dedup as (
    select
        business_date,
        target_currency,
        argMax(rate, update_at) as rate
    from {{ source('petbuddy', 'currency_rates') }}
    group by business_date, target_currency
),

usd as (
    select business_date, rate as eur_to_usd
    from dedup
    where target_currency = 'USD'
)

select
    d.business_date                            as business_date,
    d.target_currency                          as currency,
    toNullable(u.eur_to_usd / d.rate)          as usd_per_unit
from dedup d
inner join usd u using (business_date)
where d.rate > 0
