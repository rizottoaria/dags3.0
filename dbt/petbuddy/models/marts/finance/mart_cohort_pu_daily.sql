{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- PU-треугольник (как retention, но для платящих): накопительные УНИКАЛЬНЫЕ платящие
    (IAP) и IAP-выручка USD по дням жизни, по когортам (версия x страна x день закупки).

    pu_cum(d)   = уникальные игроки, совершившие первую покупку в дни 0..d (накопительно);
    pu_rate_cum = pu_cum / cohort_size (конверсия в платящего к дню d);
    iap_usd_cum = накопленная IAP-выручка (USD) к дню d.
    Плотная сетка дней 0..min(30, возраст когорты). Источник — raw.ben_monetization_transactions
    (авторитетно, usdAmount в USD, profileId = player_id). Скоуп версий >= 1.0.22. -#}

with iap as (
    select
        JSONExtractString(data, 'profileId')                                        as player_id,
        toDate(parseDateTimeBestEffortOrNull(JSONExtractString(data, 'occurredAt'))) as purchase_date,
        JSONExtractFloat(data, 'usdAmount')                                         as usd_amount
    from {{ source('raw', 'ben_monetization_transactions') }}
    where JSONExtractString(data, 'type') = 'IAP' and JSONExtractString(data, 'status') = 'RECORDED'
),
players as (
    select player_id, ifNull(country, '(unknown)') as country,
           app_version as cohort_version, first_seen_date as cohort_date
    from {{ ref('dim_players') }}
    where app_version is not null and {{ version_gte('app_version', '1.0.22') }}
),
sizes as (
    select cohort_version, country, cohort_date, count() as cohort_size
    from players group by cohort_version, country, cohort_date
),
purch as (
    select p.cohort_version as cohort_version, p.country as country, p.cohort_date as cohort_date,
           p.player_id as player_id,
           dateDiff('day', p.cohort_date, i.purchase_date) as d, i.usd_amount as usd
    from players p inner join iap i on i.player_id = p.player_id
    where dateDiff('day', p.cohort_date, i.purchase_date) between 0 and 30
),
first_pay as (   -- первый день покупки на игрока -> для накопительного УНИКАЛЬНОГО счёта
    select cohort_version, country, cohort_date, player_id, min(d) as fpd
    from purch group by cohort_version, country, cohort_date, player_id
),
new_by_day as (
    select cohort_version, country, cohort_date, fpd as d, count() as new_payers
    from first_pay group by cohort_version, country, cohort_date, fpd
),
rev_by_day as (
    select cohort_version, country, cohort_date, d, sum(usd) as iap_usd
    from purch group by cohort_version, country, cohort_date, d
),
skeleton as (
    select cohort_version, country, cohort_date, cohort_size,
           toInt32(arrayJoin(range(0, toUInt32(least(toInt64(30), toInt64(today() - cohort_date))) + 1))) as day_since_install
    from sizes
),
base as (
    select sk.cohort_version as cohort_version, sk.country as country, sk.cohort_date as cohort_date,
           sk.day_since_install as day_since_install, sk.cohort_size as cohort_size,
           ifNull(n.new_payers, 0) as new_payers, ifNull(r.iap_usd, 0) as iap_usd
    from skeleton sk
    left join new_by_day n
      on sk.cohort_version=n.cohort_version and sk.country=n.country and sk.cohort_date=n.cohort_date and sk.day_since_install=n.d
    left join rev_by_day r
      on sk.cohort_version=r.cohort_version and sk.country=r.country and sk.cohort_date=r.cohort_date and sk.day_since_install=r.d
),
cum as (
    select cohort_version, country, cohort_date, day_since_install, cohort_size,
           new_payers,
           sum(new_payers) over w as pu_cum,
           iap_usd,
           sum(iap_usd) over w as iap_usd_cum
    from base
    window w as (partition by cohort_version, country, cohort_date order by day_since_install
                 rows between unbounded preceding and current row)
)
select
    cohort_version, country, cohort_date, day_since_install, cohort_size,
    toInt32(today() - cohort_date) as cohort_age_days,
    new_payers,
    pu_cum,
    pu_cum / cohort_size as pu_rate_cum,
    iap_usd,
    iap_usd_cum,
    iap_usd_cum / cohort_size as iap_arpu_cum
from cum
order by cohort_version, country, cohort_date, day_since_install
