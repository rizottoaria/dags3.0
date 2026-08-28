{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Платящие пользователи (Paying Users) и IAP-выручка в USD по когортам установки.

    PU_Dk = уникальные игроки, совершившие ХОТЯ БЫ ОДНУ покупку (IAP) в дни 0..k от
    установки (день 0 = день установки), независимо от активности в день k.
    «Закрытый день k» (d{k}_closed) — когда день k полностью прошёл: cohort_age_days >= k+1.

    Источник покупок — raw.ben_monetization_transactions (АВТОРИТЕТНО): usdAmount уже в USD,
    все транзакции RECORDED, ключ profileId = player_id. Это исключает потери покупок из-за
    парсинга/валют в событиях. Грейн: cohort_version x country x cohort_date (день закупки).
    Скоуп версий >= 1.0.22 (можно выбрать любую в BI). Игрок принадлежит одной когорте,
    поэтому PU корректно суммируется в Superset по любым разрезам. -#}

with iap as (
    select
        JSONExtractString(data, 'profileId')                                        as player_id,
        toDate(parseDateTimeBestEffortOrNull(JSONExtractString(data, 'occurredAt'))) as purchase_date,
        JSONExtractFloat(data, 'usdAmount')                                         as usd_amount
    from {{ source('raw', 'ben_monetization_transactions') }}
    where JSONExtractString(data, 'type')   = 'IAP'
      and JSONExtractString(data, 'status') = 'RECORDED'
),
players as (
    select
        player_id,
        ifNull(country, '(unknown)') as country,
        app_version                  as cohort_version,
        first_seen_date              as cohort_date
    from {{ ref('dim_players') }}
    where app_version is not null and {{ version_gte('app_version', '1.0.22') }}
),
j as (
    select
        p.cohort_version as cohort_version,
        p.country        as country,
        p.cohort_date    as cohort_date,
        p.player_id      as player_id,
        i.player_id      as payer_id,
        dateDiff('day', p.cohort_date, i.purchase_date) as d,
        i.usd_amount     as usd_amount
    from players p
    left join iap i on i.player_id = p.player_id
)
select
    cohort_version,
    country,
    cohort_date,
    toInt32(today() - cohort_date) as cohort_age_days,
    uniqExact(player_id)                                       as installs,
    uniqExactIf(payer_id, d between 0 and 0)                   as pu_d0,
    uniqExactIf(payer_id, d between 0 and 1)                   as pu_d1,
    uniqExactIf(payer_id, d between 0 and 3)                   as pu_d3,
    uniqExactIf(payer_id, d between 0 and 7)                   as pu_d7,
    round(sumIf(usd_amount, d between 0 and 0), 2)             as iap_usd_d0,
    round(sumIf(usd_amount, d between 0 and 1), 2)             as iap_usd_d1,
    round(sumIf(usd_amount, d between 0 and 3), 2)             as iap_usd_d3,
    round(sumIf(usd_amount, d between 0 and 7), 2)             as iap_usd_d7,
    toUInt8(toInt32(today() - cohort_date) >= 4)              as d3_closed,   -- день 3 полностью прошёл
    toUInt8(toInt32(today() - cohort_date) >= 8)              as d7_closed
from j
group by cohort_version, country, cohort_date
order by cohort_version, country, cohort_date
