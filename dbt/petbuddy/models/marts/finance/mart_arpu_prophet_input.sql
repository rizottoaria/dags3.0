{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Вход для Prophet (Superset Predictive Analytics и arpu_prophet_forecast.py):
    наблюдаемая накопительная ARPU на install по дням жизни, ПО ВСЕМ версиям >= 1.0.22
    (сегменты: версия установки × {ALL, US, PH} × кампания {ALL, campaign}).
    Источник — mart_cohort_daily_campaign (сегменты ALL уже посчитаны там).

    Maturity-adjusted: на каждый день d берём ВСЕ когорты, дожившие до d, — так реальных
    точек максимум (до возраста старшей когорты). Дни с малой выборкой
    (installs < var prophet_min_installs) отбрасываем, чтобы хвост не был шумом.
    ds = 2024-01-01 + day_since_install — синтетическая дата для Prophet. -#}
{%- set min_installs = var('prophet_min_installs', 20) -%}

with seg as (
    select
        cohort_version,
        country,
        campaign,
        day_since_install,
        sum(cohort_size)                         as installs,
        sum(cum_ad_revenue)/sum(cohort_size)     as cum_ad_arpu,
        sum(cum_total_revenue)/sum(cohort_size)  as cum_arpu
    from {{ ref('mart_cohort_daily_campaign') }}
    group by cohort_version, country, campaign, day_since_install
)
select
    cohort_version,
    country,
    campaign,
    day_since_install,
    toDate('2024-01-01') + day_since_install     as ds,
    installs,
    cum_ad_arpu,
    cum_arpu
from seg
where installs >= {{ min_installs }}
order by cohort_version, country, campaign, day_since_install
