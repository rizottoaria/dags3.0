{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Вход для Prophet (Superset Predictive Analytics): чистая fixed-cohort накопительная ARPU
    по дням жизни для 1.0.24 (ALL/US/PH). Синтетическая дата ds = 2024-01-01 + day_since_install,
    чтобы Superset строил временной ряд и Prophet экстраполировал «будущие дни». -#}

with src as (
    select cohort_version, 'ALL' as country, cohort_age_days, day_since_install, cohort_size, cum_ad_revenue, cum_total_revenue
    from {{ ref('mart_cohort_daily') }} where cohort_version = '1.0.24'
    union all
    select cohort_version, country, cohort_age_days, day_since_install, cohort_size, cum_ad_revenue, cum_total_revenue
    from {{ ref('mart_cohort_daily_country') }} where cohort_version = '1.0.24' and country in ('US','PH')
),
seg_h as (
    select cohort_version, country, least(toInt32(14), toInt32(max(cohort_age_days))) as H
    from src group by cohort_version, country
)
select
    s.cohort_version as cohort_version,
    s.country        as country,
    s.day_since_install as day_since_install,
    toDate('2024-01-01') + s.day_since_install as ds,      -- синтетическая дата для Prophet
    sum(s.cum_ad_revenue)/sum(s.cohort_size)    as cum_ad_arpu,
    sum(s.cum_total_revenue)/sum(s.cohort_size) as cum_arpu
from src s inner join seg_h h on s.cohort_version=h.cohort_version and s.country=h.country
where s.cohort_age_days >= h.H and s.day_since_install between 0 and h.H
group by s.cohort_version, s.country, s.day_since_install
order by country, day_since_install
