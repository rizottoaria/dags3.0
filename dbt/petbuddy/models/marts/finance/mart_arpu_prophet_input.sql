{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Вход для Prophet (Superset Predictive Analytics и arpu_prophet_forecast.py):
    наблюдаемая накопительная ARPU на install по дням жизни, ПО ВСЕМ версиям >= 1.0.22
    (сегменты: версия × {ALL, US, PH}).

    Maturity-adjusted: на каждый день d берём ВСЕ когорты, дожившие до d, — так реальных
    точек максимум (до возраста старшей когорты). Дни с малой выборкой
    (installs < var prophet_min_installs) отбрасываем, чтобы хвост не был шумом.
    ds = 2024-01-01 + day_since_install — синтетическая дата для Prophet.

    ВАЖНО: каждый источник агрегируем ОТДЕЛЬНО и объединяем уже РЕЗУЛЬТАТЫ (не UNION входов
    с константной колонкой + GROUP BY — ClickHouse сворачивает константу и путает суммы). -#}
{%- set min_installs = var('prophet_min_installs', 20) -%}

with country_level as (
    select
        cohort_version,
        country,
        day_since_install,
        sum(cohort_size)                         as installs,
        sum(cum_ad_revenue)/sum(cohort_size)     as cum_ad_arpu,
        sum(cum_total_revenue)/sum(cohort_size)  as cum_arpu
    from {{ ref('mart_cohort_daily_country') }}
    where {{ version_gte('cohort_version', '1.0.22') }} and country in ('US','PH')
    group by cohort_version, country, day_since_install
),
all_level as (
    select
        cohort_version,
        'ALL' as country,
        day_since_install,
        sum(cohort_size)                         as installs,
        sum(cum_ad_revenue)/sum(cohort_size)     as cum_ad_arpu,
        sum(cum_total_revenue)/sum(cohort_size)  as cum_arpu
    from {{ ref('mart_cohort_daily') }}
    where {{ version_gte('cohort_version', '1.0.22') }}
    group by cohort_version, day_since_install
),
unioned as (
    select * from country_level
    union all
    select * from all_level
)
select
    cohort_version,
    country,
    day_since_install,
    toDate('2024-01-01') + day_since_install     as ds,
    installs,
    cum_ad_arpu,
    cum_arpu
from unioned
where installs >= {{ min_installs }}
order by cohort_version, country, day_since_install
