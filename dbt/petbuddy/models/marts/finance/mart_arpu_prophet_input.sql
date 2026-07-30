{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Вход для Prophet (Superset Predictive Analytics и arpu_prophet_forecast.py):
    наблюдаемая накопительная ARPU на install по дням жизни для 1.0.24 (ALL/US/PH).

    Maturity-adjusted: на каждый день d берём ВСЕ когорты, дожившие до d, — так реальных
    точек максимум (до возраста старшей когорты, а не жёстко 14). Дни с малой выборкой
    (installs < var prophet_min_installs) отбрасываем, чтобы хвост не был шумом.
    ds = 2024-01-01 + day_since_install — синтетическая дата для Prophet.

    ВАЖНО: каждый источник агрегируем ОТДЕЛЬНО и объединяем уже РЕЗУЛЬТАТЫ. Если делать
    UNION входов с константной колонкой ('ALL' as country) и потом GROUP BY country,
    ClickHouse сворачивает константу и путает суммы US/PH. -#}
{%- set min_installs = var('prophet_min_installs', 20) -%}

with country_level as (
    select
        country,
        day_since_install,
        sum(cohort_size)                         as installs,
        sum(cum_ad_revenue)/sum(cohort_size)     as cum_ad_arpu,
        sum(cum_total_revenue)/sum(cohort_size)  as cum_arpu
    from {{ ref('mart_cohort_daily_country') }}
    where cohort_version = '1.0.24' and country in ('US','PH')
    group by country, day_since_install
),
all_level as (
    select
        'ALL' as country,
        day_since_install,
        sum(cohort_size)                         as installs,
        sum(cum_ad_revenue)/sum(cohort_size)     as cum_ad_arpu,
        sum(cum_total_revenue)/sum(cohort_size)  as cum_arpu
    from {{ ref('mart_cohort_daily') }}
    where cohort_version = '1.0.24'
    group by day_since_install
),
unioned as (
    select * from country_level
    union all
    select * from all_level
)
select
    '1.0.24'                                     as cohort_version,
    country,
    day_since_install,
    toDate('2024-01-01') + day_since_install     as ds,
    installs,
    cum_ad_arpu,
    cum_arpu
from unioned
where installs >= {{ min_installs }}
order by country, day_since_install
