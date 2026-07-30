{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Когортная витрина по дню закупки С РАЗРЕЗОМ ПО СТРАНЕ (US, PH).
    Знаменатель ретеншена/ARPU — version_country_installs (инсталлы версии+страны). -#}
{%- set versions = var('report_versions', ['1.0.24','1.0.22']) -%}
{%- set countries = var('report_countries', ['US','PH']) -%}

with coh as (
    select player_id, app_version as cohort_version, country, first_seen_date as cohort_date
    from {{ ref('dim_players') }}
    where app_version in ({{ "'" ~ versions | join("','") ~ "'" }})
      and country in ({{ "'" ~ countries | join("','") ~ "'" }})
),
sizes as (
    select cohort_version, country, cohort_date, count() as cohort_size
    from coh group by cohort_version, country, cohort_date
),
vsize as (
    select cohort_version, country, count() as version_country_installs
    from coh group by cohort_version, country
),
skeleton as (
    select cohort_version, country, cohort_date, cohort_size,
           toInt32(arrayJoin(range(0, toUInt32(least(toInt64(30), toInt64(today() - cohort_date))) + 1))) as day_since_install
    from sizes
),
activity as (
    select c.cohort_version as cohort_version, c.country as country, c.cohort_date as cohort_date,
           toInt32(e.event_date - c.cohort_date) as d, uniqExact(e.player_id) as retained_users
    from {{ ref('stg_events') }} e inner join coh c using(player_id)
    where e.event_date >= c.cohort_date and e.event_date - c.cohort_date <= 30
    group by cohort_version, country, cohort_date, d
),
rev as (
    select c.cohort_version as cohort_version, c.country as country, c.cohort_date as cohort_date,
           toInt32(r.event_date - c.cohort_date) as d,
           sumIf(r.revenue_amount, r.revenue_type = 'ad_reward') as ad_revenue,
           toUInt32(countIf(r.revenue_type = 'ad_reward'))       as ad_impressions,
           sumIf(r.revenue_amount, r.revenue_type = 'purchase')  as iap_revenue
    from {{ ref('fct_revenue_events') }} r inner join coh c using(player_id)
    where r.event_date >= c.cohort_date and r.event_date - c.cohort_date <= 30
    group by cohort_version, country, cohort_date, d
),
base as (
    select sk.cohort_version as cohort_version, sk.country as country, sk.cohort_date as cohort_date,
           sk.day_since_install as day_since_install, sk.cohort_size as cohort_size,
           ifNull(a.retained_users, 0)  as retained_users,
           ifNull(rv.ad_revenue, 0)     as ad_revenue,
           ifNull(rv.ad_impressions, 0) as ad_impressions,
           ifNull(rv.iap_revenue, 0)    as iap_revenue
    from skeleton sk
    left join activity a
      on sk.cohort_version=a.cohort_version and sk.country=a.country and sk.cohort_date=a.cohort_date and sk.day_since_install=a.d
    left join rev rv
      on sk.cohort_version=rv.cohort_version and sk.country=rv.country and sk.cohort_date=rv.cohort_date and sk.day_since_install=rv.d
)
select
    b.cohort_version, b.country, b.cohort_date,
    toMonday(b.cohort_date) as cohort_week,          -- понедельник недели установки (недельные когорты)
    b.day_since_install,
    b.cohort_size, v.version_country_installs,
    toInt32(today() - b.cohort_date) as cohort_age_days,
    b.retained_users,
    b.retained_users / b.cohort_size as retention_rate,
    b.ad_revenue, b.ad_impressions, b.iap_revenue,
    b.ad_revenue + b.iap_revenue as total_revenue,
    sum(b.ad_revenue)  over w as cum_ad_revenue,
    sum(b.iap_revenue) over w as cum_iap_revenue,
    sum(b.ad_revenue + b.iap_revenue) over w as cum_total_revenue,
    (sum(b.ad_revenue) over w) / b.cohort_size as ad_arpu_cum,
    (sum(b.ad_revenue + b.iap_revenue) over w) / b.cohort_size as arpu_cum
from base b
inner join vsize v on b.cohort_version = v.cohort_version and b.country = v.country
window w as (partition by b.cohort_version, b.country, b.cohort_date order by b.day_since_install
            rows between unbounded preceding and current row)
