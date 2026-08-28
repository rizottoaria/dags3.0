{{ config(materialized='table', tags=['marts','finance','bi']) }}
{#- Когортная витрина монетизации по дню закупки (install-day).
    Плотная сетка дней 0..min(30, today-cohort_date) на каждую когорту,
    чтобы накопленная выручка и ретеншен считались без дыр.
    Скоуп версий: все >= 1.0.22 (version_gte). -#}
{%- set versions = var('report_versions', ['1.0.24','1.0.22']) -%}

with coh as (
    select player_id, app_version as cohort_version, first_seen_date as cohort_date
    from {{ ref('dim_players') }}
    where app_version is not null and {{ version_gte('app_version', '1.0.22') }}
),
sizes as (
    select cohort_version, cohort_date, count() as cohort_size
    from coh group by cohort_version, cohort_date
),
vsize as (   -- полный размер когорты версии (постоянный знаменатель ретеншена/ARPU)
    select cohort_version, count() as version_installs
    from coh group by cohort_version
),
skeleton as (  -- плотная сетка (version, date, day) до зрелости когорты
    select cohort_version, cohort_date, cohort_size,
           toInt32(arrayJoin(range(0, toUInt32(least(toInt64(30), toInt64(today() - cohort_date))) + 1))) as day_since_install
    from sizes
),
activity as (
    select c.cohort_version as cohort_version, c.cohort_date as cohort_date,
           toInt32(e.event_date - c.cohort_date) as d,
           uniqExact(e.player_id) as retained_users
    from {{ ref('stg_events') }} e
    inner join coh c using(player_id)
    where e.event_date >= c.cohort_date and e.event_date - c.cohort_date <= 30
    group by cohort_version, cohort_date, d
),
rev as (   -- реклама из событий (в USD)
    select c.cohort_version as cohort_version, c.cohort_date as cohort_date,
           toInt32(r.event_date - c.cohort_date) as d,
           sumIf(r.revenue_amount, r.revenue_type = 'ad_reward') as ad_revenue,
           toUInt32(countIf(r.revenue_type = 'ad_reward'))       as ad_impressions
    from {{ ref('fct_revenue_events') }} r
    inner join coh c using(player_id)
    where r.event_date >= c.cohort_date and r.event_date - c.cohort_date <= 30
    group by cohort_version, cohort_date, d
),
iap_rev as (   -- IAP в USD (гибрид: monetization_transactions + конверсия старых событий по курсам), см. int_iap_usd
    select c.cohort_version as cohort_version, c.cohort_date as cohort_date,
           toInt32(t.purchase_date - c.cohort_date) as d,
           sum(t.usd_amount) as iap_revenue
    from {{ ref('int_iap_usd') }} t
    inner join coh c using(player_id)
    where t.purchase_date between c.cohort_date and c.cohort_date + 30
    group by cohort_version, cohort_date, d
),
base as (
    select sk.cohort_version as cohort_version, sk.cohort_date as cohort_date,
           sk.day_since_install as day_since_install, sk.cohort_size as cohort_size,
           ifNull(a.retained_users, 0)  as retained_users,
           ifNull(rv.ad_revenue, 0)     as ad_revenue,
           ifNull(rv.ad_impressions, 0) as ad_impressions,
           ifNull(ir.iap_revenue, 0)    as iap_revenue
    from skeleton sk
    left join activity a
      on sk.cohort_version=a.cohort_version and sk.cohort_date=a.cohort_date and sk.day_since_install=a.d
    left join rev rv
      on sk.cohort_version=rv.cohort_version and sk.cohort_date=rv.cohort_date and sk.day_since_install=rv.d
    left join iap_rev ir
      on sk.cohort_version=ir.cohort_version and sk.cohort_date=ir.cohort_date and sk.day_since_install=ir.d
)
select
    b.cohort_version,
    b.cohort_date,
    toMonday(b.cohort_date) as cohort_week,          -- понедельник недели установки (недельные когорты)
    b.day_since_install,
    b.cohort_size,
    v.version_installs,
    toInt32(today() - b.cohort_date) as cohort_age_days,   -- возраст когорты (для fixed-cohort фильтра: cohort_age_days >= N)
    b.retained_users,
    b.retained_users / b.cohort_size as retention_rate,          -- per-cohort (для треугольника)
    b.ad_revenue,
    b.ad_impressions,
    b.iap_revenue,
    b.ad_revenue + b.iap_revenue as total_revenue,
    sum(b.ad_revenue)  over w as cum_ad_revenue,
    sum(b.iap_revenue) over w as cum_iap_revenue,
    sum(b.ad_revenue + b.iap_revenue) over w as cum_total_revenue,
    (sum(b.ad_revenue) over w) / b.cohort_size as ad_arpu_cum,   -- per-cohort
    (sum(b.ad_revenue + b.iap_revenue) over w) / b.cohort_size as arpu_cum
from base b
inner join vsize v on b.cohort_version = v.cohort_version
window w as (partition by b.cohort_version, b.cohort_date order by b.day_since_install
            rows between unbounded preceding and current row)
